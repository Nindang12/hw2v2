"""
FILE: parking_spark_processor.py
MÔ TẢ: Camera-based Parking Fee Calculator

LOGIC CAMERA THỰC TẾ:
- Camera ở mỗi vị trí (A1, A2, B1, ...) detect xe
- ENTERING: Xe vào cổng → CHƯA tính phí
- PARKED tại A1: Camera A1 detect xe → BẮT ĐẦU tính phí
- MOVING: Camera không thấy xe nữa → VẪN tính phí (xe di chuyển trong bãi)
- PARKED tại B5: Camera B5 detect xe → TIẾP TỤC tính từ lần PARKED đầu
- EXITING: Xe ra → CHỐT phí

GroupBy license_plate: Track xe theo biển số
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===========================
# CONFIG
# ===========================
KAFKA_BOOTSTRAP = "192.168.1.9:9092"
KAFKA_TOPIC = "parking-events"
DEFAULT_PRICE_PER_10MIN = 5000
PRICE_CONFIG_FILE = "/opt/shared/parking_price_config.json"
CHECKPOINT_DIR = "/tmp/checkpoint_parking"

def load_price_config():
    """Load giá từ config file"""
    try:
        if os.path.exists(PRICE_CONFIG_FILE):
            with open(PRICE_CONFIG_FILE, 'r') as f:
                config = json.load(f)
                price = config.get("price_per_10min", DEFAULT_PRICE_PER_10MIN)
                logger.info(f"Loaded price from config: {price:,} VND per 10 minutes")
                return price
    except Exception as e:
        logger.warning(f"Error loading price config: {e}, using default: {DEFAULT_PRICE_PER_10MIN}")
    return DEFAULT_PRICE_PER_10MIN

# Load price on startup
PRICE_PER_10MIN = load_price_config()

# ===========================
# SPARK
# ===========================
spark = SparkSession.builder \
    .appName("ParkingCameraSystem") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.adaptive.enabled", "false") \
    .master("spark://192.168.1.13:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ===========================
# SCHEMA & READ KAFKA
# ===========================
kafka_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("timestamp_unix", LongType(), True),
    StructField("license_plate", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status_code", StringType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data")
).select("data.*")

events_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# ===========================
# LOGIC: Bỏ ENTERING, chỉ xử lý từ PARKED trở đi
# ===========================
watermarked_df = events_df.withWatermark("event_time", "2 minutes")

# Filter: Chỉ lấy xe đã PARKED (bỏ ENTERING vì chưa tính phí)
events_billable = watermarked_df.filter(
    col("status_code").isin("PARKED", "MOVING", "EXITING")
)

# Đánh dấu thời điểm PARKED (để lấy entry_time)
events_marked = events_billable.withColumn(
    "parked_time",
    when(col("status_code") == "PARKED", col("timestamp_unix"))
).withColumn(
    "parked_timestamp_str",
    when(col("status_code") == "PARKED", col("timestamp"))
)

# ===========================
# AGGREGATION: GroupBy (location, license_plate) - Tổng hợp theo VỊ TRÍ và XE
# ===========================
# FIX: GroupBy theo location để track xe tại từng vị trí
# Watermark sẽ tự động xóa state sau khi EXITING (sau 2 phút)
# Khi xe vào lại, sẽ tạo state mới với entry_time mới

vehicle_aggregated = events_marked.groupBy(
    col("location"),
    col("license_plate")
).agg(
    # Status mới nhất
    last("status_code").alias("current_status"),
    
    # ✅ ENTRY TIME: Lần PARKED ĐẦU TIÊN tại vị trí này
    # min() chỉ tính trên các event PARKED (vì MOVING có parked_time = NULL)
    # Watermark sẽ tự động xóa state sau khi EXITING (sau 2 phút)
    min("parked_time").alias("entry_time"),
    
    # Entry timestamp string
    min("parked_timestamp_str").alias("entry_timestamp"),
    
    # Last update để tính thời gian đỗ
    max("timestamp_unix").alias("last_update"),
    
    # Exit timestamp (khi EXITING)
    max(when(col("status_code") == "EXITING", col("timestamp"))).alias("exit_timestamp")
)

# ===========================
# TÍNH PHÍ
# ===========================
parking_with_fees = vehicle_aggregated.withColumn(
    "parked_seconds",
    when(col("entry_time").isNotNull(), 
         col("last_update") - col("entry_time")
    ).otherwise(0)
).withColumn(
    "parked_minutes",
    (col("parked_seconds") / 60).cast("int")
).withColumn(
    "fee_blocks",
    when(col("parked_minutes") > 0,
         ((col("parked_minutes") + 9) / 10).cast("int")
    ).otherwise(0)
).withColumn(
    "total_fee",
    col("fee_blocks") * lit(PRICE_PER_10MIN)
)

# ===========================
# OUTPUT 1: REAL-TIME MONITORING (xe đang đỗ)
# ===========================
# Chỉ hiển thị xe ĐANG ĐỖ (PARKED/MOVING), filter bỏ EXITING
# Watermark sẽ tự động xóa state sau khi EXITING (sau 2 phút)
active_vehicles = parking_with_fees.select(
    col("location"),
    col("license_plate"),
    col("current_status"),
    col("entry_timestamp"),
    col("parked_minutes"),
    col("total_fee"),
    col("last_update")
).filter(
    # Xe đã PARKED ít nhất 1 lần VÀ chưa EXITING
    (col("entry_time").isNotNull()) &
    (col("current_status") != "EXITING")
).orderBy("location")

# Tạo dataset cho Parquet (bao gồm cả EXITING để API có thể xử lý)
# FIX: Parquet output bao gồm cả EXITING để API có thể xử lý checkout events
all_vehicles_for_parquet = parking_with_fees.select(
    col("location"),
    col("license_plate"),
    col("current_status"),
    col("entry_timestamp"),
    col("parked_minutes"),
    col("total_fee"),
    col("last_update")
).filter(
    # Chỉ filter bỏ những xe chưa có entry_time
    col("entry_time").isNotNull()
).orderBy("location")

# Stream 1: Memory table cho monitoring (nếu cần query từ cùng process)
memory_query = active_vehicles.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("parking_realtime") \
    .trigger(processingTime="5 seconds") \
    .start()

# Stream 1b: Ghi ra Parquet file để share với API server
# FIX: Memory table không share được giữa các SparkSession
# Dùng Parquet file để share data giữa processes
# Parquet không hỗ trợ "complete" mode, dùng foreachBatch để ghi lại toàn bộ data
parquet_output_path = "/opt/shared/parking_realtime_parquet"

def write_to_parquet(batch_df, batch_id):
    """Ghi lại toàn bộ data vào Parquet file (complete mode behavior)"""
    if not batch_df.isEmpty():
        # FIX: Ghi vào file tạm rồi rename để tránh race condition
        import shutil
        import time as time_module
        
        # Reload price config mỗi batch để cập nhật giá mới
        global PRICE_PER_10MIN
        PRICE_PER_10MIN = load_price_config()
        
        temp_path = f"{parquet_output_path}_temp_{int(time_module.time())}"
        final_path = parquet_output_path
        
        try:
            # Xóa file tạm cũ nếu có (cleanup)
            for old_temp in os.listdir(os.path.dirname(final_path) or '.'):
                if old_temp.startswith(os.path.basename(final_path) + "_temp_"):
                    old_path = os.path.join(os.path.dirname(final_path) or '.', old_temp)
                    if os.path.isdir(old_path):
                        try:
                            shutil.rmtree(old_path)
                        except:
                            pass
            
            # Ghi vào file tạm với timestamp unique
            batch_df.coalesce(1).write.mode("overwrite").parquet(temp_path)
            
            # Đợi một chút để đảm bảo file đã được ghi xong
            time_module.sleep(0.1)
            
            # Xóa file cũ và rename file tạm (atomic operation)
            if os.path.exists(final_path):
                shutil.rmtree(final_path)
            
            # Rename atomic
            os.rename(temp_path, final_path)
            
            logger.debug(f"Updated Parquet file at {final_path} (batch {batch_id}, {batch_df.count()} rows, Price: {PRICE_PER_10MIN:,} VND/10min)")
        except Exception as e:
            logger.error(f"Error writing Parquet file: {e}")
            # Cleanup temp file nếu có lỗi
            if os.path.exists(temp_path):
                try:
                    shutil.rmtree(temp_path)
                except:
                    pass

# FIX: Dùng all_vehicles_for_parquet để bao gồm cả EXITING
parquet_query = all_vehicles_for_parquet.writeStream \
    .foreachBatch(write_to_parquet) \
    .outputMode("complete") \
    .trigger(processingTime="5 seconds") \
    .start()
logger.info(f"✓ Parquet output started: {parquet_output_path} (includes EXITING)")

# Stream 2: Console cho monitoring
console_query = active_vehicles.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 30) \
    .trigger(processingTime="10 seconds") \
    .start()

# ===========================
# OUTPUT 2: CHECKOUT EVENTS (xe vừa EXITING)
# ===========================
# Lấy xe EXITING để lưu vào DB/Kafka và reset state
checkout_records = parking_with_fees.select(
    col("license_plate"),
    col("location").alias("exit_location"),  # FIX: Dùng location thay vì current_location
    col("entry_timestamp"),
    col("exit_timestamp"),
    col("parked_minutes"),
    col("total_fee"),
    current_timestamp().alias("processed_time")
).filter(
    (col("current_status") == "EXITING") &  # Chỉ lấy xe EXITING
    (col("entry_time").isNotNull())  # Có entry time hợp lệ
)

# Stream 3: foreachBatch để xử lý checkout events
def process_checkout_batch(batch_df, batch_id):
    """Xử lý checkout events: in ra console và có thể gửi vào Kafka/DB"""
    if not batch_df.isEmpty():
        print(f"\n{'='*80}")
        print(f"🛒 CHECKOUT EVENTS - Batch {batch_id}")
        print(f"{'='*80}")
        batch_df.show(truncate=False)
        print(f"{'='*80}\n")
        
        # TODO: Có thể thêm logic để:
        # 1. Gửi vào Kafka topic khác
        # 2. Insert vào database
        # 3. Reset state (nếu cần)

checkout_query = checkout_records.writeStream \
    .foreachBatch(process_checkout_batch) \
    .outputMode("update") \
    .trigger(processingTime="5 seconds") \
    .start()

# ===========================
# INFO
# ===========================
print("\n" + "=" * 80)
print("🎥 CAMERA-BASED PARKING SYSTEM")
print("=" * 80)
print(f"Kafka: {KAFKA_BOOTSTRAP} / {KAFKA_TOPIC}")
print(f"Price: {PRICE_PER_10MIN:,} VND per 10 minutes")
print("=" * 80)
print("\n📹 Logic:")
print("  • ENTERING → No fee (waiting for parking)")
print("  • PARKED → START billing from here")
print("  • MOVING → Keep billing (still inside)")
print("  • PARKED again → Continue billing from first PARKED")
print("  • EXITING → Final checkout (sent to checkout stream)")
print("\n🔑 GroupBy: license_plate (track per vehicle)")
print("   Each vehicle has: current_location, entry_time, total_fee")
print("\n📊 Outputs:")
print("  1. parking_realtime (memory) → Active vehicles (PARKED/MOVING)")
print("  2. checkout_events (foreachBatch) → EXITING events (for DB/Kafka)")
print("=" * 80 + "\n")

print("Example Flow:")
print("  00:10 - 29A-12345 ENTERING → No fee")
print("  00:15 - 29A-12345 PARKED at A1 → Start fee (entry_time = 00:15)")
print("           ↓ Appears in parking_realtime table")
print("  00:25 - 29A-12345 MOVING → Fee = 10min (still billing)")
print("           ↓ Still in parking_realtime")
print("  00:30 - 29A-12345 PARKED at B5 → Fee = 15min (continue from 00:15)")
print("           ↓ Still in parking_realtime")
print("  00:45 - 29A-12345 EXITING → Final fee = 30min")
print("           ↓ Removed from parking_realtime")
print("           ↓ Sent to checkout_events stream (for DB/Kafka)")
print("           ↓ State ready for next visit (if needed)")
print("=" * 80 + "\n")

# ===========================
# RUN
# ===========================
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n🛑 Stopping...")
    for query in spark.streams.active:
        query.stop()
    spark.stop()
    print("✅ Stopped.")