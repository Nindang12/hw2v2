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
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===========================
# CONFIG
# ===========================
CONFIG_FILE = "/opt/shared/parking_config.json"
LEGACY_PRICE_CONFIG_FILE = "/opt/shared/parking_price_config.json"  # Backward compatibility

# Default values
DEFAULT_CONFIG = {
    "price": {"price_per_10min": 5000, "currency": "VND"},
    "kafka": {"bootstrap_servers": "192.168.80.98:9092", "topic": "parking-events"},
    "spark": {"master": "spark://192.168.80.98:7077", "checkpoint_dir": "/tmp/checkpoint_parking", "shuffle_partitions": 2},
    "streaming": {"processing_interval_seconds": 5, "console_interval_seconds": 10}
}

def load_config():
    """Load config từ JSON file"""
    config = DEFAULT_CONFIG.copy()
    
    # Try to load from new config file
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                file_config = json.load(f)
                # Merge với default config
                for key in config:
                    if key in file_config:
                        config[key].update(file_config[key])
                logger.info(f"Loaded config from {CONFIG_FILE}")
        except Exception as e:
            logger.warning(f"Error loading config from {CONFIG_FILE}: {e}, using defaults")
    
    # Backward compatibility: check legacy price config file
    if os.path.exists(LEGACY_PRICE_CONFIG_FILE):
        try:
            with open(LEGACY_PRICE_CONFIG_FILE, 'r') as f:
                legacy_config = json.load(f)
                if "price_per_10min" in legacy_config:
                    config["price"]["price_per_10min"] = legacy_config["price_per_10min"]
                    logger.info(f"Loaded price from legacy config: {config['price']['price_per_10min']:,} VND")
        except Exception as e:
            logger.warning(f"Error loading legacy price config: {e}")
    
    return config

def save_config(config):
    """Lưu config vào JSON file"""
    try:
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info(f"Saved config to {CONFIG_FILE}")
        return True
    except Exception as e:
        logger.error(f"Error saving config: {e}")
        return False

def update_price_in_config(new_price):
    """Cập nhật giá trong config file"""
    config = load_config()
    config["price"]["price_per_10min"] = int(new_price)
    config["price"]["updated_at"] = datetime.now().isoformat()
    return save_config(config)

# Load config on startup
app_config = load_config()
KAFKA_BOOTSTRAP = app_config["kafka"]["bootstrap_servers"]
KAFKA_TOPIC = app_config["kafka"]["topic"]
PRICE_PER_10MIN = app_config["price"]["price_per_10min"]
CHECKPOINT_DIR = app_config["spark"]["checkpoint_dir"]
SPARK_MASTER = app_config["spark"]["master"]
SHUFFLE_PARTITIONS = app_config["spark"]["shuffle_partitions"]
PROCESSING_INTERVAL = app_config["streaming"]["processing_interval_seconds"]
CONSOLE_INTERVAL = app_config["streaming"]["console_interval_seconds"]

logger.info(f"Config loaded: Price={PRICE_PER_10MIN:,} VND/10min, Kafka={KAFKA_BOOTSTRAP}, Topic={KAFKA_TOPIC}")

# ===========================
# SPARK
# ===========================
spark = SparkSession.builder \
    .appName("ParkingCameraSystem") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS)) \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
    .master(SPARK_MASTER) \
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
    .trigger(processingTime=f"{PROCESSING_INTERVAL} seconds") \
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
        app_config = load_config()
        PRICE_PER_10MIN = app_config["price"]["price_per_10min"]
        
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
            
            # Đợi một chút để đảm bảo file đã được ghi xong hoàn toàn
            time_module.sleep(0.5)
            
            # FIX: Kiểm tra file tạm đã tồn tại và có dữ liệu trước khi xóa file cũ
            if not os.path.exists(temp_path):
                raise Exception(f"Temp file {temp_path} was not created")
            
            # Kiểm tra file tạm có ít nhất 1 file parquet bên trong
            temp_files = [f for f in os.listdir(temp_path) if f.endswith('.parquet')]
            if not temp_files:
                raise Exception(f"Temp file {temp_path} does not contain parquet files")
            
            # Xóa file cũ và rename file tạm (atomic operation)
            # FIX: Chỉ xóa file cũ sau khi file tạm đã sẵn sàng
            if os.path.exists(final_path):
                try:
                    shutil.rmtree(final_path)
                    # Đợi một chút để đảm bảo xóa hoàn tất
                    time_module.sleep(0.2)
                except Exception as e:
                    logger.warning(f"Error removing old file {final_path}: {e}")
            
            # Rename atomic
            os.rename(temp_path, final_path)
            
            # Đợi thêm một chút để đảm bảo rename hoàn tất
            time_module.sleep(0.2)
            
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
    .trigger(processingTime=f"{PROCESSING_INTERVAL} seconds") \
    .start()
logger.info(f"✓ Parquet output started: {parquet_output_path} (includes EXITING)")

# Stream 2: Console cho monitoring
console_query = active_vehicles.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 30) \
    .trigger(processingTime=f"{CONSOLE_INTERVAL} seconds") \
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
    .trigger(processingTime=f"{PROCESSING_INTERVAL} seconds") \
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