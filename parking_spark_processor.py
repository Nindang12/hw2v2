"""
FILE: parking_spark_processor.py
MÔ TẢ: Spark Structured Streaming - Đọc từ Kafka, tính phí đỗ xe, xuất ra Memory Table
CHẠY: python parking_spark_processor.py
      hoặc spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 parking_spark_processor.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===========================
# CẤU HÌNH HỆ THỐNG
# ===========================
KAFKA_BOOTSTRAP = "192.168.80.127:9092"  # Sửa thành "localhost:9092" nếu cần
KAFKA_TOPIC = "parking-events"
PRICE_PER_10MIN = 5000  # 5,000 VND mỗi 10 phút
CHECKPOINT_DIR = "/tmp/checkpoint_parking"

# ===========================
# KHỞI TẠO SPARK SESSION
# ===========================
logger.info("Initializing Spark Session...")

spark = SparkSession.builder \
    .appName("ParkingFeeCalculator") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.sql.adaptive.enabled", "false") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

logger.info("Spark Session initialized successfully")

# ===========================
# ĐỊNH NGHĨA SCHEMA
# ===========================
kafka_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("timestamp_unix", LongType(), True),
    StructField("license_plate", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status_code", StringType(), True)
])

# ===========================
# ĐỌC DỮ LIỆU TỪ KAFKA
# ===========================
logger.info(f"Connecting to Kafka: {KAFKA_BOOTSTRAP}, Topic: {KAFKA_TOPIC}")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

logger.info("Kafka connection established")

# ===========================
# PARSE JSON DATA
# ===========================
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data")
).select("data.*")

# Thêm event_time từ timestamp string
events_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# ===========================
# STATEFUL AGGREGATION
# ===========================
logger.info("Setting up stateful aggregation...")

# Tạo watermark để xử lý late data
watermarked_df = events_df.withWatermark("event_time", "2 minutes")

# Group by location và license_plate - STATEFUL PROCESSING
parking_aggregated = watermarked_df.groupBy(
    col("location"),
    col("license_plate")
).agg(
    last("status_code").alias("current_status"),
    min(when(col("status_code") == "ENTERING", col("timestamp_unix"))).alias("entry_time"),
    first(when(col("status_code") == "ENTERING", col("timestamp"))).alias("entry_timestamp"),
    max("timestamp_unix").alias("last_update")
)

# ===========================
# TÍNH PHÍ ĐỖ XE
# ===========================
parking_with_fees = parking_aggregated.withColumn(
    "parked_seconds",
    when(col("entry_time").isNotNull(), 
         col("last_update") - col("entry_time")
    ).otherwise(0)
).withColumn(
    "parked_minutes",
    (col("parked_seconds") / 60).cast("int")
).withColumn(
    "fee_blocks",
    # Round up: (minutes + 9) / 10
    ((col("parked_minutes") + 9) / 10).cast("int")
).withColumn(
    "total_fee",
    col("fee_blocks") * lit(PRICE_PER_10MIN)
)

# Select columns cuối cùng và filter bỏ xe đã ra
final_df = parking_with_fees.select(
    col("location"),
    col("license_plate"),
    col("current_status"),
    col("entry_timestamp"),
    col("parked_minutes"),
    col("total_fee"),
    col("last_update")
).filter(
    (col("current_status").isNotNull()) & 
    (col("current_status") != "EXITING")
).orderBy("location")

# ===========================
# OUTPUT 1: MEMORY TABLE (COMPLETE MODE)
# ===========================
logger.info("Starting Memory Table query (complete mode)...")

memory_query = final_df.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("parking_realtime") \
    .trigger(processingTime="5 seconds") \
    .start()

logger.info("✓ Memory Table query started - Table name: parking_realtime")

# ===========================
# OUTPUT 2: CONSOLE (UPDATE MODE)
# ===========================
logger.info("Starting Console query (update mode)...")

# Tạo query riêng cho console với window để support update mode
console_df = watermarked_df.groupBy(
    window("event_time", "30 seconds"),
    "location",
    "license_plate"
).agg(
    last("status_code").alias("current_status"),
    min(when(col("status_code") == "ENTERING", col("timestamp_unix"))).alias("entry_time"),
    first(when(col("status_code") == "ENTERING", col("timestamp"))).alias("entry_timestamp"),
    max("timestamp_unix").alias("last_update")
).withColumn(
    "parked_seconds",
    when(col("entry_time").isNotNull(), col("last_update") - col("entry_time")).otherwise(0)
).withColumn(
    "parked_minutes",
    (col("parked_seconds") / 60).cast("int")
).withColumn(
    "fee_blocks",
    ((col("parked_minutes") + 9) / 10).cast("int")
).withColumn(
    "total_fee",
    col("fee_blocks") * lit(PRICE_PER_10MIN)
).select(
    "location",
    "license_plate",
    "current_status",
    "entry_timestamp",
    "parked_minutes",
    "total_fee"
).filter(col("current_status") != "EXITING")

console_query = console_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .trigger(processingTime="10 seconds") \
    .start()

logger.info("✓ Console query started")

# ===========================
# THÔNG TIN KHỞI ĐỘNG
# ===========================
print("\n" + "=" * 80)
print("🚗 PARKING FEE CALCULATOR - SPARK STREAMING")
print("=" * 80)
print(f"Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
print(f"Kafka Topic: {KAFKA_TOPIC}")
print(f"Price: {PRICE_PER_10MIN:,} VND per 10 minutes")
print(f"Checkpoint: {CHECKPOINT_DIR}")
print("=" * 80)
print("\n✓ Streaming Queries Running:")
print("  1. Memory Table: parking_realtime (complete mode) - For API")
print("  2. Console Output (update mode) - For debugging")
print("\n📊 To query memory table in another process:")
print("  spark.sql('SELECT * FROM parking_realtime').show()")
print("=" * 80 + "\n")

# ===========================
# WAIT FOR TERMINATION
# ===========================
try:
    logger.info("Waiting for streaming queries to terminate...")
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    logger.info("\nReceived interrupt signal, stopping all queries...")
    for query in spark.streams.active:
        query_name = query.name or query.id
        logger.info(f"Stopping query: {query_name}")
        query.stop()
    logger.info("All queries stopped successfully")
    spark.stop()
except Exception as e:
    logger.error(f"Error during execution: {e}")
    raise