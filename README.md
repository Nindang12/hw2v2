# Parking Consumer Service với Spark

Service xử lý dữ liệu parking từ Kafka sử dụng Spark Structured Streaming.

## Cài đặt Dependencies

```bash
pip install -r requirements.txt
```

## Cấu hình

### Environment Variables

- `USE_SPARK=true` (mặc định) - Bật/tắt Spark
- `SPARK_MASTER=spark://192.168.80.105:7077` - URL Spark Master
- `KAFKA_BOOTSTRAP=192.168.80.105:9092` - Kafka Bootstrap Servers
- `KAFKA_TOPIC=parking-lot` - Kafka Topic
- `USE_CASSANDRA=false` - Bật/tắt Cassandra (optional)
- `CASSANDRA_HOST=127.0.0.1` - Cassandra Host (nếu dùng)
- `PRICE_PER_10MIN_BLOCK=5000` - Giá thường (VND)
- `VIP_PRICE_PER_10MIN_BLOCK=8000` - Giá VIP (VND)

## Cách chạy

### 1. Chạy với Spark (mặc định)

```bash
python parking_consumer_service.py
```

Hoặc với environment variables:

```bash
SPARK_MASTER=spark://192.168.80.105:7077 \
KAFKA_BOOTSTRAP=192.168.80.105:9092 \
python parking_consumer_service.py
```

### 2. Chạy không dùng Spark (fallback về KafkaConsumer)

```bash
USE_SPARK=false python parking_consumer_service.py
```

### 3. Chạy với tất cả cấu hình tùy chỉnh

```bash
USE_SPARK=true \
SPARK_MASTER=spark://192.168.80.105:7077 \
KAFKA_BOOTSTRAP=192.168.80.105:9092 \
KAFKA_TOPIC=parking-lot \
PRICE_PER_10MIN_BLOCK=5000 \
VIP_PRICE_PER_10MIN_BLOCK=8000 \
python parking_consumer_service.py
```

## Truy cập Dashboard

Sau khi chạy, mở trình duyệt và truy cập:
- **Dashboard**: http://localhost:8000
- **API State**: http://localhost:8000/state
- **API Locations Free**: http://localhost:8000/locations_free

## Yêu cầu

- Python 3.7+
- Spark Cluster (nếu dùng Spark)
- Kafka đang chạy
- Java 8+ (cho Spark)

## Lưu ý

- Spark sẽ tự động tải Kafka connector khi chạy
- Checkpoint location: `/tmp/spark-checkpoint-parking`
- Nếu Spark không kết nối được, sẽ tự động fallback về KafkaConsumer sau 3 lần thử

# hw2v2
