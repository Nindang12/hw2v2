# BÁO CÁO HỆ THỐNG XỬ LÝ DỮ LIỆU PARKING REAL-TIME VỚI SPARK STREAMING

## MỤC LỤC
1. [Tổng quan hệ thống](#1-tổng-quan-hệ-thống)
2. [Kiến trúc hệ thống](#2-kiến-trúc-hệ-thống)
3. [Giải thích code chi tiết](#3-giải-thích-code-chi-tiết)
4. [Xử lý dữ liệu với Spark Streaming](#4-xử-lý-dữ-liệu-với-spark-streaming)
5. [API Endpoints](#5-api-endpoints)
6. [Dashboard Web](#6-dashboard-web)
7. [Cấu hình và triển khai](#7-cấu-hình-và-triển-khai)
8. [Kết luận](#8-kết-luận)

---

## 1. TỔNG QUAN HỆ THỐNG

### 1.1. Mô tả
Hệ thống xử lý dữ liệu parking real-time sử dụng **Apache Spark Structured Streaming** để đọc và xử lý events từ **Apache Kafka**. Hệ thống cung cấp:
- Xử lý real-time các sự kiện đỗ xe (ENTERING, PARKED, MOVING, EXITING)
- Tính toán phí đỗ xe theo block 10 phút
- Hỗ trợ VIP và thường với giá khác nhau
- Dashboard web hiển thị trạng thái real-time
- REST API để truy vấn dữ liệu

### 1.2. Công nghệ sử dụng
- **Python 3.7+**
- **Apache Spark 4.0** (Structured Streaming)
- **Apache Kafka** (Message Broker)
- **FastAPI** (Web Framework)
- **Uvicorn** (ASGI Server)
- **Cassandra** (Optional - Lưu trữ lịch sử)

---

## 2. KIẾN TRÚC HỆ THỐNG

```
┌─────────────────┐
│  Kafka Producer │  (parking_json_stream.py)
│  ────────────── │
│  Topic:         │
│  parking-lot    │
└────────┬────────┘
         │
         │ Events (JSON)
         ▼
┌─────────────────────────────────────┐
│   Kafka Cluster                     │
│   ──────────────────────────────────│
│   Topic: parking-lot                │
└────────┬────────────────────────────┘
         │
         │ Spark Structured Streaming
         ▼
┌─────────────────────────────────────┐
│   Spark Cluster                     │
│   ──────────────────────────────────│
│   • Read from Kafka                 │
│   • Parse JSON                      │
│   • Process batches                 │
│   • Update state                    │
└────────┬────────────────────────────┘
         │
         │ Update parking_state
         ▼
┌─────────────────────────────────────┐
│   Parking Consumer Service          │
│   ──────────────────────────────────│
│   • In-memory state                 │
│   • FastAPI REST API                │
│   • Web Dashboard                   │
│   • Fee calculation                 │
└────────┬────────────────────────────┘
         │
         │ HTTP/WebSocket
         ▼
┌─────────────────┐
│   Web Browser   │
│   (Dashboard)   │
└─────────────────┘
```

---

## 3. GIẢI THÍCH CODE CHI TIẾT

### 3.1. Import và Cấu hình

```python
import time
import json
import threading
from math import ceil
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse, HTMLResponse
import uvicorn
import os
from kafka import KafkaProducer

# Spark configuration
USE_SPARK = os.getenv("USE_SPARK", "true").lower() == "true"
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://192.168.80.105:7077")
```

**Giải thích:**
- Sử dụng environment variables để cấu hình linh hoạt
- `USE_SPARK`: Bật/tắt Spark (có thể fallback về KafkaConsumer)
- `SPARK_MASTER`: URL của Spark cluster master

### 3.2. Conditional Import

```python
if USE_SPARK:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, struct, when, lit, udf
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
    from pyspark.sql import DataFrame
    import pyspark.sql.functions as F
else:
    from kafka import KafkaConsumer
```

**Giải thích:**
- Chỉ import Spark khi cần thiết để giảm dependencies
- Có fallback về KafkaConsumer nếu Spark không khả dụng

### 3.3. Cấu hình giá và VIP

```python
# Cấu hình giá
PRICE_PER_10MIN_BLOCK = int(os.getenv("PRICE_PER_10MIN_BLOCK", "5000"))  # VND thường
VIP_PRICE_PER_10MIN_BLOCK = int(os.getenv("VIP_PRICE_PER_10MIN_BLOCK", "8000"))  # VND VIP

# Cấu hình danh sách vị trí và VIP
PARKING_LOCATIONS = [s.strip() for s in os.getenv("PARKING_LOCATIONS", "").split(",") if s.strip()]
VIP_LOCATIONS = set([s.strip() for s in os.getenv("VIP_LOCATIONS", "").split(",") if s.strip()])
VIP_PREFIXES = set([s.strip().upper() for s in os.getenv("VIP_PREFIXES", "F").split(",") if s.strip()])
```

**Giải thích:**
- Giá có thể cấu hình qua environment variables
- VIP được xác định theo:
  - Danh sách vị trí cụ thể (`VIP_LOCATIONS`)
  - Prefix (mặc định: F* - tất cả vị trí bắt đầu bằng F)

### 3.4. In-memory State

```python
# In-memory state: { location: {license_plate, status, enter_unix, last_update_unix, parked_seconds, fee_vnd} }
parking_state = {}
state_lock = threading.Lock()
```

**Giải thích:**
- `parking_state`: Dictionary lưu trạng thái hiện tại của từng vị trí
- `state_lock`: Thread lock để đảm bảo thread-safety khi nhiều thread cập nhật state

### 3.5. Hàm xác định VIP và tính giá

```python
def is_vip_location(location: str) -> bool:
    if location in VIP_LOCATIONS:
        return True
    loc_up = (location or "").upper()
    for prefix in VIP_PREFIXES:
        if loc_up.startswith(prefix):
            return True
    return False

def price_per_block_for(location: str) -> int:
    return VIP_PRICE_PER_10MIN_BLOCK if is_vip_location(loc) else PRICE_PER_10MIN_BLOCK
```

**Giải thích:**
- Kiểm tra VIP bằng cách so sánh với danh sách hoặc prefix
- Trả về giá tương ứng (VIP hoặc thường)

### 3.6. Tính phí đỗ xe

```python
def compute_fee_seconds(seconds: int, unit_price: int) -> (int, int):
    """
    tính tiền theo block 10 phút
    seconds -> minutes, blocks, fee
    """
    minutes = (seconds + 59) // 60  # ceil để được phút nguyên
    blocks = (minutes + 9) // 10    # mỗi block 10 phút, ceil
    fee = blocks * unit_price
    return blocks, fee
```

**Giải thích:**
- Chuyển seconds thành minutes (làm tròn lên)
- Chia thành blocks 10 phút (làm tròn lên)
- Tính phí = số blocks × giá mỗi block
- Ví dụ: 23 phút = 3 blocks → 3 × 5000 = 15,000 VND

### 3.7. Xử lý Event

```python
def handle_event(event: dict):
    """
    event: {"timestamp": "YYYY-mm-dd ...", "timestamp_unix": 123456, 
            "license_plate": "...", "location":"A1", "status_code":"ENTERING"}
    Note: Hàm này cần được gọi trong context có state_lock
    """
    ts = int(event.get("timestamp_unix", time.time()))
    lp = event.get("license_plate")
    loc = event.get("location")
    status = event.get("status_code")
    
    # Giả định đã được lock ở process_batch, không lock lại ở đây
    current = parking_state.get(loc, {
        "license_plate": None,
        "status": None,
        "enter_unix": None,
        "last_update_unix": None,
        "parked_seconds": 0,
        "fee_vnd": 0
    })
    
    prev_status = current["status"]
    
    if status == "ENTERING":
        # Đặt trạng thái tạm thời (vẫn chưa đỗ)
        current["license_plate"] = lp
        current["status"] = "ENTERING"
        current["last_update_unix"] = ts
        parking_state[loc] = current
        
    elif status == "PARKED":
        # Nếu mới vào PARKED thì set enter_unix = ts (lần đầu)
        if current.get("enter_unix") is None or prev_status != "PARKED" or current["license_plate"] != lp:
            current["enter_unix"] = ts
            current["parked_seconds"] = 0
            current["license_plate"] = lp
        current["status"] = "PARKED"
        current["last_update_unix"] = ts
        parking_state[loc] = current
        
    elif status == "MOVING":
        # Giữ nguyên enter_unix — moving before exiting
        current["status"] = "MOVING"
        current["last_update_unix"] = ts
        # Tính parked seconds và phí
        if current.get("enter_unix"):
            current["parked_seconds"] = ts - current["enter_unix"]
            _, fee = compute_fee_seconds(current["parked_seconds"], price_per_block_for(loc))
            current["fee_vnd"] = fee
        parking_state[loc] = current
        
    elif status == "EXITING":
        # Finalize: tính tổng, sau đó clear slot
        if current.get("enter_unix"):
            current["parked_seconds"] = ts - current["enter_unix"]
            _, fee = compute_fee_seconds(current["parked_seconds"], price_per_block_for(loc))
            current["fee_vnd"] = fee
        current["status"] = "EXITING"
        current["last_update_unix"] = ts
        
        # Lưu lịch sử vào Cassandra nếu cần
        if USE_CASSANDRA:
            try:
                session.execute("""
                    INSERT INTO parking.history (event_id, event_time_unix, license_plate, location, status)
                    VALUES (uuid(), %s, %s, %s, %s)
                """, (ts, lp, loc, status))
            except Exception as e:
                print("Cassandra write error:", e)
        
        # Giải phóng slot
        parking_state.pop(loc, None)
```

**Giải thích:**
- **ENTERING**: Xe đang vào, chưa đỗ → chỉ cập nhật trạng thái
- **PARKED**: Xe đã đỗ → set `enter_unix` để bắt đầu tính thời gian
- **MOVING**: Xe đang di chuyển → tính phí tạm thời
- **EXITING**: Xe ra khỏi bãi → tính phí cuối cùng, lưu lịch sử, xóa khỏi state

---

## 4. XỬ LÝ DỮ LIỆU VỚI SPARK STREAMING

### 4.1. Khởi tạo Spark Session

```python
def get_spark_session():
    """Lazy initialization của SparkSession"""
    global spark_session
    if spark_session is None:
        spark_session = SparkSession.builder \
            .appName("ParkingConsumerService") \
            .master(SPARK_MASTER) \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint-parking") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0") \
            .config("spark.sql.streaming.streamingQueryListeners", "") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.default.parallelism", "2") \
            .getOrCreate()
        spark_session.sparkContext.setLogLevel("WARN")
    return spark_session
```

**Giải thích:**
- **Lazy initialization**: Chỉ tạo SparkSession khi cần
- **checkpointLocation**: Lưu checkpoint để đảm bảo fault tolerance
- **spark.jars.packages**: Tự động tải Kafka connector
- **Adaptive execution**: Tự động tối ưu số partitions
- **Giảm partitions**: Giảm overhead cho workload nhỏ

### 4.2. Spark Structured Streaming

```python
def spark_consumer_loop():
    """Spark Structured Streaming để đọc và xử lý từ Kafka"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            spark = get_spark_session()
            print(f"Spark session created with master: {SPARK_MASTER}")
            
            # Schema cho event từ Kafka
            event_schema = StructType([
                StructField("timestamp", StringType(), True),
                StructField("timestamp_unix", LongType(), True),
                StructField("license_plate", StringType(), True),
                StructField("location", StringType(), True),
                StructField("status_code", StringType(), True)
            ])
            
            # Đọc từ Kafka
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", BOOTSTRAP) \
                .option("subscribe", TOPIC) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON từ value
            df_parsed = df.select(
                from_json(col("value").cast("string"), event_schema).alias("data")
            ).select("data.*").filter(col("timestamp_unix").isNotNull())
```

**Giải thích:**
- **readStream**: Đọc streaming từ Kafka
- **Schema**: Định nghĩa cấu trúc JSON để parse
- **from_json**: Parse JSON string thành structured data
- **filter**: Lọc các record hợp lệ

### 4.3. Batch Processing

```python
# Xử lý batch và cập nhật state
def process_batch(batch_df, batch_id):
    """Process mỗi batch và cập nhật parking_state"""
    try:
        # Sử dụng isEmpty() thay vì count() để nhanh hơn
        if not batch_df.isEmpty():
            # Convert to list ngay để tránh collect() nhiều lần
            rows = batch_df.collect()
            # Batch process tất cả events cùng lúc
            events = []
            for row in rows:
                if row is not None:
                    try:
                        event = {
                            "timestamp": row.timestamp if row.timestamp else None,
                            "timestamp_unix": int(row.timestamp_unix) if row.timestamp_unix else int(time.time()),
                            "license_plate": row.license_plate if row.license_plate else None,
                            "location": row.location if row.location else None,
                            "status_code": row.status_code if row.status_code else None
                        }
                        if event.get("location") and event.get("status_code"):
                            events.append(event)
                    except AttributeError as ae:
                        print(f"Error accessing row attributes in batch {batch_id}: {ae}")
                        continue
            
            # Xử lý tất cả events trong một lần lock
            if events:
                with state_lock:
                    for event in events:
                        handle_event(event)
    except Exception as e:
        print(f"Error processing batch {batch_id}:", e)
        import traceback
        traceback.print_exc()

# Stream query với trigger - tăng interval để giảm tải
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", "/tmp/spark-checkpoint-parking") \
    .option("maxFilesPerTrigger", "100") \
    .start()

print(f"Spark streaming started, listening on topic: {TOPIC}")
query.awaitTermination()
```

**Giải thích:**
- **foreachBatch**: Xử lý mỗi batch riêng biệt
- **Batch processing**: Gom tất cả events trong một lần lock để giảm contention
- **trigger**: Xử lý mỗi 5 giây (giảm tải so với 2 giây)
- **checkpoint**: Lưu progress để đảm bảo exactly-once processing
- **awaitTermination**: Chờ streaming chạy liên tục

### 4.4. Fallback Mechanism

```python
except Exception as e:
    retry_count += 1
    print(f"Spark consumer error (attempt {retry_count}/{max_retries}): {e}")
    import traceback
    traceback.print_exc()
    if retry_count >= max_retries:
        print("Max retries reached. Falling back to Kafka consumer...")
        kafka_consumer_loop()
        return
    time.sleep(5)
```

**Giải thích:**
- Nếu Spark gặp lỗi, tự động fallback về KafkaConsumer
- Đảm bảo hệ thống vẫn hoạt động ngay cả khi Spark không khả dụng

---

## 5. API ENDPOINTS

### 5.1. GET /state

```python
@app.get("/state")
def get_state():
    """
    Trả về trạng thái hiện tại toàn bộ vị trí.
    """
    with state_lock:
        # build a copy and compute live parked_seconds & fee for PARKED
        result = []
        now = int(time.time())
        for loc in sorted([*parking_state.keys()]):
            cur = parking_state[loc].copy()
            if cur["status"] == "PARKED" and cur.get("enter_unix"):
                cur["parked_seconds"] = now - cur["enter_unix"]
                _, cur["fee_vnd"] = compute_fee_seconds(cur["parked_seconds"], price_per_block_for(loc))
            cur["is_vip"] = is_vip_location(loc)
            result.append({"location": loc, **cur})
    return JSONResponse(result)
```

**Giải thích:**
- Trả về JSON array chứa tất cả vị trí đang có xe
- Tính toán real-time `parked_seconds` và `fee_vnd` cho xe đang PARKED
- Thêm flag `is_vip` để phân biệt VIP

**Response Example:**
```json
[
  {
    "location": "A1",
    "license_plate": "29A-12345",
    "status": "PARKED",
    "enter_unix": 1700123456,
    "last_update_unix": 1700123456,
    "parked_seconds": 300,
    "fee_vnd": 5000,
    "is_vip": false
  }
]
```

### 5.2. GET /locations_free

```python
@app.get("/locations_free")
def locations_free():
    """
    Trả về list vị trí trống -- số lượng vị trí ban đầu đúng theo mô phỏng 
    trong parking_json_stream.py (từ A1-F10, 60 slot)
    """
    with state_lock:
        # Danh sách vị trí chuẩn khởi đầu từ mô phỏng (khớp parking_json_stream.py)
        all_locs = {
            "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10",
            "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "B10",
            "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10",
            "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "D10",
            "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "E10",
            "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10",
        }
        used = set(parking_state.keys())
        free = sorted(list(all_locs - used))
        # Xác định VIP dựa trên cấu hình tĩnh hoặc theo prefix (F*)
        derived_vips = {loc for loc in all_locs if is_vip_location(loc)}
        vip_all = sorted(list(set(VIP_LOCATIONS) | set(derived_vips)))
        return JSONResponse({
            "used_locations": sorted(list(used)),
            "free_locations": free,
            "vip_locations": vip_all,
        })
```

**Giải thích:**
- Trả về danh sách vị trí trống, đã sử dụng, và VIP
- Tính toán bằng cách lấy difference giữa `all_locs` và `parking_state.keys()`

**Response Example:**
```json
{
  "used_locations": ["A1", "B2", "F3"],
  "free_locations": ["A2", "A3", ..., "F10"],
  "vip_locations": ["F1", "F2", "F3", ..., "F10"]
}
```

### 5.3. GET / (Dashboard)

Trả về HTML dashboard với:
- Bảng hiển thị xe đang đỗ (real-time)
- Bảng lịch sử xe đã rời bãi
- Sơ đồ bãi đỗ xe (A1-F10) với màu sắc
- Auto-refresh mỗi 2 giây

---

## 6. DASHBOARD WEB

### 6.1. Tính năng

1. **Bảng xe đang đỗ**: Hiển thị real-time với auto-refresh
2. **Lịch sử xe đã rời**: Lưu trong `window.historyMap` (client-side)
3. **Sơ đồ bãi đỗ**: 
   - Màu xanh: Vị trí trống
   - Màu đỏ: Vị trí có xe
4. **Thông tin phí**: Hiển thị giá thường và VIP

### 6.2. JavaScript Auto-refresh

```javascript
async function refresh() {
  try {
    const [stateRes, locRes] = await Promise.all([
      fetch('/state'),
      fetch('/locations_free')
    ]);
    const state = await stateRes.json();
    const locs = await locRes.json();
    
    // Cập nhật UI
    // ...
  } catch (e) {
    console.error(e);
  }
}

refresh();
setInterval(refresh, 2000); // Refresh mỗi 2 giây
```

---

## 7. CẤU HÌNH VÀ TRIỂN KHAI

### 7.1. Environment Variables

```bash
# Spark
USE_SPARK=true
SPARK_MASTER=spark://192.168.80.105:7077

# Kafka
KAFKA_BOOTSTRAP=192.168.80.105:9092
KAFKA_TOPIC=parking-lot

# Giá
PRICE_PER_10MIN_BLOCK=5000
VIP_PRICE_PER_10MIN_BLOCK=8000

# VIP
VIP_PREFIXES=F

# Cassandra (optional)
USE_CASSANDRA=false
CASSANDRA_HOST=127.0.0.1
```

### 7.2. Cài đặt Dependencies

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```
fastapi>=0.104.0
uvicorn[standard]>=0.24.0
pyspark>=3.5.0
kafka-python>=2.0.2
cassandra-driver>=3.28.0
```

### 7.3. Chạy Service

```bash
# Chạy với Spark (mặc định)
python parking_consumer_service.py

# Hoặc với environment variables
SPARK_MASTER=spark://192.168.80.105:7077 \
KAFKA_BOOTSTRAP=192.168.80.105:9092 \
python parking_consumer_service.py

# Chạy không dùng Spark (fallback)
USE_SPARK=false python parking_consumer_service.py
```

### 7.4. Truy cập

- **Dashboard**: http://localhost:8000
- **API State**: http://localhost:8000/state
- **API Locations**: http://localhost:8000/locations_free

---

## 8. KẾT LUẬN

### 8.1. Ưu điểm

1. **Real-time Processing**: Spark Structured Streaming xử lý dữ liệu real-time
2. **Scalability**: Spark có thể scale horizontal
3. **Fault Tolerance**: Checkpoint đảm bảo không mất dữ liệu
4. **Flexibility**: Có thể fallback về KafkaConsumer nếu Spark lỗi
5. **Dashboard**: Giao diện web real-time dễ sử dụng

### 8.2. Tối ưu hóa đã thực hiện

1. **Batch Processing**: Xử lý nhiều events trong một lần lock
2. **Adaptive Execution**: Spark tự động tối ưu partitions
3. **Giảm Partitions**: Giảm overhead cho workload nhỏ
4. **Trigger Interval**: Tăng từ 2s lên 5s để giảm tải

### 8.3. Hạn chế và cải thiện

1. **In-memory State**: Dữ liệu mất khi restart → Có thể lưu vào Redis/Cassandra
2. **Single Instance**: Chưa hỗ trợ multi-instance → Cần distributed state store
3. **History**: Lưu client-side → Nên lưu vào database

### 8.4. Ứng dụng thực tế

Hệ thống có thể áp dụng cho:
- Bãi đỗ xe thông minh
- Quản lý giao thông
- Hệ thống tính phí tự động
- Monitoring và analytics real-time

---

## PHỤ LỤC: CODE ĐẦY ĐỦ

Xem file `parking_consumer_service.py` để có code đầy đủ.

---

**Ngày tạo báo cáo**: 2024-11-04  
**Tác giả**: Hệ thống Parking Real-time với Spark Streaming

