from flask import Flask, jsonify, request
from flask_cors import CORS
from pyspark.sql import SparkSession
import json
import threading
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Global variable để lưu dữ liệu mới nhất
latest_parking_data = {
    "data": [],
    "stats": {
        "total": 60,
        "occupied": 0,
        "available": 60,
        "totalRevenue": 0
    },
    "last_update": None,
    "error": None
}

# Spark Session global
spark = None
is_spark_ready = False

def init_spark():
    """Khởi tạo Spark Session và kết nối với memory table"""
    global spark, is_spark_ready
    
    try:
        logger.info("Initializing Spark Session...")
        
        spark = SparkSession.builder \
            .appName("ParkingAPIServer") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_api") \
            .config("spark.sql.shuffle.partitions", "2") \
            .master("local[*]") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        logger.info("Spark Session initialized successfully")
        is_spark_ready = True
        
        # Đợi memory table được tạo bởi streaming query
        logger.info("Waiting for streaming query to create memory table...")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize Spark: {e}")
        latest_parking_data["error"] = str(e)
        return False

def update_parking_data():
    """Background thread để update dữ liệu từ Spark memory table"""
    global latest_parking_data, spark, is_spark_ready
    
    # Đợi Spark khởi tạo
    while not is_spark_ready:
        time.sleep(1)
    
    logger.info("Starting data update loop...")
    retry_count = 0
    max_retries = 5
    
    while True:
        try:
            if spark and is_spark_ready:
                # Kiểm tra xem table có tồn tại không
                tables = [t.name for t in spark.catalog.listTables()]
                
                if 'parking_realtime' not in tables:
                    if retry_count < max_retries:
                        logger.warning(f"Memory table 'parking_realtime' not found yet. Retry {retry_count + 1}/{max_retries}")
                        retry_count += 1
                        time.sleep(5)
                        continue
                    else:
                        logger.error("Memory table not found after max retries. Make sure streaming query is running!")
                        latest_parking_data["error"] = "Memory table not found. Is the streaming query running?"
                        time.sleep(10)
                        retry_count = 0
                        continue
                
                # Reset retry counter khi tìm thấy table
                retry_count = 0
                
                # Query dữ liệu từ memory table
                df = spark.sql("""
                    SELECT 
                        location,
                        license_plate,
                        current_status,
                        entry_timestamp,
                        parked_minutes,
                        total_fee,
                        last_update
                    FROM parking_realtime
                    WHERE current_status IS NOT NULL
                    ORDER BY location, license_plate
                """)
                
                rows = df.collect()
                
                data = []
                total_revenue = 0
                
                for row in rows:
                    item = {
                        "location": row.location,
                        "license_plate": row.license_plate,
                        "current_status": row.current_status,
                        "entry_timestamp": row.entry_timestamp,
                        "parked_minutes": row.parked_minutes if row.parked_minutes else 0,
                        "total_fee": row.total_fee if row.total_fee else 0
                    }
                    data.append(item)
                    total_revenue += item["total_fee"]
                
                # Update global data
                latest_parking_data["data"] = data
                latest_parking_data["stats"] = {
                    "total": 60,
                    "occupied": len(data),
                    "available": 60 - len(data),
                    "totalRevenue": total_revenue
                }
                latest_parking_data["last_update"] = int(time.time())
                latest_parking_data["error"] = None
                
                logger.info(f"Updated: {len(data)} vehicles, Revenue: {total_revenue:,} VND")
                
        except Exception as e:
            logger.error(f"Error updating data: {e}")
            latest_parking_data["error"] = str(e)
        
        time.sleep(5)

@app.route('/api/parking/status', methods=['GET'])
def get_parking_status():
    """API endpoint để lấy trạng thái bãi đỗ xe"""
    return jsonify(latest_parking_data)

@app.route('/api/parking/summary', methods=['GET'])
def get_parking_summary():
    """API endpoint để lấy thống kê tổng quan"""
    return jsonify({
        "stats": latest_parking_data["stats"],
        "last_update": latest_parking_data["last_update"],
        "error": latest_parking_data["error"]
    })

@app.route('/api/parking/location/<location>', methods=['GET'])
def get_location_detail(location):
    """API endpoint để lấy chi tiết một vị trí"""
    data = latest_parking_data["data"]
    location_data = [item for item in data if item["location"] == location.upper()]
    
    if location_data:
        return jsonify(location_data[0])
    else:
        return jsonify({
            "location": location.upper(),
            "status": "available",
            "message": "Vị trí trống"
        })

@app.route('/api/parking/floor/<floor>', methods=['GET'])
def get_floor_summary(floor):
    """API endpoint để lấy thống kê theo tầng"""
    data = latest_parking_data["data"]
    floor_data = [item for item in data if item["location"].startswith(floor.upper())]
    
    total_revenue = sum(item["total_fee"] for item in floor_data)
    
    return jsonify({
        "floor": floor.upper(),
        "total_slots": 10,
        "occupied": len(floor_data),
        "available": 10 - len(floor_data),
        "vehicles": floor_data,
        "total_revenue": total_revenue
    })

@app.route('/api/parking/query', methods=['POST'])
def custom_query():
    """API endpoint để chạy SQL query tùy chỉnh (be careful!)"""
    global spark, is_spark_ready
    
    if not is_spark_ready:
        return jsonify({"error": "Spark not ready"}), 503
    
    try:
        query = request.json.get('query', '')
        
        # Simple validation
        if not query.upper().startswith('SELECT'):
            return jsonify({"error": "Only SELECT queries allowed"}), 400
        
        df = spark.sql(query)
        rows = df.collect()
        
        result = [row.asDict() for row in rows]
        
        return jsonify({
            "query": query,
            "result": result,
            "count": len(result)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    global spark, is_spark_ready
    
    spark_status = "ready" if is_spark_ready else "not ready"
    
    tables = []
    if spark and is_spark_ready:
        try:
            tables = [t.name for t in spark.catalog.listTables()]
        except:
            pass
    
    return jsonify({
        "status": "ok",
        "spark_status": spark_status,
        "available_tables": tables,
        "last_update": latest_parking_data["last_update"],
        "vehicles_count": len(latest_parking_data["data"]),
        "error": latest_parking_data["error"]
    })

@app.route('/', methods=['GET'])
def index():
    """Root endpoint"""
    return jsonify({
        "service": "Parking Management API",
        "version": "1.0",
        "endpoints": [
            "GET /api/parking/status",
            "GET /api/parking/summary", 
            "GET /api/parking/location/<location>",
            "GET /api/parking/floor/<floor>",
            "POST /api/parking/query",
            "GET /api/health"
        ]
    })

if __name__ == '__main__':
    print("=" * 80)
    print("Starting Parking Management API Server")
    print("=" * 80)
    
    # Khởi tạo Spark trong background thread
    logger.info("Starting Spark initialization thread...")
    spark_thread = threading.Thread(target=init_spark, daemon=True)
    spark_thread.start()
    
    # Đợi một chút để Spark khởi động
    time.sleep(3)
    
    # Start background update thread
    logger.info("Starting data update thread...")
    update_thread = threading.Thread(target=update_parking_data, daemon=True)
    update_thread.start()
    
    print("\n" + "=" * 80)
    print("API Server Ready!")
    print("=" * 80)
    print("\nAPI Endpoints:")
    print("  GET  http://localhost:5000/")
    print("  GET  http://localhost:5000/api/parking/status")
    print("  GET  http://localhost:5000/api/parking/summary")
    print("  GET  http://localhost:5000/api/parking/location/<location>")
    print("  GET  http://localhost:5000/api/parking/floor/<floor>")
    print("  POST http://localhost:5000/api/parking/query")
    print("  GET  http://localhost:5000/api/health")
    print("=" * 80)
    print("\nNote: Make sure Spark streaming query is running first!")
    print("      Run: python parking_stateful_processor.py")
    print("=" * 80 + "\n")
    
    # Run Flask
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)