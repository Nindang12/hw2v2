"""
FILE: parking_api_server.py
MÔ TẢ: Flask API Server - Đọc data từ Spark Memory Table
CHẠY: python parking_api_server.py
YÊU CẦU: parking_spark_processor.py phải đang chạy trước

FIX: Xử lý đúng stats khi GroupBy (location, license_plate)
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import threading
import time
import logging
import os
import json
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===========================
# FLASK APP SETUP
# ===========================
app = Flask(__name__)
CORS(app)

# Configuration files
CONFIG_FILE = "/opt/shared/parking_config.json"
LEGACY_PRICE_CONFIG_FILE = "/opt/shared/parking_price_config.json"  # Backward compatibility
CHECKOUT_STATUS_FILE = "/opt/shared/parking_checkout_status.json"

# Default values
DEFAULT_CONFIG = {
    "price": {"price_per_10min": 5000, "currency": "VND"},
    "parking": {"total_slots": 60, "slots_per_floor": 10},
    "api": {"host": "0.0.0.0", "port": 5000, "refresh_interval_seconds": 5},
    "spark": {"master": "spark://192.168.80.98:7077", "shuffle_partitions": 2}
}

# Spark Session global
spark = None
is_spark_ready = False

# ===========================
# CONFIG MANAGEMENT
# ===========================
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
                        if isinstance(config[key], dict) and isinstance(file_config[key], dict):
                            config[key].update(file_config[key])
                        else:
                            config[key] = file_config[key]
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

def load_price_config():
    """Load giá từ config (backward compatibility)"""
    config = load_config()
    return config["price"]["price_per_10min"]

def save_price_config(price):
    """Cập nhật giá trong config file"""
    config = load_config()
    config["price"]["price_per_10min"] = int(price)
    config["price"]["updated_at"] = datetime.now().isoformat()
    return save_config(config)

# ===========================
# Load config on startup
# ===========================
app_config = load_config()
TOTAL_SLOTS = app_config["parking"]["total_slots"]
SLOTS_PER_FLOOR = app_config["parking"]["slots_per_floor"]
API_PORT = app_config["api"]["port"]
API_HOST = app_config["api"]["host"]
SPARK_MASTER = app_config["spark"]["master"]
SHUFFLE_PARTITIONS = app_config["spark"]["shuffle_partitions"]
REFRESH_INTERVAL = app_config["api"]["refresh_interval_seconds"]

# ===========================
# GLOBAL DATA STORAGE
# ===========================
latest_parking_data = {
    "data": [],
    "stats": {
        "total": TOTAL_SLOTS,
        "occupied_locations": 0,  # Số vị trí bị chiếm
        "unique_vehicles": 0,      # Số xe unique
        "available": TOTAL_SLOTS
    },
    "last_update": None,
    "error": None
}

def load_checkout_status():
    """Load danh sách xe đã checkout"""
    try:
        if os.path.exists(CHECKOUT_STATUS_FILE):
            with open(CHECKOUT_STATUS_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error loading checkout status: {e}")
    return {}

def save_checkout_status(checkout_dict):
    """Lưu danh sách xe đã checkout"""
    try:
        os.makedirs(os.path.dirname(CHECKOUT_STATUS_FILE), exist_ok=True)
        with open(CHECKOUT_STATUS_FILE, 'w') as f:
            json.dump(checkout_dict, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving checkout status: {e}")
        return False

def mark_vehicle_checkout(license_plate, location, total_fee, parked_minutes):
    """Đánh dấu xe đã checkout"""
    checkout_status = load_checkout_status()
    key = f"{license_plate}_{location}"
    checkout_status[key] = {
        "license_plate": license_plate,
        "location": location,
        "total_fee": total_fee,
        "parked_minutes": parked_minutes,
        "checkout_time": datetime.now().isoformat()
    }
    return save_checkout_status(checkout_status)

# ===========================
# SPARK INITIALIZATION
# ===========================
def init_spark():
    """Khởi tạo Spark Session để kết nối với Memory Table"""
    global spark, is_spark_ready
    
    try:
        logger.info("Initializing Spark Session for API Server...")
        
        spark = SparkSession.builder \
            .appName("ParkingAPIServer") \
            .master(SPARK_MASTER) \
            .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS)) \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        # Test connection
        try:
            test_df = spark.sql("SELECT 1 as test")
            test_df.collect()
            logger.info("✓ Spark connection test successful")
        except Exception as e:
            logger.warning(f"Spark connection test failed: {e}")
        
        is_spark_ready = True
        logger.info("✓ Spark Session initialized successfully")
        
    except Exception as e:
        logger.error(f"✗ Failed to initialize Spark: {e}")
        import traceback
        logger.error(traceback.format_exc())
        latest_parking_data["error"] = f"Spark init error: {str(e)}"
        is_spark_ready = False

# ===========================
# DATA UPDATE LOOP
# ===========================
def update_parking_data():
    """Background thread - Query Spark Memory Table mỗi 5 giây"""
    global latest_parking_data, spark, is_spark_ready
    
    logger.info("Waiting for Spark to be ready...")
    while not is_spark_ready:
        time.sleep(1)
    
    logger.info("✓ Starting data update loop...")
    retry_count = 0
    max_retries = 10
    
    while True:
        try:
            if spark and is_spark_ready:
                # FIX: Memory table không share được giữa SparkSessions
                # Dùng Parquet file để đọc data từ processor
                parquet_path = "/opt/shared/parking_realtime_parquet"
                
                # Kiểm tra xem có file Parquet không
                if not os.path.exists(parquet_path):
                    if retry_count < max_retries:
                        logger.warning(
                            f"Parquet file not found at {parquet_path}. "
                            f"Retry {retry_count + 1}/{max_retries}"
                        )
                        retry_count += 1
                        time.sleep(REFRESH_INTERVAL)
                        continue
                    else:
                        error_msg = (
                            f"Parquet file not found after {max_retries} retries. "
                            f"Make sure parking_spark_processor.py is running and writing to {parquet_path}!"
                        )
                        logger.error(error_msg)
                        latest_parking_data["error"] = error_msg
                        time.sleep(10)
                        retry_count = 0
                        continue
                
                if retry_count > 0:
                    logger.info("✓ Parquet file found!")
                    retry_count = 0
                
                # Đọc dữ liệu từ Parquet file
                try:
                    # FIX: Retry nếu file đang được ghi (race condition)
                    max_read_retries = 3
                    df = None
                    last_error = None
                    
                    for read_retry in range(max_read_retries):
                        try:
                            # Kiểm tra xem có file không trước khi đọc
                            if not os.path.exists(parquet_path):
                                if read_retry < max_read_retries - 1:
                                    logger.debug(f"Parquet file not ready, retry {read_retry + 1}/{max_read_retries}")
                                    time.sleep(1)
                                    continue
                                else:
                                    # Nếu file không tồn tại sau nhiều lần retry, skip và dùng data cũ
                                    logger.warning(f"Parquet file not found after {max_read_retries} retries, using cached data")
                                    continue
                            
                            # FIX: Tạo DataFrame mới mỗi lần, không cache
                            # Sử dụng unpersist() và tạo DataFrame mới để tránh stale references
                            try:
                                # Clear any cached data
                                spark.catalog.clearCache()
                            except:
                                pass
                            
                            # Đọc file với error handling tốt hơn
                            # FIX: Đọc lại từ đầu mỗi lần, không cache để tránh stale file references
                            # Sử dụng option để không cache
                            df = spark.read.option("recursiveFileLookup", "true").parquet(parquet_path)
                            
                            # Test đọc một vài rows để đảm bảo file hợp lệ
                            # FIX: Dùng try-except riêng cho collect() để catch lỗi file not exist
                            try:
                                test_rows = df.limit(1).collect()
                                # Nếu thành công, break khỏi retry loop
                                break
                            except Exception as collect_error:
                                error_str = str(collect_error)
                                # Nếu là lỗi file not exist, retry
                                if any(keyword in error_str for keyword in [
                                    "FILE_NOT_EXIST",
                                    "File does not exist",
                                    "underlying files have been updated"
                                ]):
                                    if read_retry < max_read_retries - 1:
                                        logger.debug(
                                            f"File being updated during read (attempt {read_retry + 1}/{max_read_retries}), retrying..."
                                        )
                                        time.sleep(2)
                                        continue
                                    else:
                                        # Nếu retry hết, skip và dùng data cũ
                                        logger.warning(f"File still being updated after {max_read_retries} retries, skipping this update")
                                        df = None
                                        break
                                else:
                                    # Lỗi khác, raise ngay
                                    raise collect_error
                            
                        except Exception as read_error:
                            last_error = read_error
                            error_str = str(read_error)
                            
                            # Retry nếu file đang được ghi hoặc không tồn tại
                            if any(keyword in error_str for keyword in [
                                "does not exist", 
                                "FileNotFoundException",
                                "FAILED_READ_FILE",
                                "FILE_NOT_EXIST",
                                "underlying files have been updated",
                                "File does not exist"
                            ]):
                                if read_retry < max_read_retries - 1:
                                    logger.debug(
                                        f"Parquet file being written or updated (attempt {read_retry + 1}/{max_read_retries}): {error_str[:100]}"
                                    )
                                    # Tăng thời gian chờ giữa các retry
                                    time.sleep(2)
                                    continue
                                else:
                                    # Nếu retry hết, skip và dùng data cũ
                                    logger.warning(f"File still being updated after {max_read_retries} retries, skipping this update")
                                    df = None
                                    break
                            
                            # Nếu không phải lỗi file not found, raise ngay
                            if read_retry == max_read_retries - 1:
                                raise read_error
                    
                    # Nếu không đọc được file (df is None), skip update này và giữ data cũ
                    if df is None:
                        logger.debug("Skipping data update due to file being written, keeping existing data")
                        time.sleep(REFRESH_INTERVAL)
                        continue
                    
                    # Filter và select columns
                    # NOTE: Parquet bao gồm cả EXITING, API có thể filter nếu cần
                    try:
                        df_filtered = df.filter(
                            col("current_status").isNotNull()
                        ).select(
                            "location",
                            "license_plate",
                            "current_status",
                            "entry_timestamp",
                            "parked_minutes",
                            "total_fee",
                            "last_update"
                        ).orderBy("location", "license_plate")
                        
                        # FIX: Wrap collect() trong try-except để catch lỗi file not exist
                        rows = df_filtered.collect()
                    except Exception as collect_error:
                        error_str = str(collect_error)
                        if any(keyword in error_str for keyword in [
                            "FILE_NOT_EXIST",
                            "File does not exist",
                            "underlying files have been updated"
                        ]):
                            logger.warning(f"File was updated during processing, skipping this update: {error_str[:100]}")
                            time.sleep(REFRESH_INTERVAL)
                            continue
                        else:
                            # Lỗi khác, raise
                            raise collect_error
                    
                    # FIX: Filter bỏ những xe đã checkout
                    checkout_status = load_checkout_status()
                    rows_filtered = []
                    for row in rows:
                        key = f"{row.license_plate}_{row.location}"
                        if key not in checkout_status:
                            rows_filtered.append(row)
                    rows = rows_filtered
                except Exception as e:
                    logger.error(f"Error querying memory table: {e}")
                    latest_parking_data["error"] = f"Error querying table: {str(e)}"
                    time.sleep(REFRESH_INTERVAL)
                    continue
                
                # Convert Spark Rows sang Python dict
                data = []
                unique_vehicles = set()  # Track unique license plates
                
                for row in rows:
                    item = {
                        "location": row.location if row.location else None,
                        "license_plate": row.license_plate if row.license_plate else None,
                        "current_status": row.current_status if row.current_status else None,
                        "entry_timestamp": row.entry_timestamp if row.entry_timestamp else None,
                        "parked_minutes": int(row.parked_minutes) if row.parked_minutes is not None else 0,
                        "total_fee": int(row.total_fee) if row.total_fee is not None else 0,
                        "last_update": int(row.last_update) if row.last_update is not None else None
                    }
                    data.append(item)
                    
                    # Track unique vehicles
                    if item["license_plate"]:
                        unique_vehicles.add(item["license_plate"])
                
                # Update global data
                latest_parking_data["data"] = data
                latest_parking_data["stats"] = {
                    "total": TOTAL_SLOTS,
                    "occupied_locations": len(data),      # Số records = số vị trí
                    "unique_vehicles": len(unique_vehicles),  # Số xe unique
                    "available": TOTAL_SLOTS - len(data)          # Số vị trí trống
                }
                latest_parking_data["last_update"] = int(time.time())
                latest_parking_data["error"] = None
                
                if len(data) > 0:
                    logger.info(
                        f"Updated: {len(data)} records, "
                        f"{len(unique_vehicles)} unique vehicles"
                    )
                
        except Exception as e:
            logger.error(f"Error updating data: {e}")
            latest_parking_data["error"] = str(e)
        
        time.sleep(REFRESH_INTERVAL)

# ===========================
# API ENDPOINTS
# ===========================

@app.route('/', methods=['GET'])
def index():
    """Root endpoint - API info"""
    return jsonify({
        "service": "Parking Management API",
        "version": "1.0",
        "description": "Real-time parking fee calculator API",
        "note": "GroupBy (location, license_plate) - One vehicle may have multiple records",
        "endpoints": [
            "GET /api/parking/status - Get all parking status",
            "GET /api/parking/summary - Get parking summary",
            "GET /api/parking/location/<location> - Get specific location",
            "GET /api/parking/floor/<floor> - Get floor summary",
            "GET /api/parking/vehicle/<license_plate> - Get vehicle info (all locations)",
            "GET /api/health - Health check"
        ]
    })

@app.route('/api/parking/status', methods=['GET'])
def get_parking_status():
    """Lấy toàn bộ trạng thái bãi đỗ xe"""
    return jsonify(latest_parking_data)

@app.route('/api/parking/summary', methods=['GET'])
def get_parking_summary():
    """Lấy thống kê tổng quan"""
    return jsonify({
        "stats": latest_parking_data["stats"],
        "last_update": latest_parking_data["last_update"],
        "error": latest_parking_data["error"]
    })

@app.route('/api/parking/location/<location>', methods=['GET'])
def get_location_detail(location):
    """Lấy chi tiết một vị trí cụ thể (ví dụ: A1, B5)"""
    data = latest_parking_data["data"]
    location_upper = location.upper()
    location_data = [item for item in data if item["location"] == location_upper]
    
    if location_data:
        # Trả về record đầu tiên (có thể có nhiều xe cùng vị trí)
        return jsonify({
            "location": location_upper,
            "vehicles": location_data,
            "count": len(location_data)
        })
    else:
        return jsonify({
            "location": location_upper,
            "status": "available",
            "message": "Vị trí trống",
            "vehicles": [],
            "count": 0
        })

@app.route('/api/parking/floor/<floor>', methods=['GET'])
def get_floor_summary(floor):
    """Lấy thống kê theo tầng (A, B, C, D, E, F)"""
    data = latest_parking_data["data"]
    floor_upper = floor.upper()
    floor_data = [item for item in data if item["location"] and item["location"].startswith(floor_upper)]
    
    unique_vehicles_in_floor = len(set(item["license_plate"] for item in floor_data if item["license_plate"]))
    
    return jsonify({
        "floor": floor_upper,
        "total_slots": SLOTS_PER_FLOOR,
        "occupied_locations": len(floor_data),  # Số vị trí bị chiếm
        "unique_vehicles": unique_vehicles_in_floor,  # Số xe unique
        "available": SLOTS_PER_FLOOR - len(floor_data),
        "vehicles": floor_data
    })

@app.route('/api/parking/vehicle/<license_plate>', methods=['GET'])
def get_vehicle_detail(license_plate):
    """Lấy thông tin chi tiết của một xe theo biển số (tất cả vị trí)"""
    data = latest_parking_data["data"]
    license_upper = license_plate.upper()
    vehicle_data = [item for item in data if item["license_plate"] and item["license_plate"].upper() == license_upper]
    
    if vehicle_data:
        # Tính tổng phí từ tất cả vị trí
        total_fee_all_locations = sum(item["total_fee"] for item in vehicle_data)
        
        return jsonify({
            "license_plate": license_upper,
            "locations": vehicle_data,
            "location_count": len(vehicle_data),
            "total_fee_all_locations": total_fee_all_locations,
            "note": "Vehicle may appear in multiple locations due to movement"
        })
    else:
        return jsonify({
            "license_plate": license_upper,
            "status": "not_found",
            "message": "Xe không có trong bãi đỗ hoặc đã ra khỏi bãi"
        }), 404

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
        "status": "ok" if is_spark_ready else "initializing",
        "spark_status": spark_status,
        "available_tables": tables,
        "last_update": latest_parking_data["last_update"],
        "records_count": len(latest_parking_data["data"]),
        "unique_vehicles": latest_parking_data["stats"]["unique_vehicles"],
        "error": latest_parking_data["error"]
    })

@app.route('/api/parking/checkout', methods=['POST'])
def confirm_checkout():
    """Xác nhận thanh toán cho xe EXITING"""
    try:
        data = request.get_json()
        license_plate = data.get("license_plate")
        location = data.get("location")
        
        if not license_plate or not location:
            return jsonify({"error": "license_plate and location are required"}), 400
        
        # Tìm xe trong data hiện tại
        vehicle_data = None
        for item in latest_parking_data["data"]:
            if (item["license_plate"] and item["license_plate"].upper() == license_plate.upper() and
                item["location"] and item["location"].upper() == location.upper()):
                vehicle_data = item
                break
        
        if not vehicle_data:
            return jsonify({
                "error": "Vehicle not found or already checked out",
                "license_plate": license_plate,
                "location": location
            }), 404
        
        # Chỉ cho phép checkout nếu xe đang EXITING
        if vehicle_data["current_status"] != "EXITING":
            return jsonify({
                "error": f"Vehicle is not in EXITING status. Current status: {vehicle_data['current_status']}",
                "current_status": vehicle_data["current_status"]
            }), 400
        
        # Đánh dấu xe đã checkout
        if mark_vehicle_checkout(
            license_plate.upper(),
            location.upper(),
            vehicle_data["total_fee"],
            vehicle_data["parked_minutes"]
        ):
            logger.info(
                f"Checkout confirmed: {license_plate} at {location}, "
                f"Fee: {vehicle_data['total_fee']:,} VND"
            )
            return jsonify({
                "success": True,
                "message": "Checkout confirmed",
                "vehicle": {
                    "license_plate": license_plate.upper(),
                    "location": location.upper(),
                    "total_fee": vehicle_data["total_fee"],
                    "parked_minutes": vehicle_data["parked_minutes"]
                }
            })
        else:
            return jsonify({"error": "Failed to save checkout status"}), 500
            
    except Exception as e:
        logger.error(f"Error confirming checkout: {e}")
        return jsonify({"error": str(e)}), 500

# ===========================
# MAIN ENTRY POINT
# ===========================
if __name__ == '__main__':
    print("\n" + "=" * 80)
    print("🚗 PARKING MANAGEMENT API SERVER")
    print("=" * 80)
    
    # Start Spark initialization thread
    logger.info("Starting Spark initialization thread...")
    spark_thread = threading.Thread(target=init_spark, daemon=True)
    spark_thread.start()
    
    time.sleep(3)
    
    # Start background data update thread
    logger.info("Starting data update thread...")
    update_thread = threading.Thread(target=update_parking_data, daemon=True)
    update_thread.start()
    
    print("\n" + "=" * 80)
    print("✓ API SERVER READY!")
    print("=" * 80)
    print("\n📡 API Endpoints:")
    print("  GET  http://localhost:5000/")
    print("  GET  http://localhost:5000/api/parking/status")
    print("  GET  http://localhost:5000/api/parking/summary")
    print("  GET  http://localhost:5000/api/parking/location/<location>")
    print("  GET  http://localhost:5000/api/parking/floor/<floor>")
    print("  GET  http://localhost:5000/api/parking/vehicle/<license_plate>")
    print("  GET  http://localhost:5000/api/health")
    print("=" * 80)
    print("\n⚠️  IMPORTANT:")
    print("  - Make sure parking_spark_processor.py is running!")
    print("  - GroupBy (location, license_plate): One vehicle may have multiple records")
    print("  - Stats include: occupied_locations AND unique_vehicles")
    print("  - EXITING vehicles are handled by checkout_events stream")
    print("=" * 80 + "\n")
    
    # Run Flask server
    app.run(host=API_HOST, port=API_PORT, debug=False, threaded=True)