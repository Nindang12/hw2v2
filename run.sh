#!/bin/bash

# Script để chạy parking consumer service với Spark

# Cấu hình mặc định
export USE_SPARK=${USE_SPARK:-"true"}
export SPARK_MASTER=${SPARK_MASTER:-"spark://192.168.80.105:7077"}
export KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-"192.168.80.105:9092"}
export KAFKA_TOPIC=${KAFKA_TOPIC:-"parking-lot"}
export PRICE_PER_10MIN_BLOCK=${PRICE_PER_10MIN_BLOCK:-"5000"}
export VIP_PRICE_PER_10MIN_BLOCK=${VIP_PRICE_PER_10MIN_BLOCK:-"8000"}

echo "=========================================="
echo "Parking Consumer Service với Spark"
echo "=========================================="
echo "USE_SPARK: $USE_SPARK"
echo "SPARK_MASTER: $SPARK_MASTER"
echo "KAFKA_BOOTSTRAP: $KAFKA_BOOTSTRAP"
echo "KAFKA_TOPIC: $KAFKA_TOPIC"
echo "=========================================="
echo ""

# Chạy service
python parking_consumer_service.py

