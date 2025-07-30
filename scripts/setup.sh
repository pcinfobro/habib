#!/bin/bash

# E-commerce Data Pipeline Setup Script
# This script sets up the environment and dependencies

set -e

echo "Setting up E-commerce Data Pipeline..."

# Create directories
echo "Creating directory structure..."
mkdir -p /data/input
mkdir -p /data/output
mkdir -p /data/logs
mkdir -p /data/temp
mkdir -p /opt/pipeline/scripts
mkdir -p /opt/pipeline/config
mkdir -p /opt/pipeline/logs

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Set up Hadoop/HDFS directories
echo "Setting up HDFS directories..."
hdfs dfs -mkdir -p /data/ecommerce/raw
hdfs dfs -mkdir -p /data/ecommerce/processed
hdfs dfs -mkdir -p /data/ecommerce/archive

# Create Hive database
echo "Setting up Hive database..."
hive -e "CREATE DATABASE IF NOT EXISTS ecommerce;"

# Set up Kafka topics
echo "Setting up Kafka topics..."
kafka-topics.sh --create --topic raw-orders --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic raw-customers --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic raw-products --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic processed-orders --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic processed-customers --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
kafka-topics.sh --create --topic processed-products --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1

# Set permissions
echo "Setting permissions..."
chmod +x /opt/pipeline/scripts/*.py
chmod +x /opt/pipeline/scripts/*.sh

# Create sample data files
echo "Creating sample data files..."
cat > /data/input/orders.csv << EOF
order_id,customer_id,order_amount,order_date,product_id,quantity
ORD-001,CUST-001,150.50,2023-01-15,PROD-001,2
ORD-002,CUST-002,275.00,2023-01-16,PROD-002,1
ORD-003,CUST-001,89.99,2023-01-17,PROD-003,3
ORD-004,CUST-003,450.00,2023-01-18,PROD-001,1
EOF

cat > /data/input/products.json << EOF
{
  "products": [
    {
      "product_id": "PROD-001",
      "product_name": "Wireless Headphones",
      "price": 75.25,
      "category": "Electronics",
      "description": "High-quality wireless headphones"
    },
    {
      "product_id": "PROD-002", 
      "product_name": "Running Shoes",
      "price": 275.00,
      "category": "Sports",
      "description": "Professional running shoes"
    },
    {
      "product_id": "PROD-003",
      "product_name": "Coffee Maker",
      "price": 29.99,
      "category": "Home",
      "description": "Automatic coffee maker"
    }
  ]
}
EOF

echo "Setup completed successfully!"
echo "You can now run the pipeline using:"
echo "python /opt/pipeline/scripts/pipeline_orchestrator.py --config /opt/pipeline/config/pipeline_config.json --mode batch"
