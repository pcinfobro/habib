#!/bin/bash

# Enterprise Ecommerce Pipeline Startup Script
# ============================================

echo "🚀 Starting Enterprise Ecommerce Data Pipeline"
echo "=============================================="

# Set script to exit on any error
set -e

# Function to wait for service to be ready
wait_for_service() {
    local service_name=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    echo "⏳ Waiting for $service_name to be ready on port $port..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f http://localhost:$port/health 2>/dev/null || nc -z localhost $port 2>/dev/null; then
            echo "✅ $service_name is ready!"
            return 0
        fi
        
        echo "⌛ Attempt $attempt/$max_attempts - $service_name not ready yet..."
        sleep 10
        ((attempt++))
    done
    
    echo "❌ $service_name failed to start within expected time"
    return 1
}

# Function to check if container is running
check_container() {
    local container_name=$1
    if docker ps --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo "✅ $container_name is running"
        return 0
    else
        echo "❌ $container_name is not running"
        return 1
    fi
}

echo "📋 Step 1: Starting all services..."
docker-compose -f docker-compose-complete.yml up -d

echo "📋 Step 2: Waiting for core infrastructure..."

# Wait for Zookeeper
echo "⏳ Waiting for Zookeeper..."
sleep 20

# Wait for Kafka
echo "⏳ Waiting for Kafka..."
sleep 30

# Wait for Hadoop services
echo "⏳ Waiting for Hadoop services..."
sleep 45

# Wait for PostgreSQL databases
echo "⏳ Waiting for PostgreSQL services..."
sleep 20

echo "📋 Step 3: Verifying service health..."

# Check critical services
services_to_check=(
    "zookeeper:2181"
    "kafka:9092" 
    "namenode:9870"
    "resourcemanager:8088"
    "spark-master:8080"
    "trino:8090"
    "postgres-powerbi:5432"
    "airflow-webserver:8081"
)

all_services_ready=true

for service in "${services_to_check[@]}"; do
    service_name=$(echo $service | cut -d: -f1)
    port=$(echo $service | cut -d: -f2)
    
    if ! wait_for_service $service_name $port; then
        all_services_ready=false
    fi
done

if [ "$all_services_ready" = true ]; then
    echo "🎉 All services are running successfully!"
else
    echo "⚠️ Some services may not be fully ready yet"
fi

echo "📋 Step 4: Initializing data and topics..."

# Create Kafka topics
echo "📤 Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic ecommerce_transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>/dev/null || echo "Topic may already exist"

# Initialize HDFS directories
echo "📁 Creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /warehouse/ecommerce_streaming || echo "Directory may already exist"
docker exec namenode hdfs dfs -mkdir -p /warehouse/processed || echo "Directory may already exist"

# Copy sample data
echo "📊 Copying sample data..."
if [ -f "data/input/ecommerce_data.csv" ]; then
    docker cp data/input/ecommerce_data.csv spark-master:/opt/spark-data/input/ || echo "Data already copied"
fi

echo "📋 Step 5: Starting automation services..."

# Register sample dataset with PowerBI service
echo "📈 Registering sample dataset..."
sleep 10  # Wait for PowerBI service to be ready

# Create sample dataset registration
cat > /tmp/dataset_registration.json << EOF
{
    "name": "Ecommerce Sample Dataset",
    "source_type": "csv",
    "source_path": "/opt/spark-data/input/ecommerce_data.csv",
    "metadata": {
        "description": "Sample ecommerce transaction data for pipeline testing"
    }
}
EOF

# Register with PowerBI automation service
curl -X POST http://localhost:5000/datasets \
    -H "Content-Type: application/json" \
    -d @/tmp/dataset_registration.json || echo "Dataset registration will be done manually"

echo "📋 Step 6: Pipeline Status Summary"
echo "================================="

echo "🌐 Web Interfaces Available:"
echo "   • Hadoop HDFS:           http://localhost:9870"
echo "   • Spark Master:          http://localhost:8080" 
echo "   • Trino Query Engine:    http://localhost:8090"
echo "   • Airflow Orchestration: http://localhost:8081 (admin/admin123)"
echo "   • YARN Resource Manager: http://localhost:8088"
echo "   • PowerBI Automation:    http://localhost:5000"
echo "   • Streamlit Dashboard:   http://localhost:8501"

echo ""
echo "🔧 Service Status:"
check_container "kafka" && echo "   ✅ Kafka (Data Ingestion)" || echo "   ❌ Kafka"
check_container "namenode" && echo "   ✅ HDFS (Storage)" || echo "   ❌ HDFS"
check_container "spark-master" && echo "   ✅ Spark (Processing)" || echo "   ❌ Spark"
check_container "trino" && echo "   ✅ Trino (Query Engine)" || echo "   ❌ Trino"
check_container "airflow-webserver" && echo "   ✅ Airflow (Orchestration)" || echo "   ❌ Airflow"
check_container "powerbi-automation" && echo "   ✅ PowerBI (Automation)" || echo "   ❌ PowerBI"

echo ""
echo "🎯 Next Steps:"
echo "   1. Access Airflow at http://localhost:8081 (admin/admin123)"
echo "   2. Enable the 'enterprise_ecommerce_pipeline' DAG"
echo "   3. Trigger a manual run to test the complete pipeline"
echo "   4. Monitor progress through the web interfaces"
echo "   5. Check PowerBI automation at http://localhost:5000"

echo ""
echo "🚀 Enterprise Pipeline is Ready!"
echo "=================================="
