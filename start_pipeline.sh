#!/bin/bash

# E-commerce Big Data Pipeline Startup Script
# This script starts all services and the Streamlit dashboard

set -e

echo "🚀 Starting E-commerce Big Data Pipeline..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

# Check if Docker is running
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    print_status "Docker is running ✅"
}

# Check if Docker Compose is available
check_docker_compose() {
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose is not available. Please install Docker Compose."
        exit 1
    fi
    
    print_status "Docker Compose is available ✅"
}

# Create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    mkdir -p data/input data/output data/logs data/temp
    mkdir -p hadoop-config notebooks
    print_status "Directories created ✅"
}

# Create Hadoop configuration
create_hadoop_config() {
    print_status "Creating Hadoop configuration..."
    
    cat > hadoop-config/core-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:9000</value>
    </property>
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>root</value>
    </property>
</configuration>
EOF

    cat > hadoop-config/hdfs-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>dfs.namenode.rpc-address</name>
        <value>namenode:9000</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
EOF

    print_status "Hadoop configuration created ✅"
}

# Start services
start_services() {
    print_header "Starting Big Data Services"
    
    print_status "Starting infrastructure services..."
    docker-compose up -d postgres namenode datanode
    
    print_status "Waiting for infrastructure to be ready..."
    sleep 30
    
    print_status "Starting Hadoop services..."
    docker-compose up -d namenode datanode
    
    print_status "Starting Spark services..."
    docker-compose up -d spark-master spark-worker spark-history
    
    print_status "Starting Kafka services..."
    docker-compose up -d kafka kafka-ui
    
    print_status "Starting Hive Metastore..."
    docker-compose up -d hive-metastore
    
    print_status "Starting Iceberg services..."
    docker-compose up -d iceberg-spark
    
    print_status "Waiting for all services to be ready..."
    sleep 60
    
    print_status "Starting Streamlit Dashboard..."
    docker-compose up -d streamlit-dashboard
    
    print_status "All services started! ✅"
}

# Install Python dependencies locally
install_dependencies() {
    print_status "Installing Python dependencies..."
    if command -v python3 &> /dev/null; then
        python3 -m pip install -r requirements.txt
        print_status "Python dependencies installed ✅"
    else
        print_warning "Python3 not found. Skipping local dependency installation."
    fi
}

# Run Power BI automation
run_powerbi_automation() {
    print_status "Running Power BI automation..."
    if command -v python3 &> /dev/null; then
        python3 powerbi_automation.py
        print_status "Power BI automation completed ✅"
    else
        print_warning "Python3 not found. Skipping Power BI automation."
    fi
}

# Check service status
check_services() {
    print_header "Service Status Check"
    
    services=(
        "namenode:9870:HDFS NameNode"
        "spark-master:8080:Spark Master"
        "kafka-ui:8090:Kafka UI"
        "streamlit-dashboard:8501:Streamlit Dashboard"
        "spark-history:18080:Spark History Server"
    )
    
    for service in "${services[@]}"; do
        IFS=':' read -r container port name <<< "$service"
        if curl -f -s "http://localhost:$port" > /dev/null 2>&1; then
            print_status "$name is running on port $port ✅"
        else
            print_warning "$name may not be ready on port $port ⚠️"
        fi
    done
}

# Show access URLs
show_urls() {
    print_header "Access URLs"
    echo -e "${BLUE}🌐 Streamlit Dashboard:${NC} http://localhost:8501"
    echo -e "${BLUE}🔥 Spark Master UI:${NC} http://localhost:8080"
    echo -e "${BLUE}📊 Spark History Server:${NC} http://localhost:18080"
    echo -e "${BLUE}🗂️ HDFS NameNode:${NC} http://localhost:9870"
    echo -e "${BLUE}📨 Kafka UI:${NC} http://localhost:8090"
    echo -e "${BLUE}🔍 Hive Metastore:${NC} thrift://localhost:9083"
}

# Main execution
main() {
    print_header "E-commerce Big Data Pipeline Setup"
    
    check_docker
    check_docker_compose
    create_directories
    create_hadoop_config
    install_dependencies
    start_services
    
    print_status "Waiting for services to stabilize..."
    sleep 30
    
    check_services
    run_powerbi_automation
    show_urls
    
    print_header "Setup Complete!"
    echo -e "${GREEN}✅ All services are running!${NC}"
    echo -e "${GREEN}🎉 You can now upload datasets and see real-time insights!${NC}"
    echo ""
    echo -e "${YELLOW}To stop all services, run:${NC}"
    echo -e "${BLUE}docker-compose down${NC}"
    echo ""
    echo -e "${YELLOW}To view logs, run:${NC}"
    echo -e "${BLUE}docker-compose logs -f [service-name]${NC}"
}

# Handle script arguments
case "${1:-}" in
    "start")
        main
        ;;
    "stop")
        print_status "Stopping all services..."
        docker-compose down
        print_status "All services stopped ✅"
        ;;
    "restart")
        print_status "Restarting all services..."
        docker-compose down
        sleep 5
        main
        ;;
    "status")
        check_services
        ;;
    "logs")
        docker-compose logs -f "${2:-}"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs [service-name]}"
        echo ""
        echo "Commands:"
        echo "  start    - Start all Big Data services and Streamlit dashboard"
        echo "  stop     - Stop all services"
        echo "  restart  - Restart all services"
        echo "  status   - Check service status"
        echo "  logs     - View logs (optionally specify service name)"
        exit 1
        ;;
esac
