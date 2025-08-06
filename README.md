# Enterprise Big Data Pipeline - Core Implementation

## 📋 Overview

This directory contains the core implementation of the Enterprise Big Data Pipeline. For complete documentation, setup instructions, and project overview, please refer to the main **[README.md](../README.md)** file in the root directory.

## 🚀 Quick Summary

This is a production-ready enterprise data pipeline with:
- **Data Ingestion**: Kafka streaming
- **Data Storage**: HDFS distributed storage
- **Data Processing**: Spark cluster processing
- **Data Export**: Automated PowerBI integration
- **Visualization**: PowerBI dashboards + Streamlit interface
- **Automation**: PowerBI auto-refresh and scheduling
- **Orchestration**: Airflow DAG workflow management

## Quick Start
```bash
# Start the complete pipeline
docker-compose up -d

# Check all services are running
docker-compose ps

# Access services:
# - Streamlit Dashboard: http://localhost:8501
# - PowerBI Service API: http://localhost:5000
# - Airflow UI: http://localhost:8080 (admin/admin)
# - Kafka UI: http://localhost:8081
# - HDFS UI: http://localhost:9870
# - Spark UI: http://localhost:8080
```

## Core Components

### 1. Docker Services (`docker-compose.yml`)
- **Kafka + Zookeeper**: Data streaming and messaging
- **HDFS (NameNode + DataNode)**: Distributed data storage
- **Spark (Master + Worker)**: Data processing cluster
- **PostgreSQL**: Metadata and pipeline state storage
- **Airflow**: Pipeline orchestration and scheduling
- **PowerBI Service**: Automated dashboard creation and refresh
- **Streamlit**: Interactive web dashboard

### 2. Pipeline Orchestration (`dags/enterprise_ecommerce_pipeline.py`)
Complete Airflow DAG implementing:
- Data ingestion from CSV to Kafka
- Streaming data processing with Spark
- HDFS storage management
- PowerBI dataset creation and refresh
- Data quality validation
- Error handling and notifications

### 3. PowerBI Automation (`powerbi_service.py`)
Enterprise PowerBI service with:
- Flask API for dashboard management
- Automated dataset registration
- Dashboard creation and refresh scheduling
- PostgreSQL integration for tracking
- MSAL authentication for PowerBI REST API

### 4. Data Visualization (`streamlit_dashboard.py`)
Interactive dashboard with:
- Real-time data visualization
- Pipeline status monitoring
- Data quality metrics
- Performance analytics

### 5. Pipeline Verification (`verify_pipeline.py`)
Comprehensive testing script:
- Service health checks
- Data flow validation
- End-to-end pipeline testing
- Performance monitoring

## File Structure
```
ecommerce-data-pipeline/
├── docker-compose.yml              # Main orchestration
├── powerbi_service.py              # PowerBI automation service  
├── streamlit_dashboard.py          # Interactive dashboard
├── verify_pipeline.py              # Pipeline testing
├── dags/
│   └── enterprise_ecommerce_pipeline.py  # Airflow DAG
├── config/
│   ├── pipeline_config.json        # Pipeline configuration
│   └── pipeline_config_secure.json # Secure configuration
├── data/
│   ├── input/                      # Source data files
│   ├── processed/                  # Processed data
│   ├── output/                     # Final outputs
│   └── logs/                       # Pipeline logs
├── spark-apps/                     # Spark applications
├── plugins/                        # Airflow plugins
└── logs/                          # System logs
```

## Deployment Notes
- All services are containerized and networked properly
- Data persistence configured for databases and storage
- Environment variables for secure configuration
- Health checks and restart policies enabled
- Scalable Spark cluster configuration

## Next Steps
1. Configure PowerBI authentication (MSAL credentials)
2. Upload source data to `data/input/`
3. Run `docker-compose up -d` to start all services
4. Access Airflow UI to trigger the pipeline DAG
5. Monitor progress through Streamlit dashboard
6. View generated PowerBI dashboards

This implementation provides a complete, production-ready data pipeline matching all enterprise requirements.
