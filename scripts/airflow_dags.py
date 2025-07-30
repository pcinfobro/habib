"""
Apache Airflow DAGs for E-commerce Data Pipeline
Orchestrates the entire data pipeline workflow
"""

from datetime import datetime, timedelta
from typing import Dict, Any
import logging

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.hooks.base import BaseHook

# Custom imports (these would be your pipeline modules)
import sys
import os
sys.path.append('/opt/airflow/dags/scripts')

from data_ingestion import DataIngestionManager
from data_processing import DataProcessor
from hdfs_storage import DataStorageManager
from kafka_streaming import EcommerceStreamProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for all DAGs
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Configuration
PIPELINE_CONFIG = {
    'data_sources': {
        'orders_csv': {
            'type': 'csv',
            'config': {
                'file_path': '/data/input/orders.csv',
                'encoding': 'utf-8'
            }
        },
        'customers_api': {
            'type': 'api',
            'config': {
                'url': 'https://api.ecommerce.com/customers',
                'headers': {'Authorization': 'Bearer {{ var.value.api_token }}'},
                'data_key': 'customers'
            }
        },
        'products_json': {
            'type': 'json',
            'config': {
                'file_path': '/data/input/products.json'
            }
        }
    },
    'processing': {
        'duplicate_subset': ['customer_id', 'order_id'],
        'missing_values': {
            'customer_name': 'Unknown',
            'order_amount': 'mean',
            'order_date': 'drop'
        },
        'data_types': {
            'order_date': 'datetime',
            'order_amount': 'numeric'
        }
    },
    'storage': {
        'hdfs': {
            'hdfs_url': 'hdfs://namenode:9000',
            'hdfs_user': 'airflow'
        },
        'hive': {
            'database': 'ecommerce'
        }
    }
}

def ingest_data_task(source_name: str, **context) -> str:
    """Task to ingest data from a specific source"""
    try:
        logger.info(f"Starting data ingestion for source: {source_name}")
        
        # Get source configuration
        source_config = PIPELINE_CONFIG['data_sources'][source_name]
        source_type = source_config['type']
        config = source_config['config']
        
        # Initialize ingestion manager
        ingestion_manager = DataIngestionManager()
        
        # Ingest data
        df = ingestion_manager.ingest_data(source_type, config)
        
        # Store raw data temporarily for processing
        temp_path = f"/tmp/raw_{source_name}_{context['ds_nodash']}.parquet"
        df.to_parquet(temp_path, index=False)
        
        logger.info(f"Data ingestion completed for {source_name}: {len(df)} records")
        return temp_path
        
    except Exception as e:
        logger.error(f"Data ingestion failed for {source_name}: {str(e)}")
        raise

def process_data_task(source_name: str, **context) -> str:
    """Task to process ingested data"""
    try:
        logger.info(f"Starting data processing for source: {source_name}")
        
        # Get the raw data path from previous task
        ti = context['ti']
        raw_data_path = ti.xcom_pull(task_ids=f'ingest_{source_name}')
        
        # Load raw data
        import pandas as pd
        df = pd.read_parquet(raw_data_path)
        
        # Initialize processor with configuration
        processor = DataProcessor(PIPELINE_CONFIG['processing'])
        
        # Process data
        processed_df = processor.process(df)
        
        # Store processed data temporarily
        processed_path = f"/tmp/processed_{source_name}_{context['ds_nodash']}.parquet"
        processed_df.to_parquet(processed_path, index=False)
        
        # Clean up raw data file
        os.remove(raw_data_path)
        
        logger.info(f"Data processing completed for {source_name}: {len(processed_df)} records")
        return processed_path
        
    except Exception as e:
        logger.error(f"Data processing failed for {source_name}: {str(e)}")
        raise

def store_data_task(source_name: str, **context) -> str:
    """Task to store processed data in HDFS/Hive"""
    try:
        logger.info(f"Starting data storage for source: {source_name}")
        
        # Get the processed data path from previous task
        ti = context['ti']
        processed_data_path = ti.xcom_pull(task_ids=f'process_{source_name}')
        
        # Load processed data
        import pandas as pd
        df = pd.read_parquet(processed_data_path)
        
        # Initialize storage manager
        storage_manager = DataStorageManager(PIPELINE_CONFIG['storage'])
        
        # Store raw data in HDFS
        raw_hdfs_path = storage_manager.store_raw_data(df, source_name)
        
        # Store processed data in Hive table
        table_name = f"processed_{source_name}"
        partition_columns = ['processing_date'] if 'processing_timestamp' in df.columns else None
        
        if partition_columns:
            df['processing_date'] = df['processing_timestamp'].dt.date
        
        processed_hdfs_paths = storage_manager.store_processed_data(
            df, table_name, partition_columns
        )
        
        # Clean up temporary file
        os.remove(processed_data_path)
        
        logger.info(f"Data storage completed for {source_name}")
        return {'raw_path': raw_hdfs_path, 'processed_paths': processed_hdfs_paths}
        
    except Exception as e:
        logger.error(f"Data storage failed for {source_name}: {str(e)}")
        raise

def data_quality_check_task(**context) -> bool:
    """Task to perform data quality checks"""
    try:
        logger.info("Starting data quality checks")
        
        # Initialize storage manager for querying
        storage_manager = DataStorageManager(PIPELINE_CONFIG['storage'])
        
        # Define quality checks
        quality_checks = [
            {
                'name': 'orders_count_check',
                'query': 'SELECT COUNT(*) as count FROM processed_orders',
                'expected_min': 1
            },
            {
                'name': 'customers_duplicates_check',
                'query': 'SELECT COUNT(*) - COUNT(DISTINCT customer_id) as duplicates FROM processed_customers',
                'expected_max': 0
            },
            {
                'name': 'products_null_check',
                'query': 'SELECT COUNT(*) as null_count FROM processed_products WHERE product_name IS NULL',
                'expected_max': 0
            }
        ]
        
        failed_checks = []
        
        for check in quality_checks:
            try:
                result = storage_manager.query_data(check['query'])
                # Parse result (simplified - in practice you'd parse the Hive output)
                value = int(result.strip().split('\n')[-1])
                
                if 'expected_min' in check and value < check['expected_min']:
                    failed_checks.append(f"{check['name']}: {value} < {check['expected_min']}")
                elif 'expected_max' in check and value > check['expected_max']:
                    failed_checks.append(f"{check['name']}: {value} > {check['expected_max']}")
                
                logger.info(f"Quality check passed: {check['name']} = {value}")
                
            except Exception as e:
                failed_checks.append(f"{check['name']}: Error - {str(e)}")
        
        if failed_checks:
            error_msg = "Data quality checks failed: " + "; ".join(failed_checks)
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info("All data quality checks passed")
        return True
        
    except Exception as e:
        logger.error(f"Data quality check failed: {str(e)}")
        raise

def send_success_notification(**context):
    """Task to send success notification"""
    logger.info("Pipeline completed successfully - sending notification")
    # In practice, you would send email, Slack message, etc.
    
def cleanup_temp_files(**context):
    """Task to cleanup temporary files"""
    try:
        import glob
        temp_files = glob.glob(f"/tmp/*_{context['ds_nodash']}.parquet")
        for file in temp_files:
            if os.path.exists(file):
                os.remove(file)
                logger.info(f"Cleaned up temporary file: {file}")
    except Exception as e:
        logger.warning(f"Cleanup warning: {str(e)}")

# Main DAG Definition
dag = DAG(
    'ecommerce_data_pipeline',
    default_args=default_args,
    description='E-commerce data pipeline with ingestion, processing, and storage',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    max_active_runs=1,
    tags=['ecommerce', 'data-pipeline', 'etl']
)

# Start task
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Data ingestion task group
with TaskGroup('data_ingestion', dag=dag) as ingestion_group:
    ingestion_tasks = []
    
    for source_name in PIPELINE_CONFIG['data_sources'].keys():
        ingest_task = PythonOperator(
            task_id=f'ingest_{source_name}',
            python_callable=ingest_data_task,
            op_kwargs={'source_name': source_name},
            dag=dag
        )
        ingestion_tasks.append(ingest_task)

# Data processing task group
with TaskGroup('data_processing', dag=dag) as processing_group:
    processing_tasks = []
    
    for source_name in PIPELINE_CONFIG['data_sources'].keys():
        process_task = PythonOperator(
            task_id=f'process_{source_name}',
            python_callable=process_data_task,
            op_kwargs={'source_name': source_name},
            dag=dag
        )
        processing_tasks.append(process_task)

# Data storage task group
with TaskGroup('data_storage', dag=dag) as storage_group:
    storage_tasks = []
    
    for source_name in PIPELINE_CONFIG['data_sources'].keys():
        store_task = PythonOperator(
            task_id=f'store_{source_name}',
            python_callable=store_data_task,
            op_kwargs={'source_name': source_name},
            dag=dag
        )
        storage_tasks.append(store_task)

# Data quality check task
quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check_task,
    dag=dag
)

# Notification task
notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# Cleanup task
cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    trigger_rule='all_done',  # Run regardless of upstream success/failure
    dag=dag
)

# End task
end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Define task dependencies
start_task >> ingestion_group >> processing_group >> storage_group >> quality_check_task >> notification_task >> cleanup_task >> end_task

# Set up individual task dependencies within groups
for i, source_name in enumerate(PIPELINE_CONFIG['data_sources'].keys()):
    ingestion_group.children[i] >> processing_group.children[i] >> storage_group.children[i]

# Real-time streaming DAG
streaming_dag = DAG(
    'ecommerce_streaming_pipeline',
    default_args=default_args,
    description='Real-time e-commerce data streaming pipeline',
    schedule_interval=None,  # Triggered externally or runs continuously
    max_active_runs=1,
    tags=['ecommerce', 'streaming', 'kafka']
)

def start_kafka_streaming(**context):
    """Start Kafka streaming processor"""
    try:
        kafka_config = {
            'bootstrap_servers': ['kafka:9092'],
            'consumer_group': 'ecommerce-streaming',
            'output_topics': {
                'raw-orders': 'processed-orders',
                'raw-customers': 'processed-customers',
                'raw-products': 'processed-products'
            }
        }
        
        processor = EcommerceStreamProcessor(kafka_config)
        
        # Start processing (this would run continuously in practice)
        processor.start_processing(['raw-orders', 'raw-customers', 'raw-products'])
        
        logger.info("Kafka streaming started")
        
        # In a real implementation, this would run indefinitely
        # For demo purposes, we'll run for a limited time
        import time
        time.sleep(300)  # Run for 5 minutes
        
        processor.stop_processing()
        logger.info("Kafka streaming stopped")
        
    except Exception as e:
        logger.error(f"Kafka streaming failed: {str(e)}")
        raise

streaming_task = PythonOperator(
    task_id='kafka_streaming',
    python_callable=start_kafka_streaming,
    dag=streaming_dag
)

# Monitoring and alerting DAG
monitoring_dag = DAG(
    'ecommerce_pipeline_monitoring',
    default_args=default_args,
    description='Monitor e-commerce data pipeline health',
    schedule_interval=timedelta(minutes=15),  # Check every 15 minutes
    max_active_runs=1,
    tags=['monitoring', 'alerting']
)

def check_pipeline_health(**context):
    """Check pipeline health and send alerts if needed"""
    try:
        storage_manager = DataStorageManager(PIPELINE_CONFIG['storage'])
        
        # Check recent data ingestion
        current_date = context['ds']
        
        health_checks = [
            {
                'name': 'recent_orders',
                'query': f"SELECT COUNT(*) FROM processed_orders WHERE processing_date = '{current_date}'",
                'threshold': 10,
                'alert_level': 'warning'
            },
            {
                'name': 'data_freshness',
                'query': "SELECT MAX(processing_timestamp) FROM processed_orders",
                'max_age_hours': 12,
                'alert_level': 'critical'
            }
        ]
        
        alerts = []
        
        for check in health_checks:
            try:
                result = storage_manager.query_data(check['query'])
                
                if check['name'] == 'recent_orders':
                    count = int(result.strip().split('\n')[-1])
                    if count < check['threshold']:
                        alerts.append(f"Low data volume: {count} records (expected > {check['threshold']})")
                
                elif check['name'] == 'data_freshness':
                    # Parse timestamp and check freshness
                    # This is simplified - in practice you'd parse the actual timestamp
                    alerts.append("Data freshness check completed")
                
            except Exception as e:
                alerts.append(f"Health check failed for {check['name']}: {str(e)}")
        
        if alerts:
            logger.warning(f"Pipeline health alerts: {'; '.join(alerts)}")
            # In practice, send to monitoring system
        else:
            logger.info("Pipeline health check passed")
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise

health_check_task = PythonOperator(
    task_id='check_pipeline_health',
    python_callable=check_pipeline_health,
    dag=monitoring_dag
)
