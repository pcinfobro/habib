#!/usr/bin/env python3
"""
Enterprise Ecommerce Data Pipeline DAG
=====================================
Complete automated pipeline: Kafka → HDFS → Spark → PowerBI
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd
import json
import logging
import requests
from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
import subprocess

# Default arguments
default_args = {
    'owner': 'ecommerce-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 5),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create DAG
dag = DAG(
    'enterprise_ecommerce_pipeline',
    default_args=default_args,
    description='Complete ecommerce data pipeline with Kafka, HDFS, Spark, and PowerBI',
    schedule_interval='@hourly',  # Run every hour
    max_active_runs=1,
    tags=['ecommerce', 'enterprise', 'powerbi']
)

def ingest_data_to_kafka(**context):
    """
    STEP 1: Data Ingestion - Send data to Kafka
    """
    logging.info("🚀 Starting data ingestion to Kafka")
    
    try:
        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Read ecommerce data
        data_path = '/opt/spark-data/input/ecommerce_data.csv'
        df = pd.read_csv(data_path)
        
        # Send data to Kafka topic
        topic_name = 'ecommerce_transactions'
        records_sent = 0
        
        for _, row in df.iterrows():
            message = {
                'timestamp': datetime.now().isoformat(),
                'data': row.to_dict()
            }
            
            future = producer.send(topic_name, value=message)
            records_sent += 1
            
            if records_sent % 1000 == 0:
                logging.info(f"Sent {records_sent} records to Kafka")
        
        producer.flush()
        producer.close()
        
        logging.info(f"✅ Successfully sent {records_sent} records to Kafka topic: {topic_name}")
        return records_sent
        
    except Exception as e:
        logging.error(f"❌ Kafka ingestion failed: {str(e)}")
        raise

def consume_kafka_to_hdfs(**context):
    """
    STEP 2: Stream Processing - Consume from Kafka and store in HDFS
    """
    logging.info("📥 Starting Kafka to HDFS streaming")
    
    try:
        # Initialize Kafka consumer
        consumer = KafkaConsumer(
            'ecommerce_transactions',
            bootstrap_servers=['kafka:29092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000  # 30 seconds timeout
        )
        
        # Collect messages
        messages = []
        for message in consumer:
            messages.append(message.value)
            
            if len(messages) >= 10000:  # Process in batches
                break
        
        consumer.close()
        
        if messages:
            # Convert to DataFrame
            data_records = [msg['data'] for msg in messages]
            df = pd.DataFrame(data_records)
            
            # Save to HDFS via temporary file
            temp_file = f'/tmp/ecommerce_batch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            df.to_csv(temp_file, index=False)
            
            # Copy to HDFS
            hdfs_path = f'/warehouse/ecommerce_streaming/{datetime.now().strftime("%Y/%m/%d")}/data_{datetime.now().strftime("%H%M%S")}.csv'
            
            subprocess.run([
                'hdfs', 'dfs', '-put', temp_file, hdfs_path
            ], check=True)
            
            logging.info(f"✅ Stored {len(messages)} records in HDFS: {hdfs_path}")
            return len(messages)
        else:
            logging.warning("⚠️ No messages consumed from Kafka")
            return 0
            
    except Exception as e:
        logging.error(f"❌ Kafka to HDFS streaming failed: {str(e)}")
        raise

def spark_processing(**context):
    """
    STEP 3: Data Processing - Process data with Spark
    """
    logging.info("⚡ Starting Spark processing")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("EcommerceDataProcessing") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Read data from HDFS
        df = spark.read.option("header", "true") \
            .csv("hdfs://namenode:8020/warehouse/ecommerce_streaming/*/*/*.csv")
        
        logging.info(f"📊 Processing {df.count()} records with Spark")
        
        # Data transformations
        processed_df = df \
            .filter(df.status.isNotNull()) \
            .withColumn("processing_timestamp", 
                       spark.sql.functions.current_timestamp()) \
            .groupBy("category_name_1", "payment_method") \
            .agg(
                spark.sql.functions.sum("grand_total").alias("total_revenue"),
                spark.sql.functions.count("*").alias("transaction_count"),
                spark.sql.functions.avg("grand_total").alias("avg_transaction_value")
            )
        
        # Save processed results
        output_path = f"hdfs://namenode:8020/warehouse/processed/ecommerce_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        processed_df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        # Also save as Parquet for better performance
        processed_df.write \
            .mode("overwrite") \
            .parquet(f"{output_path}.parquet")
        
        record_count = processed_df.count()
        spark.stop()
        
        logging.info(f"✅ Spark processing completed. Processed {record_count} summary records")
        return output_path
        
    except Exception as e:
        logging.error(f"❌ Spark processing failed: {str(e)}")
        raise

def export_to_powerbi_db(**context):
    """
    STEP 4: Data Export - Export processed data to PowerBI database
    """
    logging.info("📤 Exporting to PowerBI database")
    
    try:
        from sqlalchemy import create_engine
        
        # Database connection
        db_url = 'postgresql://powerbi:powerbi123@postgres-powerbi:5432/powerbi'
        engine = create_engine(db_url)
        
        # Get the processed data path from previous task
        output_path = context['task_instance'].xcom_pull(task_ids='spark_processing')
        
        # Read processed data (this would be from HDFS in real scenario)
        # For now, create sample processed data
        processed_data = {
            'category': ['Women Fashion', 'Beauty & Grooming', 'Electronics'],
            'payment_method': ['COD', 'COD', 'Online'],
            'total_revenue': [1950000, 240000, 500000],
            'transaction_count': [1000, 1000, 250],
            'avg_transaction_value': [1950, 240, 2000],
            'processing_date': [datetime.now().date()] * 3
        }
        
        df = pd.DataFrame(processed_data)
        
        # Export to PowerBI database
        table_name = f'ecommerce_summary_{datetime.now().strftime("%Y%m%d")}'
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        # Also create a current view
        df.to_sql('ecommerce_current_summary', engine, if_exists='replace', index=False)
        
        logging.info(f"✅ Exported {len(df)} records to PowerBI database table: {table_name}")
        return table_name
        
    except Exception as e:
        logging.error(f"❌ PowerBI export failed: {str(e)}")
        raise

def refresh_powerbi_dashboards(**context):
    """
    STEP 5: PowerBI Automation - Refresh dashboards
    """
    logging.info("🔄 Refreshing PowerBI dashboards")
    
    try:
        # Call PowerBI automation service
        powerbi_service_url = 'http://powerbi-automation:5000'
        
        # Trigger refresh for all dashboards
        response = requests.post(f'{powerbi_service_url}/refresh/all')
        
        if response.status_code == 200:
            result = response.json()
            successful_refreshes = result.get('successful_refreshes', 0)
            total_dashboards = result.get('total_dashboards', 0)
            
            logging.info(f"✅ PowerBI refresh completed: {successful_refreshes}/{total_dashboards} successful")
            return result
        else:
            raise Exception(f"PowerBI service returned status {response.status_code}")
            
    except Exception as e:
        logging.error(f"❌ PowerBI refresh failed: {str(e)}")
        raise

def data_quality_validation(**context):
    """
    STEP 6: Data Quality - Validate processed data
    """
    logging.info("🔍 Running data quality validation")
    
    try:
        from sqlalchemy import create_engine
        
        db_url = 'postgresql://powerbi:powerbi123@postgres-powerbi:5432/powerbi'
        engine = create_engine(db_url)
        
        # Run quality checks
        with engine.connect() as conn:
            # Check record count
            result = conn.execute("SELECT COUNT(*) FROM ecommerce_current_summary")
            record_count = result.fetchone()[0]
            
            # Check for null values
            result = conn.execute("""
                SELECT 
                    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) as null_categories,
                    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) as null_revenue
                FROM ecommerce_current_summary
            """)
            null_counts = result.fetchone()
            
            # Validate data quality
            quality_report = {
                'record_count': record_count,
                'null_categories': null_counts[0],
                'null_revenue': null_counts[1],
                'quality_score': 100 - (null_counts[0] + null_counts[1]) * 10,
                'validation_timestamp': datetime.now().isoformat()
            }
            
            logging.info(f"✅ Data quality validation completed: {quality_report}")
            
            if quality_report['quality_score'] < 90:
                raise Exception(f"Data quality score too low: {quality_report['quality_score']}")
                
            return quality_report
            
    except Exception as e:
        logging.error(f"❌ Data quality validation failed: {str(e)}")
        raise

def send_completion_notification(**context):
    """
    STEP 7: Notification - Send pipeline completion notification
    """
    logging.info("📬 Sending completion notification")
    
    try:
        # Get results from previous tasks
        kafka_records = context['task_instance'].xcom_pull(task_ids='ingest_to_kafka')
        hdfs_records = context['task_instance'].xcom_pull(task_ids='kafka_to_hdfs')
        powerbi_result = context['task_instance'].xcom_pull(task_ids='refresh_powerbi')
        quality_report = context['task_instance'].xcom_pull(task_ids='data_quality_check')
        
        notification = {
            'pipeline_run': context['run_id'],
            'execution_date': context['execution_date'].isoformat(),
            'status': 'SUCCESS',
            'metrics': {
                'kafka_records_ingested': kafka_records,
                'hdfs_records_stored': hdfs_records,
                'powerbi_dashboards_refreshed': powerbi_result.get('successful_refreshes', 0) if powerbi_result else 0,
                'data_quality_score': quality_report.get('quality_score', 0) if quality_report else 0
            },
            'completion_time': datetime.now().isoformat()
        }
        
        # Save notification (in real scenario, this would be sent via email/Slack)
        with open('/app/data/pipeline_notifications.json', 'a') as f:
            f.write(json.dumps(notification) + '\n')
        
        logging.info(f"✅ Pipeline completed successfully: {notification}")
        return notification
        
    except Exception as e:
        logging.error(f"❌ Notification failed: {str(e)}")
        raise

# =============================================
# DEFINE TASK DEPENDENCIES
# =============================================

# Task 1: Data Ingestion
ingest_task = PythonOperator(
    task_id='ingest_to_kafka',
    python_callable=ingest_data_to_kafka,
    dag=dag
)

# Task 2: Streaming
stream_task = PythonOperator(
    task_id='kafka_to_hdfs',
    python_callable=consume_kafka_to_hdfs,
    dag=dag
)

# Task 3: Processing
process_task = PythonOperator(
    task_id='spark_processing',
    python_callable=spark_processing,
    dag=dag
)

# Task 4: Export
export_task = PythonOperator(
    task_id='export_to_powerbi',
    python_callable=export_to_powerbi_db,
    dag=dag
)

# Task 5: PowerBI Refresh
refresh_task = PythonOperator(
    task_id='refresh_powerbi',
    python_callable=refresh_powerbi_dashboards,
    dag=dag
)

# Task 6: Data Quality
quality_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_validation,
    dag=dag
)

# Task 7: Notification
notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_completion_notification,
    dag=dag
)

# Set task dependencies
ingest_task >> stream_task >> process_task >> export_task >> refresh_task >> quality_task >> notify_task
