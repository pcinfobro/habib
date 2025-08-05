#!/usr/bin/env python3
"""
Production Big Data Pipeline for Pakistan's Largest Ecommerce Dataset
Processing 1M+ records with Spark cluster optimization - CONTAINER VERSION
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/bitnami/spark/big_data_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_spark_big_data():
    """Setup Spark for big data processing with optimized configuration"""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, sum, avg, count, countDistinct, desc
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
        
        # Create Spark session optimized for big data - local cluster mode
        spark = SparkSession.builder \
            .appName("Pakistan-Ecommerce-BigData-Pipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.default.parallelism", "100") \
            .getOrCreate()
            
        logger.info(f"✅ Spark session created for big data processing")
        logger.info(f"   Spark version: {spark.version}")
        logger.info(f"   Driver memory: 2GB")
        logger.info(f"   Executor memory: 4GB")
        logger.info(f"   Parallelism: 100")
        
        return spark
        
    except Exception as e:
        logger.error(f"❌ Failed to setup Spark: {e}")
        return None

def load_big_dataset(spark):
    """Load the Pakistan Largest Ecommerce Dataset (1M+ records)"""
    dataset_path = "/opt/bitnami/spark/dataset.csv"
    
    if not os.path.exists(dataset_path):
        logger.error(f"❌ Dataset not found: {dataset_path}")
        return None
        
    try:
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
        
        logger.info(f"📊 Loading Pakistan's Largest Ecommerce Dataset...")
        logger.info(f"   Path: {dataset_path}")
        
        # Load with optimized settings for big data (let Spark infer schema)
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .option("escape", '"') \
            .csv(dataset_path)
        
        # Cache for performance
        df.cache()
        
        record_count = df.count()
        logger.info(f"✅ Successfully loaded dataset with {record_count:,} records")
        logger.info(f"   Columns: {len(df.columns)}")
        logger.info(f"   Memory cached: Yes")
        
        # Show sample data and schema
        logger.info("📋 Dataset Schema:")
        df.printSchema()
        logger.info("📋 Sample Data:")
        df.show(5)
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to load dataset: {e}")
        return None

def process_big_data_analytics(spark, df):
    """Process comprehensive analytics on 1M+ records"""
    try:
        from pyspark.sql.functions import col, sum, avg, count, countDistinct, desc, when, isnan, isnull
        
        logger.info("🔄 Processing big data analytics...")
        
        # Show all columns first
        logger.info(f"Available columns: {df.columns}")
        
        # Clean and prepare data - be flexible with column names
        df_clean = df.filter(~col(df.columns[1]).isNull())  # Filter on second column (likely status)
        
        # Find numeric columns for analysis
        numeric_cols = []
        for col_name, col_type in df.dtypes:
            if col_type in ['double', 'float', 'int', 'bigint']:
                numeric_cols.append(col_name)
        
        logger.info(f"Numeric columns found: {numeric_cols}")
        
        # Comprehensive analytics
        analytics = {}
        
        # 1. Basic Statistics
        logger.info("   📈 Processing basic statistics...")
        total_records = df_clean.count()
        total_columns = len(df.columns)
        
        analytics["basic_stats"] = {
            "total_records": total_records,
            "total_columns": total_columns,
            "numeric_columns": len(numeric_cols)
        }
        
        # 2. Analyze first few columns for patterns
        logger.info("   📊 Processing column analysis...")
        column_analysis = {}
        
        for i, col_name in enumerate(df.columns[:10]):  # Analyze first 10 columns
            try:
                distinct_count = df_clean.select(col_name).distinct().count()
                null_count = df_clean.filter(col(col_name).isNull()).count()
                
                column_analysis[col_name] = {
                    "distinct_values": distinct_count,
                    "null_count": null_count,
                    "data_type": dict(df.dtypes)[col_name]
                }
            except Exception as e:
                logger.warning(f"   Warning: Could not analyze column {col_name}: {e}")
        
        analytics["column_analysis"] = column_analysis
        
        # 3. Numeric Analysis (if numeric columns exist)
        if numeric_cols:
            logger.info("   💰 Processing numeric analytics...")
            numeric_analysis = {}
            
            for col_name in numeric_cols[:5]:  # Analyze first 5 numeric columns
                try:
                    stats = df_clean.select(col_name).describe().collect()
                    numeric_analysis[col_name] = {
                        row["summary"]: float(row[col_name]) if row[col_name] else 0.0 
                        for row in stats
                    }
                except Exception as e:
                    logger.warning(f"   Warning: Could not analyze numeric column {col_name}: {e}")
            
            analytics["numeric_analysis"] = numeric_analysis
        
        # 4. Categorical Analysis
        logger.info("   🏷️ Processing categorical analytics...")
        categorical_cols = [col_name for col_name, col_type in df.dtypes if col_type == 'string'][:5]
        
        categorical_analysis = {}
        for col_name in categorical_cols:
            try:
                top_values = df_clean.groupBy(col_name) \
                    .count() \
                    .orderBy(desc("count")) \
                    .limit(10) \
                    .collect()
                
                categorical_analysis[col_name] = [
                    {"value": row[col_name], "count": row["count"]}
                    for row in top_values
                ]
            except Exception as e:
                logger.warning(f"   Warning: Could not analyze categorical column {col_name}: {e}")
        
        analytics["categorical_analysis"] = categorical_analysis
        
        # Add metadata
        analytics["metadata"] = {
            "total_records_processed": total_records,
            "processing_timestamp": datetime.now().isoformat(),
            "dataset": "Pakistan Largest Ecommerce Dataset",
            "data_size": f"{total_records:,} records"
        }
        
        logger.info(f"✅ Big data analytics completed!")
        logger.info(f"   Records processed: {total_records:,}")
        
        return analytics
        
    except Exception as e:
        logger.error(f"❌ Failed to process analytics: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def create_powerbi_datasets(analytics):
    """Create PowerBI-compatible datasets from big data analytics"""
    try:
        logger.info("📊 Creating PowerBI datasets...")
        
        # Ensure output directory exists
        os.makedirs("/opt/bitnami/spark/output", exist_ok=True)
        
        # 1. Basic Statistics Dataset
        basic_data = pd.DataFrame([analytics["basic_stats"]])
        basic_file = "/opt/bitnami/spark/output/big_data_basic_stats.csv"
        basic_data.to_csv(basic_file, index=False)
        logger.info(f"   ✅ Basic statistics: {basic_file}")
        
        # 2. Column Analysis Dataset
        if "column_analysis" in analytics:
            column_data = pd.DataFrame(analytics["column_analysis"]).T
            column_file = "/opt/bitnami/spark/output/big_data_column_analysis.csv"
            column_data.to_csv(column_file, index=True)
            logger.info(f"   ✅ Column analysis: {column_file}")
        
        # 3. Complete Analytics JSON for PowerBI
        analytics_file = "/opt/bitnami/spark/output/big_data_complete_analytics.json"
        with open(analytics_file, 'w') as f:
            json.dump(analytics, f, indent=2, default=str)
        logger.info(f"   ✅ Complete analytics: {analytics_file}")
        
        # 4. Executive Summary for PowerBI Dashboard
        exec_summary = {
            "total_records": analytics["basic_stats"]["total_records"],
            "total_columns": analytics["basic_stats"]["total_columns"],
            "processing_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dataset_size": f"{analytics['basic_stats']['total_records']:,} records"
        }
        
        exec_file = "/opt/bitnami/spark/output/big_data_executive_summary.json"
        with open(exec_file, 'w') as f:
            json.dump(exec_summary, f, indent=2)
        logger.info(f"   ✅ Executive summary: {exec_file}")
        
        logger.info("✅ All PowerBI datasets created successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create PowerBI datasets: {e}")
        return False

def main():
    """Main pipeline execution for big data processing"""
    logger.info("🚀 Starting Production Big Data Pipeline")
    logger.info("   Dataset: Pakistan Largest Ecommerce Dataset")
    logger.info("   Records: 1,048,587+")
    logger.info("   Processing: Spark cluster with optimized configuration")
    
    # Setup Spark for big data
    spark = setup_spark_big_data()
    if not spark:
        logger.error("❌ Failed to initialize Spark session")
        return False
    
    try:
        # Load big dataset
        df = load_big_dataset(spark)
        if df is None:
            logger.error("❌ Failed to load dataset")
            return False
        
        # Process analytics
        analytics = process_big_data_analytics(spark, df)
        if analytics is None:
            logger.error("❌ Failed to process analytics")
            return False
        
        # Create PowerBI datasets
        success = create_powerbi_datasets(analytics)
        if not success:
            logger.error("❌ Failed to create PowerBI datasets")
            return False
        
        # Final summary
        logger.info("🎉 BIG DATA PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info(f"   Records Analyzed: {analytics['metadata']['total_records_processed']:,}")
        
        return True
        
    finally:
        if spark:
            spark.stop()
            logger.info("✅ Spark session closed")

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ BIG DATA PIPELINE COMPLETED - Ready for PowerBI Integration!")
        exit(0)
    else:
        print("\n❌ BIG DATA PIPELINE FAILED")
        exit(1)
