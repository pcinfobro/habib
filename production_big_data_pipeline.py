#!/usr/bin/env python3
"""
Production Big Data Pipeline for Pakistan's Largest Ecommerce Dataset
Processing 1M+ records with Spark cluster optimization
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/big_data_pipeline.log'),
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
        
        # Create Spark session optimized for big data - connect to Docker cluster
        spark = SparkSession.builder \
            .appName("Pakistan-Ecommerce-BigData-Pipeline") \
            .master("spark://localhost:7077") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.default.parallelism", "100") \
            .getOrCreate()
            
        logger.info(f"✅ Spark session created for big data processing")
        logger.info(f"   Spark version: {spark.version}")
        logger.info(f"   Driver memory: 2GB")
        logger.info(f"   Executor memory: 2GB")
        logger.info(f"   Parallelism: 100")
        
        return spark
        
    except Exception as e:
        logger.error(f"❌ Failed to setup Spark: {e}")
        return None

def load_big_dataset(spark):
    """Load the Pakistan Largest Ecommerce Dataset (1M+ records)"""
    dataset_path = "data/input/Pakistan Largest Ecommerce Dataset.csv"
    
    if not os.path.exists(dataset_path):
        logger.error(f"❌ Dataset not found: {dataset_path}")
        return None
        
    try:
        logger.info(f"📊 Loading Pakistan's Largest Ecommerce Dataset...")
        logger.info(f"   Path: {dataset_path}")
        
        # Define schema for better performance
        schema = StructType([
            StructField("item_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("qty_ordered", IntegerType(), True),
            StructField("grand_total", DoubleType(), True),
            StructField("increment_id", StringType(), True),
            StructField("category_name_1", StringType(), True),
            StructField("sales_commission_code", StringType(), True),
            StructField("discount_amount", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("working_date", StringType(), True),
            StructField("bi_status", StringType(), True),
            StructField("mv", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("customer_since", StringType(), True),
            StructField("m_y", StringType(), True),
            StructField("fy", StringType(), True),
            StructField("customer_id", StringType(), True)
        ])
        
        # Load with optimized settings for big data
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("multiline", "true") \
            .option("escape", '"') \
            .schema(schema) \
            .csv(dataset_path)
        
        # Cache for performance
        df.cache()
        
        record_count = df.count()
        logger.info(f"✅ Successfully loaded dataset with {record_count:,} records")
        logger.info(f"   Columns: {len(df.columns)}")
        logger.info(f"   Memory cached: Yes")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to load dataset: {e}")
        return None

def process_big_data_analytics(spark, df):
    """Process comprehensive analytics on 1M+ records"""
    try:
        from pyspark.sql.functions import col, sum, avg, count, countDistinct, desc
        
        logger.info("🔄 Processing big data analytics...")
        
        # Clean and prepare data
        df_clean = df.filter(col("status").isNotNull()) \
                    .filter(col("price") > 0) \
                    .filter(col("grand_total") > 0)
        
        # Comprehensive analytics
        analytics = {}
        
        # 1. Revenue Analytics
        logger.info("   📈 Processing revenue analytics...")
        revenue_stats = df_clean.agg(
            sum("grand_total").alias("total_revenue"),
            avg("grand_total").alias("avg_order_value"),
            count("*").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers")
        ).collect()[0]
        
        analytics["revenue"] = {
            "total_revenue": float(revenue_stats["total_revenue"] or 0),
            "avg_order_value": float(revenue_stats["avg_order_value"] or 0),
            "total_orders": int(revenue_stats["total_orders"] or 0),
            "unique_customers": int(revenue_stats["unique_customers"] or 0)
        }
        
        # 2. Category Performance
        logger.info("   🏪 Processing category analytics...")
        category_stats = df_clean.groupBy("category_name_1") \
            .agg(
                sum("grand_total").alias("category_revenue"),
                count("*").alias("category_orders"),
                avg("grand_total").alias("category_avg_order")
            ) \
            .orderBy(desc("category_revenue")) \
            .limit(10) \
            .collect()
        
        analytics["top_categories"] = [
            {
                "category": row["category_name_1"],
                "revenue": float(row["category_revenue"] or 0),
                "orders": int(row["category_orders"] or 0),
                "avg_order": float(row["category_avg_order"] or 0)
            } for row in category_stats
        ]
        
        # 3. Monthly Trends
        logger.info("   📅 Processing temporal analytics...")
        monthly_stats = df_clean.groupBy("year", "month") \
            .agg(
                sum("grand_total").alias("monthly_revenue"),
                count("*").alias("monthly_orders")
            ) \
            .orderBy("year", "month") \
            .collect()
        
        analytics["monthly_trends"] = [
            {
                "year": int(row["year"] or 0),
                "month": int(row["month"] or 0),
                "revenue": float(row["monthly_revenue"] or 0),
                "orders": int(row["monthly_orders"] or 0)
            } for row in monthly_stats
        ]
        
        # 4. Payment Method Analysis
        logger.info("   💳 Processing payment analytics...")
        payment_stats = df_clean.groupBy("payment_method") \
            .agg(
                sum("grand_total").alias("payment_revenue"),
                count("*").alias("payment_orders")
            ) \
            .orderBy(desc("payment_revenue")) \
            .collect()
        
        analytics["payment_methods"] = [
            {
                "method": row["payment_method"],
                "revenue": float(row["payment_revenue"] or 0),
                "orders": int(row["payment_orders"] or 0)
            } for row in payment_stats
        ]
        
        # 5. Status Distribution
        logger.info("   📊 Processing order status analytics...")
        status_stats = df_clean.groupBy("status") \
            .agg(
                count("*").alias("status_count"),
                sum("grand_total").alias("status_revenue")
            ) \
            .collect()
        
        analytics["order_status"] = [
            {
                "status": row["status"],
                "count": int(row["status_count"] or 0),
                "revenue": float(row["status_revenue"] or 0)
            } for row in status_stats
        ]
        
        # Add metadata
        analytics["metadata"] = {
            "total_records_processed": df_clean.count(),
            "processing_timestamp": datetime.now().isoformat(),
            "dataset": "Pakistan Largest Ecommerce Dataset",
            "data_size": "1M+ records"
        }
        
        logger.info(f"✅ Big data analytics completed!")
        logger.info(f"   Records processed: {analytics['metadata']['total_records_processed']:,}")
        logger.info(f"   Total revenue: ${analytics['revenue']['total_revenue']:,.2f}")
        logger.info(f"   Unique customers: {analytics['revenue']['unique_customers']:,}")
        
        return analytics
        
    except Exception as e:
        logger.error(f"❌ Failed to process analytics: {e}")
        return None

def create_powerbi_datasets(analytics):
    """Create PowerBI-compatible datasets from big data analytics"""
    try:
        logger.info("📊 Creating PowerBI datasets...")
        
        # Ensure output directory exists
        os.makedirs("data/output", exist_ok=True)
        
        # 1. Revenue Summary Dataset
        revenue_data = pd.DataFrame([analytics["revenue"]])
        revenue_file = "data/output/big_data_revenue_summary.csv"
        revenue_data.to_csv(revenue_file, index=False)
        logger.info(f"   ✅ Revenue summary: {revenue_file}")
        
        # 2. Category Performance Dataset
        category_data = pd.DataFrame(analytics["top_categories"])
        category_file = "data/output/big_data_category_performance.csv"
        category_data.to_csv(category_file, index=False)
        logger.info(f"   ✅ Category performance: {category_file}")
        
        # 3. Monthly Trends Dataset
        trends_data = pd.DataFrame(analytics["monthly_trends"])
        trends_file = "data/output/big_data_monthly_trends.csv"
        trends_data.to_csv(trends_file, index=False)
        logger.info(f"   ✅ Monthly trends: {trends_file}")
        
        # 4. Payment Methods Dataset
        payment_data = pd.DataFrame(analytics["payment_methods"])
        payment_file = "data/output/big_data_payment_analysis.csv"
        payment_data.to_csv(payment_file, index=False)
        logger.info(f"   ✅ Payment analysis: {payment_file}")
        
        # 5. Complete Analytics JSON for PowerBI
        analytics_file = "data/output/big_data_complete_analytics.json"
        with open(analytics_file, 'w') as f:
            json.dump(analytics, f, indent=2, default=str)
        logger.info(f"   ✅ Complete analytics: {analytics_file}")
        
        # 6. Executive Summary for PowerBI Dashboard
        exec_summary = {
            "total_revenue": analytics["revenue"]["total_revenue"],
            "total_orders": analytics["revenue"]["total_orders"],
            "unique_customers": analytics["revenue"]["unique_customers"],
            "avg_order_value": analytics["revenue"]["avg_order_value"],
            "top_category": analytics["top_categories"][0]["category"] if analytics["top_categories"] else "N/A",
            "top_category_revenue": analytics["top_categories"][0]["revenue"] if analytics["top_categories"] else 0,
            "processing_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "dataset_size": "1,048,587+ records"
        }
        
        exec_file = "data/output/big_data_executive_summary.json"
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
        logger.info(f"   Total Revenue Processed: ${analytics['revenue']['total_revenue']:,.2f}")
        logger.info(f"   Records Analyzed: {analytics['metadata']['total_records_processed']:,}")
        logger.info(f"   Categories Analyzed: {len(analytics['top_categories'])}")
        logger.info(f"   Payment Methods: {len(analytics['payment_methods'])}")
        logger.info(f"   Time Periods: {len(analytics['monthly_trends'])}")
        
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
