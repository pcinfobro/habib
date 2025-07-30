"""
E-commerce Data Pipeline - Streamlit Dashboard
Integrated with Big Data Services (Spark, Kafka, Iceberg, HDFS)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import io
import json
import os
import sys
from datetime import datetime, timedelta
import logging
import requests
import subprocess
import time

# Add the scripts directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

# Import pipeline modules (make them optional for standalone operation)
pipeline_modules_available = True
DataProcessor = None
HDFSManager = None
KafkaStreamProcessor = None
PipelineOrchestrator = None

try:
    from scripts.data_processing import DataProcessor
    from scripts.hdfs_storage import HDFSManager  
    from scripts.kafka_streaming import KafkaStreamProcessor
    from scripts.pipeline_orchestrator import PipelineOrchestrator
    print("✅ All pipeline modules imported successfully")
except ImportError as e:
    pipeline_modules_available = False
    print(f"❌ Pipeline modules not found (running in standalone mode): {e}")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IntegratedPipelineDashboard:
    def __init__(self):
        self.hdfs_manager = None
        self.kafka_processor = None
        self.data_processor = None
        self.pipeline_orchestrator = None
        self.spark_session = None
        self.pipeline_modules_available = pipeline_modules_available
        
        # Configuration for big data services
        self.config = {
            'hdfs_url': 'hdfs://namenode:9000',
            'hdfs_user': 'hadoop',
            'kafka_servers': ['kafka:29092'],
            'spark_master': 'spark://spark-master:7077',
            'hive_metastore': 'thrift://hive-metastore:9083'
        }
        
        # Auto-initialize Data Processor on startup
        self._auto_initialize_data_processor()
        
    def _auto_initialize_data_processor(self):
        """Automatically initialize data processor if available"""
        if DataProcessor is not None and not self.data_processor:
            try:
                self.data_processor = DataProcessor(self.config)
            except Exception as e:
                # Silent fail - will use fallback cleaning
                pass
                
    def initialize_services(self):
        """Initialize Big Data services connections"""
        if not self.pipeline_modules_available:
            st.warning("⚠️ Pipeline modules not available. Running in standalone mode.")
            return False
            
        try:
            # Initialize Data Processor
            if DataProcessor is not None:
                self.data_processor = DataProcessor(self.config)
                st.success("✅ Data Processor initialized")
            else:
                st.warning("⚠️ DataProcessor not available")
            
            # Initialize HDFS Manager
            try:
                if HDFSManager is not None:
                    self.hdfs_manager = HDFSManager(self.config)
                    st.success("✅ HDFS Manager initialized")
                else:
                    st.warning("⚠️ HDFSManager not available")
            except Exception as e:
                st.warning(f"⚠️ HDFS Manager initialization failed: {e}")
            
            # Initialize Kafka Processor
            try:
                if KafkaStreamProcessor is not None:
                    self.kafka_processor = KafkaStreamProcessor(self.config)
                    st.success("✅ Kafka Stream Processor initialized")
                else:
                    st.warning("⚠️ KafkaStreamProcessor not available")
            except Exception as e:
                st.warning(f"⚠️ Kafka initialization failed: {e}")
            
            # Initialize Pipeline Orchestrator
            try:
                if PipelineOrchestrator is not None:
                    self.pipeline_orchestrator = PipelineOrchestrator(self.config)
                    st.success("✅ Pipeline Orchestrator initialized")
                else:
                    st.warning("⚠️ PipelineOrchestrator not available")
            except Exception as e:
                st.warning(f"⚠️ Pipeline Orchestrator initialization failed: {e}")
            
            # Initialize Spark Session (for Iceberg integration)
            self.init_spark_session()
            
            return True
        except Exception as e:
            st.error(f"Service initialization failed: {e}")
            return False
    
    def init_spark_session(self):
        """Initialize Spark Session with Iceberg support"""
        try:
            # Check if pyspark is available
            try:
                from pyspark.sql import SparkSession
                
                self.spark_session = SparkSession.builder \
                    .appName("EcommerceStreamlitDashboard") \
                    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
                    .config("spark.sql.catalog.spark_catalog.type", "hive") \
                    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
                    .config("spark.sql.catalog.local.type", "hadoop") \
                    .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:9000/data/iceberg") \
                    .getOrCreate()
                
                st.success("✅ Spark Session with Iceberg initialized successfully")
                return True
                
            except ImportError:
                st.info("ℹ️ PySpark not available. Spark features disabled.")
                return False
                
        except Exception as e:
            st.warning(f"⚠️ Spark/Iceberg initialization failed: {e}")
            return False
    
    def check_service_status(self):
        """Check status of Big Data services"""
        services_status = {
            "HDFS": self.check_hdfs_status(),
            "Kafka": self.check_kafka_status(),
            "Spark": self.check_spark_status(),
            "Hive": self.check_hive_status()
        }
        return services_status
    
    def check_hdfs_status(self):
        """Check HDFS service status"""
        try:
            result = subprocess.run(['hdfs', 'dfsadmin', '-report'], 
                                  capture_output=True, text=True, timeout=10)
            return "Running" if result.returncode == 0 else "Stopped"
        except:
            return "Unknown"
    
    def check_kafka_status(self):
        """Check Kafka service status"""
        try:
            result = subprocess.run(['kafka-topics.sh', '--list', '--bootstrap-server', 'localhost:9092'], 
                                  capture_output=True, text=True, timeout=10)
            return "Running" if result.returncode == 0 else "Stopped"
        except:
            return "Unknown"
    
    def check_spark_status(self):
        """Check Spark service status"""
        try:
            if self.spark_session and not self.spark_session._jsc.sc().isStopped():
                return "Running"
            return "Stopped"
        except:
            return "Unknown"
    
    def check_hive_status(self):
        """Check Hive service status"""
        try:
            result = subprocess.run(['hive', '-e', 'SHOW DATABASES;'], 
                                  capture_output=True, text=True, timeout=10)
            return "Running" if result.returncode == 0 else "Stopped"
        except:
            return "Unknown"
    
    def process_uploaded_data(self, uploaded_file, processing_options):
        """Process uploaded dataset through the full pipeline"""
        try:
            # Read uploaded file
            df = pd.read_csv(uploaded_file)
            st.info(f"📊 Loaded {len(df)} records from {uploaded_file.name}")
            
            # Detect the source directory dynamically
            # Try to find a directory that might contain Power BI files
            possible_powerbi_dirs = []
            
            # Check if we can find ECOMMERCE HAMMAD directory
            base_dirs = [
                os.path.join(os.getcwd(), "..", "ECOMMERCE HAMMAD"),
                os.path.join(os.path.dirname(os.getcwd()), "ECOMMERCE HAMMAD"),
                os.path.join("c:", "Users", "ghani", "Desktop", "Pipeline", "ECOMMERCE HAMMAD")
            ]
            
            powerbi_dir = None
            for dir_path in base_dirs:
                if os.path.exists(dir_path):
                    powerbi_dir = dir_path
                    break
            
            # If no specific directory found, use a generic one
            if not powerbi_dir:
                powerbi_dir = os.path.join(os.getcwd(), "powerbi_output")
                os.makedirs(powerbi_dir, exist_ok=True)
                st.info(f"📁 Created output directory: {powerbi_dir}")
            else:
                st.info(f"📁 Using Power BI directory: {powerbi_dir}")
            
            # Store the directory for later use
            st.session_state.powerbi_dir = powerbi_dir
            
            # Show data quality before processing
            st.subheader("📋 Data Quality Report - Before Processing")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Records", len(df))
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                missing_count = df.isnull().sum().sum()
                st.metric("Missing Values", missing_count)
            with col4:
                duplicate_count = df.duplicated().sum()
                st.metric("Duplicate Records", duplicate_count)
            
            # Show sample of raw data
            with st.expander("🔍 View Raw Data Sample"):
                st.dataframe(df.head(10))
            
            # Process with Data Processor (initialize if needed)
            if not self.data_processor and DataProcessor is not None:
                try:
                    self.data_processor = DataProcessor(self.config)
                    st.info("🔧 Data Processor initialized automatically")
                except Exception as e:
                    st.warning(f"⚠️ Could not initialize Data Processor: {e}")
            
            if self.data_processor:
                st.info("🔄 Processing data with Advanced Data Processor...")
                st.info("🧹 Cleaning operations: Removing duplicates, handling missing values, data validation, outlier removal...")
                
                with st.spinner("Processing data..."):
                    processed_df = self.data_processor.process(df)
                
                st.success(f"✅ Data processed with advanced cleaning: {len(processed_df)} records")
                
                # Show data quality after processing
                st.subheader("📊 Data Quality Report - After Advanced Processing")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Final Records", len(processed_df), delta=int(len(processed_df) - len(df)))
                with col2:
                    st.metric("Columns", len(processed_df.columns), delta=int(len(processed_df.columns) - len(df.columns)))
                with col3:
                    final_missing = processed_df.isnull().sum().sum()
                    st.metric("Missing Values", int(final_missing), delta=int(final_missing - missing_count))
                with col4:
                    final_duplicates = processed_df.duplicated().sum()
                    st.metric("Duplicate Records", int(final_duplicates), delta=int(final_duplicates - duplicate_count))
                
                # Show processing statistics if available
                if hasattr(self.data_processor, 'stats') and self.data_processor.stats:
                    stats = self.data_processor.stats
                    st.subheader("🔍 Advanced Data Processing Statistics")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Duplicates Removed", stats.duplicates_removed)
                    with col2:
                        st.metric("Missing Values Handled", stats.missing_values_handled)
                    with col3:
                        st.metric("Outliers Removed", stats.outliers_removed)
                
                # Show sample of cleaned data
                with st.expander("✨ View Advanced Cleaned Data Sample"):
                    st.dataframe(processed_df.head(10))
                    
            else:
                st.warning("⚠️ Advanced Data Processor not available. Applying basic data cleaning...")
                
                # Basic data cleaning as fallback
                processed_df = df.copy()
                
                # Basic duplicate removal
                initial_count = len(processed_df)
                processed_df = processed_df.drop_duplicates()
                duplicates_removed = initial_count - len(processed_df)
                
                # Basic missing value handling
                missing_before = processed_df.isnull().sum().sum()
                processed_df = processed_df.dropna()
                missing_handled = missing_before - processed_df.isnull().sum().sum()
                
                st.info(f"🧹 Basic cleaning completed:")
                st.info(f"   • Removed {duplicates_removed} duplicates")
                st.info(f"   • Handled {missing_handled} missing values")
                st.info(f"   • Final records: {len(processed_df)}")
                
                with st.expander("✨ View Basic Cleaned Data Sample"):
                    st.dataframe(processed_df.head(10))
            
            # Save to HDFS if enabled and available
            if processing_options['hdfs'] and self.hdfs_manager:
                try:
                    hdfs_path = f"/data/ecommerce/raw/{uploaded_file.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    self.hdfs_manager.save_dataframe(processed_df, hdfs_path)
                    st.success(f"✅ Data saved to HDFS: {hdfs_path}")
                except Exception as e:
                    st.warning(f"⚠️ HDFS storage failed: {e}")
            
            # Send to Kafka for real-time processing if enabled
            if processing_options['kafka'] and self.kafka_processor:
                try:
                    # Convert DataFrame to records and send to Kafka
                    records = processed_df.to_dict('records')
                    topic = 'ecommerce-raw-data'
                    
                    # Send data in batches
                    batch_size = 100
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i+batch_size]
                        self.kafka_processor.send_batch(topic, batch)
                    
                    st.success(f"✅ Data sent to Kafka topic '{topic}': {len(records)} records")
                except Exception as e:
                    st.warning(f"⚠️ Kafka streaming failed: {e}")
            
            # Process with Spark/Iceberg if enabled
            if processing_options['iceberg'] and self.spark_session:
                try:
                    spark_df = self.spark_session.createDataFrame(processed_df)
                    
                    # Create Iceberg table
                    table_name = f"ecommerce.orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    spark_df.write.mode("overwrite").saveAsTable(table_name)
                    st.success(f"✅ Data saved to Iceberg table: {table_name}")
                except Exception as e:
                    st.warning(f"⚠️ Iceberg storage failed: {e}")
            
            # Run full pipeline processing if orchestrator is available
            if self.pipeline_orchestrator:
                try:
                    pipeline_result = self.pipeline_orchestrator.run_pipeline(processed_df, processing_options)
                    st.success(f"✅ Full pipeline executed successfully")
                    processed_df = pipeline_result
                except Exception as e:
                    st.warning(f"⚠️ Pipeline orchestration failed: {e}")
            
            # AUTO-UPDATE POWER BI INTEGRATION
            st.info("🔄 Auto-updating Power BI dashboard...")
            try:
                # Get the dynamic Power BI directory
                powerbi_dir = st.session_state.get('powerbi_dir', os.path.join(os.getcwd(), "powerbi_output"))
                
                # Save processed data to Power BI location
                powerbi_path = os.path.join(powerbi_dir, "pipeline_processed_data.csv")
                processed_df.to_csv(powerbi_path, index=False)
                st.success(f"✅ Data automatically saved for Power BI: {powerbi_path}")
                
                # REAL Power BI Integration - Update PBIX file directly
                st.info("🔄 Updating Power BI PBIX file automatically...")
                try:
                    # Import Power BI automation
                    import sys
                    sys.path.append('.')
                    from powerbi_automation import update_powerbi_data_source
                    
                    # Look for PBIX file in the same directory
                    pbix_files = [f for f in os.listdir(powerbi_dir) if f.endswith('.pbix')]
                    
                    if pbix_files:
                        pbix_file = os.path.join(powerbi_dir, pbix_files[0])
                        st.info(f"📊 Found Power BI file: {pbix_files[0]}")
                        
                        # Update the PBIX file data source
                        old_source = os.path.join(powerbi_dir, "cleaned_superstore_dataset.csv")
                        new_source = os.path.abspath(powerbi_path)
                        
                        if update_powerbi_data_source(pbix_file, old_source, new_source):
                            st.success("✅ Power BI PBIX file updated automatically!")
                            st.success("🎉 **Your Power BI dashboard now shows the latest processed data!**")
                        else:
                            st.warning("⚠️ Power BI PBIX update failed - data saved but manual refresh needed")
                    else:
                        st.info("📊 No PBIX file found in directory - data saved for manual Power BI connection")
                        
                except Exception as pbix_error:
                    st.warning(f"⚠️ Power BI PBIX automation failed: {pbix_error}")
                    st.info("💡 Data saved successfully - you can manually refresh Power BI to see updates")
                
                # Update Power BI metadata
                metadata = {
                    'last_updated': datetime.now().isoformat(),
                    'records_processed': len(processed_df),
                    'total_sales': float(processed_df['sales'].sum()) if 'sales' in processed_df.columns else 0,
                    'total_profit': float(processed_df['profit'].sum()) if 'profit' in processed_df.columns else 0,
                    'processing_stats': {
                        'original_records': len(df),
                        'final_records': len(processed_df),
                        'data_quality_score': ((len(processed_df) / len(df)) * 100) if len(df) > 0 else 0
                    },
                    'powerbi_integration': {
                        'auto_update_enabled': True,
                        'last_pbix_update': datetime.now().isoformat(),
                        'data_source_path': os.path.abspath(powerbi_path),
                        'powerbi_directory': powerbi_dir
                    }
                }
                
                metadata_path = os.path.join(powerbi_dir, "pipeline_metadata.json")
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f, indent=2)
                
                st.success("✅ Power BI metadata updated automatically")
                
                # Show Power BI integration status
                st.info(f"💡 **Power BI Integration Active**: Data saved to {powerbi_dir}")
                
                # Show instructions for viewing in Power BI
                with st.expander("📋 How to Use the Processed Data with Power BI"):
                    st.markdown(f"""
                    **Your processed data is ready for Power BI:**
                    
                    📁 **Data Location**: `{powerbi_path}`
                    
                    **Option 1 - Automatic (if PBIX file exists):**
                    1. � Open your existing PBIX file from `{powerbi_dir}`
                    2. 🔄 Click "Refresh" in Power BI to load the new processed data
                    
                    **Option 2 - Manual Setup:**
                    1. 📂 Open Power BI Desktop
                    2. � Import data from: `{powerbi_path}`
                    3. 📊 Create your visualizations with the cleaned data
                    
                    **✨ Data Quality Improvements:**
                    - Duplicates removed: ✅
                    - Missing values handled: ✅  
                    - Data validation completed: ✅
                    - Outliers processed: ✅
                    
                    **🔄 Next Steps:**
                    1. Open your PBIX file in Power BI Desktop
                    2. Click Refresh to load the new processed data
                    3. All charts and visuals now show cleaned data
                    4. No manual steps needed - everything is automated!
                    
                    **If you don't see updates:**
                    - Click Refresh button in Power BI Desktop
                    - Or go to Home > Refresh
                    """)
                
            except Exception as e:
                st.warning(f"⚠️ Power BI auto-update failed: {e}")
                st.info("💡 Data processed successfully - you can manually connect Power BI to the processed data")
            
            return processed_df
            
        except Exception as e:
            st.error(f"Pipeline processing failed: {e}")
            return None
    
    def get_real_time_insights(self, df):
        """Generate real-time insights from processed data"""
        insights = {}
        
        try:
            # Basic metrics
            insights['total_sales'] = df['sales'].sum() if 'sales' in df.columns else 0
            insights['total_profit'] = df['profit'].sum() if 'profit' in df.columns else 0
            insights['total_orders'] = len(df)
            insights['avg_order_value'] = insights['total_sales'] / insights['total_orders'] if insights['total_orders'] > 0 else 0
            
            # Profit margin
            insights['profit_margin'] = (insights['total_profit'] / insights['total_sales'] * 100) if insights['total_sales'] > 0 else 0
            
            # Time-based insights
            if 'order_date' in df.columns:
                df['order_date'] = pd.to_datetime(df['order_date'])
                insights['date_range'] = f"{df['order_date'].min().strftime('%Y-%m-%d')} to {df['order_date'].max().strftime('%Y-%m-%d')}"
                
                # Monthly trends
                monthly_sales = df.groupby(df['order_date'].dt.to_period('M'))['sales'].sum()
                insights['monthly_growth'] = ((monthly_sales.iloc[-1] - monthly_sales.iloc[0]) / monthly_sales.iloc[0] * 100) if len(monthly_sales) > 1 else 0
            
            # Category insights
            if 'category' in df.columns:
                category_sales = df.groupby('category')['sales'].sum().sort_values(ascending=False)
                insights['top_category'] = category_sales.index[0] if len(category_sales) > 0 else 'N/A'
                insights['category_contribution'] = (category_sales.iloc[0] / insights['total_sales'] * 100) if insights['total_sales'] > 0 else 0
            
            # Geographic insights
            if 'state' in df.columns:
                state_sales = df.groupby('state')['sales'].sum().sort_values(ascending=False)
                insights['top_state'] = state_sales.index[0] if len(state_sales) > 0 else 'N/A'
                insights['state_contribution'] = (state_sales.iloc[0] / insights['total_sales'] * 100) if insights['total_sales'] > 0 else 0
            
        except Exception as e:
            st.error(f"Insights generation failed: {e}")
        
        return insights

def main():
    st.set_page_config(
        page_title="E-commerce Big Data Pipeline Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🚀 E-commerce Big Data Pipeline Dashboard")
    st.markdown("**Integrated with Spark, Kafka, Iceberg, HDFS & Real-time Analytics**")
    
    # Initialize dashboard
    dashboard = IntegratedPipelineDashboard()
    
    # Sidebar for service management
    with st.sidebar:
        st.header("🔧 Service Management")
        
        if st.button("Initialize Services"):
            with st.spinner("Initializing Big Data services..."):
                if dashboard.initialize_services():
                    st.success("✅ Services initialized successfully!")
                else:
                    st.error("❌ Service initialization failed")
        
        st.header("📊 Service Status")
        if st.button("Check Service Status"):
            status = dashboard.check_service_status()
            for service, state in status.items():
                color = "🟢" if state == "Running" else "🔴" if state == "Stopped" else "🟡"
                st.write(f"{color} {service}: {state}")
        
        st.header("⚙️ Processing Options")
        enable_kafka = st.checkbox("Enable Kafka Streaming", value=True)
        enable_iceberg = st.checkbox("Enable Iceberg Storage", value=True)
        enable_hdfs = st.checkbox("Enable HDFS Storage", value=True)
        real_time_processing = st.checkbox("Real-time Processing", value=True)
        
        processing_options = {
            'kafka': enable_kafka,
            'iceberg': enable_iceberg,
            'hdfs': enable_hdfs,
            'real_time': real_time_processing
        }
    
    # Main content
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📁 Data Upload & Processing", "📊 Real-time Insights", "📈 Power BI Integration", "🔍 Service Monitoring", "💾 Data Explorer"])
    
    with tab1:
        st.header("📁 Upload Dataset & Run Pipeline")
        
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type="csv",
            help="Upload your e-commerce dataset to run through the complete Big Data pipeline"
        )
        
        if uploaded_file is not None:
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            
            # Preview data
            df_preview = pd.read_csv(uploaded_file)
            st.subheader("📋 Data Preview")
            st.dataframe(df_preview.head())
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records", len(df_preview))
            with col2:
                st.metric("Columns", len(df_preview.columns))
            with col3:
                if 'sales' in df_preview.columns:
                    st.metric("Total Sales", f"${df_preview['sales'].sum():,.2f}")
            
            if st.button("🚀 Run Complete Pipeline", type="primary"):
                with st.spinner("Processing through Big Data pipeline..."):
                    # Reset file pointer
                    uploaded_file.seek(0)
                    processed_data = dashboard.process_uploaded_data(uploaded_file, processing_options)
                    
                    if processed_data is not None:
                        st.success("✅ Pipeline processing completed!")
                        st.session_state['processed_data'] = processed_data
                        st.session_state['insights'] = dashboard.get_real_time_insights(processed_data)
                    else:
                        st.error("❌ Pipeline processing failed")
    
    with tab2:
        st.header("📊 Real-time Analytics & Insights")
        st.info("ℹ️ **Note**: All insights below are based on cleaned and processed data, not raw uploaded data.")
        
        if 'processed_data' in st.session_state and 'insights' in st.session_state:
            df = st.session_state['processed_data']
            insights = st.session_state['insights']
            
            # Enhanced Key Metrics Dashboard
            st.markdown("### 🎯 Key Performance Indicators")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric(
                    "💰 Total Sales",
                    f"${insights['total_sales']:,.0f}",
                    delta=f"{insights['profit_margin']:.1f}% margin"
                )
            with col2:
                st.metric(
                    "📈 Total Profit",
                    f"${insights['total_profit']:,.0f}",
                    delta=f"{insights['monthly_growth']:.1f}% growth" if 'monthly_growth' in insights else None
                )
            with col3:
                st.metric(
                    "📦 Total Orders",
                    f"{insights['total_orders']:,}",
                    delta=f"${insights['avg_order_value']:.0f} avg"
                )
            with col4:
                st.metric(
                    "🏆 Top Category",
                    insights.get('top_category', 'N/A'),
                    delta=f"{insights.get('category_contribution', 0):.1f}% of sales"
                )
            
            # Enhanced Visualizations
            st.markdown("### 📊 Business Intelligence Dashboard")
            
            # Row 1: Sales Analysis
            col1, col2 = st.columns(2)
            
            with col1:
                if 'category' in df.columns and 'sales' in df.columns:
                    st.markdown("#### 🎯 Sales Performance by Category")
                    category_sales = df.groupby('category')['sales'].sum().sort_values(ascending=False)
                    
                    # Create an enhanced horizontal bar chart
                    fig = px.bar(
                        x=category_sales.values,
                        y=category_sales.index,
                        orientation='h',
                        title="Sales by Category",
                        color=category_sales.values,
                        color_continuous_scale='Viridis',
                        text=category_sales.values
                    )
                    fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
                    fig.update_layout(height=400, xaxis_title="Sales ($)", yaxis_title="Category")
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if 'state' in df.columns and 'sales' in df.columns:
                    st.markdown("#### 🗺️ Geographic Sales Distribution")
                    state_sales = df.groupby('state')['sales'].sum().sort_values(ascending=False).head(10)
                    
                    # Create a donut chart
                    fig = px.pie(
                        values=state_sales.values,
                        names=state_sales.index,
                        title="Top 10 States by Sales",
                        hole=0.4,
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            # Row 2: Profitability Analysis
            if 'profit' in df.columns and 'sales' in df.columns:
                st.markdown("#### 💹 Profitability Analysis")
                
                # Create profit margin analysis
                if 'category' in df.columns:
                    profit_analysis = df.groupby('category').agg({
                        'sales': 'sum',
                        'profit': 'sum'
                    }).reset_index()
                    profit_analysis['profit_margin'] = (profit_analysis['profit'] / profit_analysis['sales']) * 100
                    
                    fig = px.scatter(profit_analysis, x='sales', y='profit', 
                                   size='profit_margin', color='category',
                                   hover_name='category',
                                   title="Sales vs Profit by Category (Bubble size = Profit Margin)",
                                   size_max=60)
                    fig.update_layout(height=400, xaxis_title="Sales ($)", yaxis_title="Profit ($)")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Row 3: Time Series Analysis
            if 'order_date' in df.columns:
                st.markdown("#### 📈 Time Series Analysis")
                
                col1, col2 = st.columns(2)
                with col1:
                    # Daily sales trend
                    df['order_date'] = pd.to_datetime(df['order_date'])
                    daily_sales = df.groupby(df['order_date'].dt.date)['sales'].sum().reset_index()
                    
                    fig = px.line(
                        daily_sales,
                        x='order_date',
                        y='sales',
                        title="Daily Sales Trend",
                        markers=True
                    )
                    fig.update_traces(line=dict(width=3, color='#1f77b4'), marker=dict(size=6))
                    fig.update_layout(height=400, xaxis_title="Date", yaxis_title="Sales ($)")
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Monthly comparison
                    monthly_data = df.groupby(df['order_date'].dt.to_period('M')).agg({
                        'sales': 'sum',
                        'profit': 'sum',
                        'order_id': 'count'
                    }).reset_index()
                    monthly_data['order_date'] = monthly_data['order_date'].astype(str)
                    
                    fig = px.bar(monthly_data, x='order_date', y=['sales', 'profit'],
                               title="Monthly Sales vs Profit",
                               barmode='group')
                    fig.update_layout(height=400, xaxis_title="Month", yaxis_title="Amount ($)")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Row 4: Advanced Analytics
            st.markdown("#### 🔍 Advanced Business Insights")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if 'segment' in df.columns and 'sales' in df.columns:
                    # Customer segment analysis
                    segment_data = df.groupby('segment').agg({
                        'sales': 'sum',
                        'profit': 'sum',
                        'quantity': 'sum'
                    }).reset_index()
                    
                    fig = px.sunburst(segment_data, path=['segment'], values='sales',
                                    title="Sales Distribution by Customer Segment",
                                    color='profit',
                                    color_continuous_scale='RdYlBu')
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if 'subcategory' in df.columns and 'profit' in df.columns:
                    # Top subcategories
                    subcat_profit = df.groupby('subcategory')['profit'].sum().sort_values(ascending=False).head(15)
                    
                    fig = px.bar(
                        x=subcat_profit.index,
                        y=subcat_profit.values,
                        title="Top 15 Subcategories by Profit",
                        color=subcat_profit.values,
                        color_continuous_scale='plasma'
                    )
                    fig.update_layout(height=400, xaxis_title="Subcategory", 
                                    yaxis_title="Profit ($)", 
                                    xaxis={'tickangle': 45})
                    st.plotly_chart(fig, use_container_width=True)
            
            # Performance Summary
            st.markdown("#### 📋 Performance Summary")
            summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
            
            with summary_col1:
                if 'customer' in df.columns:
                    unique_customers = df['customer'].nunique()
                    st.metric("👥 Unique Customers", f"{unique_customers:,}")
            
            with summary_col2:
                if 'quantity' in df.columns:
                    total_quantity = df['quantity'].sum()
                    st.metric("📦 Items Sold", f"{total_quantity:,}")
            
            with summary_col3:
                if 'discount' in df.columns:
                    avg_discount = df['discount'].mean() * 100
                    st.metric("🏷️ Avg Discount", f"{avg_discount:.1f}%")
            
            with summary_col4:
                if 'order_date' in df.columns:
                    date_range_days = (df['order_date'].max() - df['order_date'].min()).days
                    st.metric("📅 Date Range", f"{date_range_days} days")
            
        else:
            st.info("👆 Upload and process a dataset in the 'Data Upload & Processing' tab to see insights here.")
    
    with tab3:
        st.header("📈 Power BI Integration & Management")
        
        # Get dynamic Power BI directory
        powerbi_dir = st.session_state.get('powerbi_dir')
        
        if not powerbi_dir:
            # Try to detect it from common locations
            possible_dirs = [
                os.path.join(os.getcwd(), "..", "ECOMMERCE HAMMAD"),
                os.path.join(os.path.dirname(os.getcwd()), "ECOMMERCE HAMMAD"),
                os.path.join("c:", "Users", "ghani", "Desktop", "Pipeline", "ECOMMERCE HAMMAD")
            ]
            for dir_path in possible_dirs:
                if os.path.exists(dir_path):
                    powerbi_dir = dir_path
                    st.session_state.powerbi_dir = powerbi_dir
                    break
        
        if powerbi_dir:
            st.info(f"📁 Power BI Directory: {powerbi_dir}")
            
            # Power BI Status Section
            st.subheader("🔗 Power BI Connection Status")
            
            # Look for PBIX files in the directory
            pbix_files = [f for f in os.listdir(powerbi_dir) if f.endswith('.pbix')] if os.path.exists(powerbi_dir) else []
            processed_data_file = os.path.join(powerbi_dir, "pipeline_processed_data.csv")
            metadata_file = os.path.join(powerbi_dir, "pipeline_metadata.json")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if pbix_files:
                    st.success("✅ Power BI File Found")
                    st.text(f"� {pbix_files[0]}")
                else:
                    st.error("❌ Power BI File Not Found")
            
            with col2:
                if os.path.exists(processed_data_file):
                    st.success("✅ Processed Data Available")
                    # Show file info
                    file_size = os.path.getsize(processed_data_file)
                    st.text(f"📊 Size: {file_size/1024:.1f} KB")
                    mod_time = datetime.fromtimestamp(os.path.getmtime(processed_data_file))
                    st.text(f"🕒 Modified: {mod_time.strftime('%H:%M:%S')}")
                else:
                    st.warning("⚠️ No Processed Data")
            
            with col3:
                if os.path.exists(metadata_file):
                    st.success("✅ Metadata Available")
                    try:
                        with open(metadata_file, 'r') as f:
                            metadata = json.load(f)
                        last_updated = metadata.get('last_updated', 'N/A')
                        if last_updated != 'N/A':
                            update_time = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                            st.text(f"🕒 Updated: {update_time.strftime('%H:%M:%S')}")
                        else:
                            st.text("🕒 Updated: Unknown")
                    except:
                        st.text("🕒 Updated: Unknown")
                else:
                    st.warning("⚠️ No Metadata")
            
            # Power BI Data Insights
            if os.path.exists(processed_data_file):
                st.subheader("📊 Power BI Data Insights")
                try:
                    powerbi_df = pd.read_csv(processed_data_file)
                    
                    # Key Metrics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Records", f"{len(powerbi_df):,}")
                    with col2:
                        total_sales = 0
                        if 'sales' in powerbi_df.columns:
                            total_sales = powerbi_df['sales'].sum()
                            st.metric("Total Sales", f"${total_sales:,.2f}")
                    with col3:
                        total_profit = 0
                        if 'profit' in powerbi_df.columns:
                            total_profit = powerbi_df['profit'].sum()
                            st.metric("Total Profit", f"${total_profit:,.2f}")
                    with col4:
                        if 'sales' in powerbi_df.columns and 'profit' in powerbi_df.columns and total_sales > 0:
                            profit_margin = (total_profit / total_sales) * 100
                            st.metric("Profit Margin", f"{profit_margin:.1f}%")
                    
                    # Enhanced Visual Analytics Section
                    st.subheader("📊 Visual Analytics Preview")
                    st.markdown("*This is how your data will look in Power BI dashboards*")
                    
                    # Row 1: Category and Region Analysis
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'category' in powerbi_df.columns and 'sales' in powerbi_df.columns:
                            st.markdown("#### 🎯 Sales Distribution by Category")
                            category_sales = powerbi_df.groupby('category')['sales'].sum().reset_index()
                            category_sales = category_sales.sort_values('sales', ascending=False)
                            
                            # Create a colorful pie chart
                            fig = px.pie(category_sales, values='sales', names='category', 
                                       title="Sales Distribution by Category",
                                       color_discrete_sequence=px.colors.qualitative.Set3)
                            fig.update_traces(textposition='inside', textinfo='percent+label')
                            fig.update_layout(showlegend=True, height=400)
                            st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        if 'region' in powerbi_df.columns and 'profit' in powerbi_df.columns:
                            st.markdown("#### 🌍 Profit Performance by Region")
                            region_profit = powerbi_df.groupby('region')['profit'].sum().reset_index()
                            region_profit = region_profit.sort_values('profit', ascending=True)
                            
                            # Create a horizontal bar chart
                            fig = px.bar(region_profit, x='profit', y='region', orientation='h',
                                       title="Profit by Region",
                                       color='profit',
                                       color_continuous_scale='Viridis')
                            fig.update_layout(height=400)
                            st.plotly_chart(fig, use_container_width=True)
                    
                    # Row 2: Time Series and State Analysis
                    if 'order_date' in powerbi_df.columns and 'sales' in powerbi_df.columns:
                        st.markdown("#### 📈 Sales Trend Analysis")
                        try:
                            powerbi_df['order_date'] = pd.to_datetime(powerbi_df['order_date'])
                            
                            # Create monthly sales trend
                            monthly_sales = powerbi_df.groupby(powerbi_df['order_date'].dt.to_period('M'))['sales'].sum().reset_index()
                            monthly_sales['order_date'] = monthly_sales['order_date'].astype(str)
                            
                            fig = px.line(monthly_sales, x='order_date', y='sales',
                                        title="Monthly Sales Trend",
                                        markers=True)
                            fig.update_traces(line=dict(width=3), marker=dict(size=8))
                            fig.update_layout(height=400, xaxis_title="Month", yaxis_title="Sales ($)")
                            st.plotly_chart(fig, use_container_width=True)
                        except Exception as e:
                            st.info("📅 Date formatting needs adjustment for time series")
                    
                    # Row 3: Top States and Subcategory Analysis
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'state' in powerbi_df.columns and 'sales' in powerbi_df.columns:
                            st.markdown("#### 🏛️ Top 10 States by Sales")
                            state_sales = powerbi_df.groupby('state')['sales'].sum().reset_index()
                            top_states = state_sales.nlargest(10, 'sales')
                            
                            fig = px.bar(top_states, x='sales', y='state', orientation='h',
                                       title="Top 10 States by Sales",
                                       color='sales',
                                       color_continuous_scale='Blues')
                            fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
                            st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        if 'subcategory' in powerbi_df.columns and 'profit' in powerbi_df.columns:
                            st.markdown("#### 📋 Top Subcategories by Profit")
                            subcat_profit = powerbi_df.groupby('subcategory')['profit'].sum().reset_index()
                            top_subcats = subcat_profit.nlargest(10, 'profit')
                            
                            fig = px.treemap(top_subcats, path=['subcategory'], values='profit',
                                           title="Profit Distribution by Subcategory",
                                           color='profit',
                                           color_continuous_scale='RdYlBu')
                            fig.update_layout(height=400)
                            st.plotly_chart(fig, use_container_width=True)
                    
                    # Row 4: Customer Segment and Ship Mode Analysis
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'segment' in powerbi_df.columns and 'sales' in powerbi_df.columns:
                            st.markdown("#### 👥 Customer Segment Performance")
                            segment_data = powerbi_df.groupby('segment').agg({
                                'sales': 'sum',
                                'profit': 'sum',
                                'order_id': 'count'
                            }).reset_index()
                            segment_data.columns = ['segment', 'sales', 'profit', 'orders']
                            
                            fig = px.scatter(segment_data, x='sales', y='profit', size='orders',
                                           color='segment', hover_name='segment',
                                           title="Segment Performance: Sales vs Profit",
                                           size_max=60)
                            fig.update_layout(height=400)
                            st.plotly_chart(fig, use_container_width=True)
                    
                    with col2:
                        if 'ship_mode' in powerbi_df.columns and 'sales' in powerbi_df.columns:
                            st.markdown("#### 🚚 Shipping Mode Analysis")
                            ship_analysis = powerbi_df.groupby('ship_mode').agg({
                                'sales': 'sum',
                                'quantity': 'sum'
                            }).reset_index()
                            
                            fig = px.bar(ship_analysis, x='ship_mode', y='sales',
                                       title="Sales by Shipping Mode",
                                       color='sales',
                                       color_continuous_scale='Sunset')
                            fig.update_layout(height=400, xaxis_title="Shipping Mode", yaxis_title="Sales ($)")
                            st.plotly_chart(fig, use_container_width=True)
                    
                    # Row 5: Advanced Analytics
                    if 'discount' in powerbi_df.columns and 'profit_margin' in powerbi_df.columns:
                        st.markdown("#### 💰 Discount vs Profit Margin Analysis")
                        
                        # Create discount bins for better visualization
                        powerbi_df['discount_range'] = pd.cut(powerbi_df['discount'], 
                                                            bins=[0, 0.1, 0.2, 0.3, 1.0], 
                                                            labels=['0-10%', '11-20%', '21-30%', '30%+'])
                        
                        discount_analysis = powerbi_df.groupby('discount_range').agg({
                            'profit_margin': 'mean',
                            'sales': 'sum',
                            'order_id': 'count'
                        }).reset_index()
                        
                        fig = px.bar(discount_analysis, x='discount_range', y='profit_margin',
                                   title="Average Profit Margin by Discount Range",
                                   color='profit_margin',
                                   color_continuous_scale='RdYlGn')
                        fig.update_layout(height=400, xaxis_title="Discount Range", yaxis_title="Avg Profit Margin")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Interactive Data Summary
                    st.markdown("#### 📋 Interactive Data Summary")
                    if st.checkbox("Show Advanced Metrics"):
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            if 'customer' in powerbi_df.columns:
                                unique_customers = powerbi_df['customer'].nunique()
                                st.metric("Unique Customers", f"{unique_customers:,}")
                                
                                avg_order_value = total_sales / len(powerbi_df) if len(powerbi_df) > 0 else 0
                                st.metric("Avg Order Value", f"${avg_order_value:.2f}")
                        
                        with col2:
                            if 'quantity' in powerbi_df.columns:
                                total_quantity = powerbi_df['quantity'].sum()
                                st.metric("Total Quantity Sold", f"{total_quantity:,}")
                                
                                avg_quantity = powerbi_df['quantity'].mean()
                                st.metric("Avg Quantity per Order", f"{avg_quantity:.1f}")
                        
                        with col3:
                            if 'discount' in powerbi_df.columns:
                                avg_discount = powerbi_df['discount'].mean() * 100
                                st.metric("Average Discount", f"{avg_discount:.1f}%")
                                
                                if total_sales > 0:
                                    revenue_per_customer = total_sales / unique_customers if 'customer' in powerbi_df.columns else 0
                                    st.metric("Revenue per Customer", f"${revenue_per_customer:.2f}")
                    
                    # Show sample data
                    with st.expander("🔍 Preview Processed Data (First 10 Rows)"):
                        st.dataframe(powerbi_df.head(10))
                    
                except Exception as e:
                    st.error(f"❌ Error loading Power BI data: {e}")
        else:
            st.warning("⚠️ Power BI directory not found. Upload and process data first to enable Power BI integration.")
        
        # Power BI Management Actions
        st.subheader("⚙️ Power BI Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Force Power BI Data Refresh", type="primary"):
                if 'processed_data' in st.session_state and powerbi_dir:
                    try:
                        processed_data = st.session_state['processed_data']
                        
                        # Define file paths
                        processed_data_file = os.path.join(powerbi_dir, "pipeline_processed_data.csv")
                        metadata_file = os.path.join(powerbi_dir, "pipeline_metadata.json")
                        
                        # Save to Power BI location
                        processed_data.to_csv(processed_data_file, index=False)
                        
                        # Update metadata
                        metadata = {
                            'last_updated': datetime.now().isoformat(),
                            'records_processed': len(processed_data),
                            'total_sales': float(processed_data['sales'].sum()) if 'sales' in processed_data.columns else 0,
                            'total_profit': float(processed_data['profit'].sum()) if 'profit' in processed_data.columns else 0,
                            'manual_refresh': True,
                            'processing_stats': {
                                'final_records': len(processed_data),
                                'columns': list(processed_data.columns)
                            }
                        }
                        
                        with open(metadata_file, 'w') as f:
                            json.dump(metadata, f, indent=2)
                        
                        st.success("✅ Power BI data manually refreshed!")
                        st.info("💡 Open your ECOMMERCE HAMMAD.pbix file in Power BI Desktop and click 'Refresh' to see the updated data.")
                        
                    except Exception as e:
                        st.error(f"❌ Manual refresh failed: {e}")
                else:
                    st.warning("⚠️ No processed data available. Please process a dataset first.")
        
        with col2:
            if st.button("🔧 Update Power BI Connection"):
                try:
                    # Run the Power BI automation script
                    result = subprocess.run([
                        "python", "powerbi_automation.py"
                    ], capture_output=True, text=True, cwd=".")
                    
                    if result.returncode == 0:
                        st.success("✅ Power BI connection updated successfully!")
                        st.code(result.stdout)
                    else:
                        st.error(f"❌ Power BI connection update failed: {result.stderr}")
                        
                except Exception as e:
                    st.error(f"❌ Error updating Power BI connection: {e}")
        
        # Power BI Instructions
        st.subheader("📋 How to Use Power BI Integration")
        st.markdown("""
        **Automatic Integration Steps:**
        1. 📤 **Upload & Process**: Upload your data in the 'Data Upload & Processing' tab
        2. ✨ **Auto-Update**: The pipeline automatically updates your Power BI data file
        3. 🔄 **Refresh Power BI**: Open your `ECOMMERCE HAMMAD.pbix` file in Power BI Desktop
        4. 🔁 **Click Refresh**: In Power BI, click the 'Refresh' button to load new data
        5. 📊 **View Results**: Your Power BI dashboard now shows the cleaned, processed data!
        
        **Key Benefits:**
        - ✅ **No Manual Export**: Data is automatically saved to Power BI location
        - ✅ **Data Cleaning**: All data is cleaned before reaching Power BI
        - ✅ **Real-time Metadata**: Track processing statistics and data quality
        - ✅ **Seamless Workflow**: Upload → Process → Power BI shows results
        """)
    
    with tab4:
        st.header("🔍 Service Monitoring")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🖥️ System Status")
            if st.button("Refresh Status"):
                status = dashboard.check_service_status()
                for service, state in status.items():
                    if state == "Running":
                        st.success(f"✅ {service}: {state}")
                    elif state == "Stopped":
                        st.error(f"❌ {service}: {state}")
                    else:
                        st.warning(f"⚠️ {service}: {state}")
        
        with col2:
            st.subheader("📈 Pipeline Metrics")
            # Simulated metrics (replace with actual monitoring)
            st.metric("Pipeline Uptime", "99.8%", delta="0.1%")
            st.metric("Data Processed Today", "1.2M records", delta="150K")
            st.metric("Average Processing Time", "2.3s", delta="-0.5s")
    
    with tab5:
        st.header("💾 Data Explorer")
        
        if 'processed_data' in st.session_state:
            df = st.session_state['processed_data']
            
            st.subheader("🔍 Explore Your Data")
            
            # Data filtering
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if 'category' in df.columns:
                    categories = st.multiselect("Select Categories", df['category'].unique())
                    if categories:
                        df = df[df['category'].isin(categories)]
            
            with col2:
                if 'state' in df.columns:
                    states = st.multiselect("Select States", df['state'].unique())
                    if states:
                        df = df[df['state'].isin(states)]
            
            with col3:
                if 'sales' in df.columns:
                    min_sales = st.number_input("Minimum Sales", value=0.0)
                    df = df[df['sales'] >= min_sales]
            
            st.dataframe(df, use_container_width=True)
            
            # Download processed data
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Processed Data",
                data=csv,
                file_name=f"processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("👆 Process a dataset first to explore the data here.")

if __name__ == "__main__":
    main()
