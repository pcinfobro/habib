"""
E-commerce Data Pipeline - Streamlit Dashboard
Integrated with Big Data Services (Spark, Kafka, Iceberg, HDFS)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from path_config import get_base_dirs, find_data_directory, get_powerbi_paths
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
    print(" All pipeline modules imported successfully")
except ImportError as e:
    pipeline_modules_available = False
    print(f" Pipeline modules not found (running in standalone mode): {e}")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_create_chart(data, chart_function, *args, **kwargs):
    """
    Safely create a plotly chart with validation for empty data
    
    Args:
        data: DataFrame to be used for the chart
        chart_function: Plotly function to create the chart (e.g., px.bar, px.treemap)
        *args, **kwargs: Arguments to pass to the chart function
    
    Returns:
        fig: Plotly figure object or None if data is empty
    """
    try:
        # Check if data is empty
        if data is None or len(data) == 0:
            return None
            
        # For charts that require numeric values, check if they exist
        if 'values' in kwargs:
            value_col = kwargs['values']
            if value_col in data.columns and data[value_col].sum() <= 0:
                return None
                
        # Create the chart
        fig = chart_function(data, *args, **kwargs)
        return fig
        
    except Exception as e:
        logger.error(f"Error creating chart: {e}")
        return None

def robust_csv_reader(file_path_or_buffer):
    """
    Robust CSV reader that handles various file formats and encoding issues
    Specifically designed for the 4 main files:
    1. superstore_dataset.csv (with BOM)
    2. cleaned_superstore_dataset.csv 
    3. navigator_ft-data_preview.csv
    4. MOCK_DATA.csv
    """
    
    try:
        # Try multiple approaches without external dependencies
        attempts = [
            # Standard UTF-8
            {'encoding': 'utf-8', 'sep': ','},
            # UTF-8 with BOM (for superstore files)
            {'encoding': 'utf-8-sig', 'sep': ','},
            # Windows encoding
            {'encoding': 'cp1252', 'sep': ','},
            # ISO encoding
            {'encoding': 'iso-8859-1', 'sep': ','},
            # Try semicolon separator
            {'encoding': 'utf-8', 'sep': ';'},
            # Latin-1 (handles most characters)
            {'encoding': 'latin-1', 'sep': ','},
        ]
        
        for attempt in attempts:
            try:
                # Reset file pointer if it's a file-like object
                if hasattr(file_path_or_buffer, 'seek'):
                    file_path_or_buffer.seek(0)
                
                df = pd.read_csv(
                    file_path_or_buffer, 
                    encoding=attempt['encoding'], 
                    sep=attempt['sep'],
                    low_memory=False,
                    skipinitialspace=True,
                    na_values=['', 'NA', 'N/A', 'null', 'NULL', 'nan'],
                    keep_default_na=True
                )
                
                # Clean up common issues
                if df is not None:
                    # Remove completely empty columns
                    df = df.dropna(axis=1, how='all')
                    
                    # Remove columns that are just index numbers (common issue)
                    cols_to_drop = []
                    for col in df.columns:
                        if (str(col).startswith('Unnamed') or 
                            str(col).strip() == '' or 
                            str(col).lower() in ['index', 'id'] and df[col].equals(df.index)):
                            cols_to_drop.append(col)
                    
                    if cols_to_drop:
                        df = df.drop(columns=cols_to_drop)
                    
                    # Clean column names (remove BOM, extra spaces)
                    df.columns = df.columns.astype(str).str.strip()
                    df.columns = df.columns.str.replace(r'^[\uFEFF\ufeff]*', '', regex=True)  # Remove BOM
                    
                    # Basic validation - must have at least 2 columns and 10 rows
                    if len(df.columns) >= 2 and len(df) >= 10:
                        st.success(f"✅ Successfully loaded {len(df)} records with {len(df.columns)} columns")
                        st.info(f"📝 Used encoding: {attempt['encoding']}, separator: '{attempt['sep']}'")
                        return df
                        
            except Exception as e:
                # Continue to next attempt
                continue
        
        # If all attempts fail, raise the last error
        raise Exception("Could not read CSV file with any encoding/separator combination")
        
    except Exception as e:
        st.error(f"❌ Error reading CSV file: {e}")
        st.info("💡 **Tip**: Ensure your CSV file is properly formatted and not corrupted")
        return None

def detect_column_mappings(df):
    """
    Detect and map common column patterns in different datasets
    Returns a dictionary mapping standard names to actual column names
    """
    column_mapping = {}
    
    # Convert column names to lowercase for matching
    lower_cols = {col.lower(): col for col in df.columns}
    
    # Sales/Revenue detection
    sales_patterns = ['sales', 'revenue', 'amount', 'total', 'value', 'price']
    for pattern in sales_patterns:
        matches = [col for col_lower, col in lower_cols.items() if pattern in col_lower]
        if matches:
            column_mapping['sales'] = matches[0]
            break
    
    # Profit detection
    profit_patterns = ['profit', 'margin', 'earnings', 'income']
    for pattern in profit_patterns:
        matches = [col for col_lower, col in lower_cols.items() if pattern in col_lower]
        if matches:
            column_mapping['profit'] = matches[0]
            break
    
    # Category detection
    category_patterns = ['category', 'type', 'class', 'group', 'segment', 'product_category']
    for pattern in category_patterns:
        matches = [col for col_lower, col in lower_cols.items() if pattern in col_lower]
        if matches:
            column_mapping['category'] = matches[0]
            break
    
    # Customer detection
    customer_patterns = ['customer', 'client', 'user', 'buyer', 'customer_name', 'customer_id']
    for pattern in customer_patterns:
        matches = [col for col_lower, col in lower_cols.items() if pattern in col_lower]
        if matches:
            column_mapping['customer'] = matches[0]
            break
    
    # Date detection
    date_patterns = ['date', 'time', 'order_date', 'purchase_date', 'transaction_date']
    for pattern in date_patterns:
        matches = [col for col_lower, col in lower_cols.items() if pattern in col_lower]
        if matches:
            column_mapping['date'] = matches[0]
            break
    
    # Region/Location detection
    region_patterns = ['region', 'state', 'country', 'location', 'area', 'city']
    for pattern in region_patterns:
        matches = [col for col_lower, col in lower_cols.items() if pattern in col_lower]
        if matches:
            column_mapping['region'] = matches[0]
            break
    
    # Quantity detection
    quantity_patterns = ['quantity', 'qty', 'amount', 'count', 'units']
    for pattern in quantity_patterns:
        matches = [col for col_lower, col in lower_cols.items() if pattern in col_lower and 'sales' not in col_lower]
        if matches:
            column_mapping['quantity'] = matches[0]
            break
    
    # Product detection
    product_patterns = ['product', 'item', 'sku', 'product_name', 'product_id']
    for pattern in product_patterns:
        matches = [col for col_lower, col in lower_cols.items() if pattern in col_lower]
        if matches:
            column_mapping['product'] = matches[0]
            break
    
    return column_mapping

def safe_chart(chart_func, fallback_message="Chart not available with current dataset"):
    """
    Wrapper function to safely execute chart creation and show fallback message if it fails
    """
    try:
        return chart_func()
    except Exception as e:
        st.warning(f"{fallback_message}: {str(e)}")
        return None

def check_dataset_compatibility(df):
    """
    Check dataset compatibility and suggest column mappings - simplified without sidebar
    """
    col_mapping = detect_column_mappings(df)
    return col_mapping

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
        
        # Data processing configuration - more lenient to preserve data
        self.data_processing_config = {
            'default_missing_strategy': 'fill_unknown',  # Don't drop rows, fill with 'Unknown'
            'missing_values': {
                # Specific strategies for key columns
                'sales': 'drop',  # Drop rows without sales data
                'profit': 0,      # Fill missing profit with 0
                'quantity': 1,    # Fill missing quantity with 1
                'discount': 0,    # Fill missing discount with 0
                'order_date': 'drop',  # Drop rows without order date
            },
            'outlier_threshold': 3.0,  # Keep more data by being less strict
            'validation_rules': {
                'sales': {'min': 0},
                'quantity': {'min': 0},
            }
        }
        
        # Auto-initialize Data Processor on startup
        self._auto_initialize_data_processor()
        
    def _auto_initialize_data_processor(self):
        """Automatically initialize data processor if available"""
        if DataProcessor is not None and not self.data_processor:
            try:
                self.data_processor = DataProcessor(self.data_processing_config)
            except Exception as e:
                # Silent fail - will use fallback cleaning
                pass
                
    def initialize_services(self):
        """Initialize Big Data services connections"""
        if not self.pipeline_modules_available:
            st.warning(" Pipeline modules not available. Running in standalone mode.")
            return False
            
        try:
            # Initialize Data Processor
            if DataProcessor is not None:
                self.data_processor = DataProcessor(self.data_processing_config)
                st.success(" Data Processor initialized")
            else:
                st.warning(" DataProcessor not available")
            
            # Initialize HDFS Manager
            try:
                if HDFSManager is not None:
                    self.hdfs_manager = HDFSManager(self.config)
                    st.success(" HDFS Manager initialized")
                else:
                    st.warning(" HDFSManager not available")
            except Exception as e:
                st.warning(f"  HDFS Manager initialization failed: {e}")
            
            # Initialize Kafka Processor
            try:
                if KafkaStreamProcessor is not None:
                    self.kafka_processor = KafkaStreamProcessor(self.config)
                    st.success(" Kafka Stream Processor initialized")
                else:
                    st.warning(" KafkaStreamProcessor not available")
            except Exception as e:
                st.warning(f" Kafka initialization failed: {e}")
            
            # Initialize Pipeline Orchestrator
            try:
                if PipelineOrchestrator is not None:
                    self.pipeline_orchestrator = PipelineOrchestrator(self.config)
                    st.success(" Pipeline Orchestrator initialized")
                else:
                    st.warning(" PipelineOrchestrator not available")
            except Exception as e:
                st.warning(f" Pipeline Orchestrator initialization failed: {e}")
            
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
                
                st.success(" Spark Session with Iceberg initialized successfully")
                return True
                
            except ImportError:
                st.info(" PySpark not available. Spark features disabled.")
                return False
                
        except Exception as e:
            st.warning(f" Spark/Iceberg initialization failed: {e}")
            return False

    def check_service_status(self):
        """Check status of Big Data services - simplified for reliability"""
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
            import subprocess
            result = subprocess.run(['hdfs', 'version'], 
                                  capture_output=True, text=True, timeout=3)
            if result.returncode == 0:
                return "Running"
            else:
                return "Not Installed"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return "Not Installed"
        except:
            return "Stopped"

    def check_kafka_status(self):
        """Check Kafka service status"""
        try:
            # Try to check if Kafka is actually installed
            import subprocess
            result = subprocess.run(['kafka-topics.sh', '--version'], 
                                  capture_output=True, text=True, timeout=3)
            if result.returncode == 0:
                return "Stopped"  # Installed but not running
            else:
                return "Not Installed"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return "Not Installed"
        except:
            return "Not Installed"
    
    def check_spark_status(self):
        """Check Spark service status"""
        try:
            import subprocess
            result = subprocess.run(['spark-submit', '--version'], 
                                  capture_output=True, text=True, timeout=3)
            if result.returncode == 0:
                return "Stopped"  # Installed but not running
            else:
                return "Not Installed"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return "Not Installed"
        except:
            return "Not Installed"
    
    def check_hive_status(self):
        """Check Hive service status"""
        try:
            import subprocess
            result = subprocess.run(['hive', '--version'], 
                                  capture_output=True, text=True, timeout=3)
            if result.returncode == 0:
                return "Stopped"  # Installed but not running
            else:
                return "Not Installed"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return "Not Installed"
        except:
            return "Not Installed"
            return "Unknown"
    
    def process_uploaded_data(self, uploaded_file, processing_options=None):
        """Process uploaded data through the Big Data pipeline with default options"""
        if processing_options is None:
            processing_options = {
                'kafka': False,
                'iceberg': False, 
                'hdfs': False,
                'real_time': True
            }
        
        """Process uploaded dataset through the full pipeline with robust CSV handling"""
        try:
            # Use robust CSV reader
            uploaded_file.seek(0)  # Reset file pointer
            df = robust_csv_reader(uploaded_file)
            
            if df is None:
                st.error("❌ Failed to read CSV file")
                return None
            
            # Display column information for debugging
            with st.expander("📋 Dataset Information", expanded=False):
                st.write("**Shape:**", df.shape)
                st.write("**Columns:**", list(df.columns))
                st.dataframe(df.head())
            
            # Detect the source directory dynamically
            # Try to find a directory that might contain Power BI files
            possible_powerbi_dirs = []
            
            # Check if we can find ECOMMERCE HAMMAD directory using portable paths
            base_dirs = get_base_dirs()
            
            powerbi_dir = find_data_directory()
            
            # If no specific directory found, use a generic one
            if not powerbi_dir:
                powerbi_dir = os.path.join(os.getcwd(), "powerbi_output")
                os.makedirs(powerbi_dir, exist_ok=True)
                st.info(f" Created output directory: {powerbi_dir}")
            else:
                st.info(f" Using Power BI directory: {powerbi_dir}")
            
            # Store the directory for later use
            st.session_state.powerbi_dir = powerbi_dir
            
            # Show data quality before processing
            st.subheader(" Data Quality Report - Before Processing")
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
            with st.expander(" View Raw Data Sample"):
                st.dataframe(df.head(10))
            
            # Process with Data Processor (initialize if needed)
            if not self.data_processor and DataProcessor is not None:
                try:
                    self.data_processor = DataProcessor(self.data_processing_config)
                    st.info("🔧 Data Processor initialized automatically")
                except Exception as e:
                    st.warning(f" Could not initialize Data Processor: {e}")
            
            if self.data_processor:
                st.info(" Processing data with Advanced Data Processor...")
                st.info("🧹 Cleaning operations: Removing duplicates, handling missing values, data validation, outlier removal...")
                
                with st.spinner("Processing data..."):
                    processed_df = self.data_processor.process(df)
                
                st.success(f" Data processed with advanced cleaning: {len(processed_df)} records")
                
                # Show data quality after processing
                st.subheader(" Data Quality Report - After Advanced Processing")
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
                st.warning("  Advanced Data Processor not available. Applying basic data cleaning...")
                
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
                    st.success(f"  Data saved to HDFS: {hdfs_path}")
                except Exception as e:
                    st.warning(f"  HDFS storage failed: {e}")
            
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
                    
                    st.success(f"  Data sent to Kafka topic '{topic}': {len(records)} records")
                except Exception as e:
                    st.warning(f"  Kafka streaming failed: {e}")
            
            # Process with Spark/Iceberg if enabled
            if processing_options['iceberg'] and self.spark_session:
                try:
                    spark_df = self.spark_session.createDataFrame(processed_df)
                    
                    # Create Iceberg table
                    table_name = f"ecommerce.orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    spark_df.write.mode("overwrite").saveAsTable(table_name)
                    st.success(f"  Data saved to Iceberg table: {table_name}")
                except Exception as e:
                    st.warning(f"  Iceberg storage failed: {e}")
            
            # Run full pipeline processing if orchestrator is available
            if self.pipeline_orchestrator:
                try:
                    pipeline_result = self.pipeline_orchestrator.run_pipeline(processed_df, processing_options)
                    st.success(f"  Full pipeline executed successfully")
                    processed_df = pipeline_result
                except Exception as e:
                    st.warning(f"  Pipeline orchestration failed: {e}")
            
            # AUTO-UPDATE POWER BI INTEGRATION
            st.info("  Auto-updating Power BI dashboard...")
            try:
                # Get the dynamic Power BI directory
                powerbi_dir = st.session_state.get('powerbi_dir', os.path.join(os.getcwd(), "powerbi_output"))
                
                # Save processed data to Power BI location
                powerbi_path = os.path.join(powerbi_dir, "pipeline_processed_data.csv")
                processed_df.to_csv(powerbi_path, index=False)
                st.success(f"  Data automatically saved for Power BI: {powerbi_path}")
                
                # REAL Power BI Integration - Update PBIX file directly
                st.info("  Updating Power BI PBIX file automatically...")
                try:
                    # Import Power BI automation
                    import sys
                    sys.path.append('.')
                    from powerbi_automation import update_powerbi_data_source
                    
                    # Look for PBIX file in the same directory
                    pbix_files = [f for f in os.listdir(powerbi_dir) if f.endswith('.pbix')]
                    
                    if pbix_files:
                        pbix_file = os.path.join(powerbi_dir, pbix_files[0])
                        st.info(f" Found Power BI file: {pbix_files[0]}")
                        
                        # Update the PBIX file data source
                        old_source = os.path.join(powerbi_dir, "cleaned_superstore_dataset.csv")
                        new_source = os.path.abspath(powerbi_path)
                        
                        if update_powerbi_data_source(pbix_file, old_source, new_source):
                            st.success("  Power BI PBIX file updated automatically!")
                            st.success(" **Your Power BI dashboard now shows the latest processed data!**")
                        else:
                            st.warning(" Power BI PBIX update failed - data saved but manual refresh needed")
                    else:
                        st.info("  No PBIX file found in directory - data saved for manual Power BI connection")
                        
                except Exception as pbix_error:
                    st.warning(f"  Power BI PBIX automation failed: {pbix_error}")
                    st.info(" Data saved successfully - you can manually refresh Power BI to see updates")
                
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
                
                st.success("  Power BI metadata updated automatically")
                
                # Show Power BI integration status
                st.info(f"  **Power BI Integration Active**: Data saved to {powerbi_dir}")
                
                # Show instructions for viewing in Power BI
                with st.expander("  How to Use the Processed Data with Power BI"):
                    st.markdown(f"""
                    **Your processed data is ready for Power BI:**
                    
                     **Data Location**: `{powerbi_path}`
                    
                    **Option 1 - Automatic (if PBIX file exists):**
                    1. � Open your existing PBIX file from `{powerbi_dir}`
                    2.   Click "Refresh" in Power BI to load the new processed data
                    
                    **Option 2 - Manual Setup:**
                    1. 📂 Open Power BI Desktop
                    2. � Import data from: `{powerbi_path}`
                    3.   Create your visualizations with the cleaned data
                    
                    **✨ Data Quality Improvements:**
                    - Duplicates removed:  
                    - Missing values handled:    
                    - Data validation completed:  
                    - Outliers processed:  
                    
                    **  Next Steps:**
                    1. Open your PBIX file in Power BI Desktop
                    2. Click Refresh to load the new processed data
                    3. All charts and visuals now show cleaned data
                    4. No manual steps needed - everything is automated!
                    
                    **If you don't see updates:**
                    - Click Refresh button in Power BI Desktop
                    - Or go to Home > Refresh
                    """)
                
            except Exception as e:
                st.warning(f"  Power BI auto-update failed: {e}")
                st.info("  Data processed successfully - you can manually connect Power BI to the processed data")
            
            return processed_df
            
        except Exception as e:
            st.error(f"Pipeline processing failed: {e}")
            return None
    
    def get_real_time_insights(self, df):
        """Generate real-time insights from processed data with flexible column detection"""
        insights = {}
        
        # Detect columns flexibly
        col_mapping = detect_column_mappings(df)
        
        try:
            # Basic metrics using flexible column detection
            sales_col = col_mapping.get('sales')
            profit_col = col_mapping.get('profit')
            date_col = col_mapping.get('date')
            category_col = col_mapping.get('category')
            region_col = col_mapping.get('region')
            
            insights['total_sales'] = pd.to_numeric(df[sales_col], errors='coerce').sum() if sales_col else 0
            insights['total_profit'] = pd.to_numeric(df[profit_col], errors='coerce').sum() if profit_col else 0
            insights['total_orders'] = len(df)
            insights['avg_order_value'] = insights['total_sales'] / insights['total_orders'] if insights['total_orders'] > 0 else 0
            
            # Profit margin
            insights['profit_margin'] = (insights['total_profit'] / insights['total_sales'] * 100) if insights['total_sales'] > 0 else 0
            
            # Time-based insights
            if date_col:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                
                # Safe date range calculation
                min_date = df[date_col].min()
                max_date = df[date_col].max()
                
                if pd.notna(min_date) and pd.notna(max_date):
                    insights['date_range'] = f"{min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}"
                else:
                    insights['date_range'] = "No valid dates available"
                
                # Monthly trends
                if sales_col:
                    monthly_sales = df.groupby(df[date_col].dt.to_period('M'))[sales_col].sum()
                    insights['monthly_growth'] = ((monthly_sales.iloc[-1] - monthly_sales.iloc[0]) / monthly_sales.iloc[0] * 100) if len(monthly_sales) > 1 else 0
            
            # Category insights
            if category_col and sales_col:
                category_sales = df.groupby(category_col)[sales_col].sum().sort_values(ascending=False)
                insights['top_category'] = category_sales.index[0] if len(category_sales) > 0 else 'N/A'
                insights['category_contribution'] = (category_sales.iloc[0] / insights['total_sales'] * 100) if insights['total_sales'] > 0 else 0
            
            # Geographic insights
            if region_col and sales_col:
                region_sales = df.groupby(region_col)[sales_col].sum().sort_values(ascending=False)
                insights['top_state'] = region_sales.index[0] if len(region_sales) > 0 else 'N/A'
                insights['state_contribution'] = (region_sales.iloc[0] / insights['total_sales'] * 100) if insights['total_sales'] > 0 else 0
            
        except Exception as e:
            st.error(f"Insights generation failed: {e}")
        
        return insights

def apply_custom_css():
    """Apply beautiful custom CSS styling to make the dashboard gorgeous with better visibility"""
    st.markdown("""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Global Styles with better contrast */
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        min-height: 100vh;
    }
    
    /* Main container with improved visibility */
    .main > div {
        background: rgba(255, 255, 255, 0.95) !important;
        backdrop-filter: blur(10px);
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.3);
        padding: 2rem;
        margin: 1rem;
        box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.37);
    }
    
    /* Ensure text is visible */
    .main * {
        color: #1a1a1a !important;
    }
    
    /* Custom font family */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Header styling with better visibility */
    .main-header {
        text-align: center;
        background: linear-gradient(45deg, #FF6B6B, #4ECDC4, #45B7D1, #96CEB4);
        background-size: 300% 300%;
        animation: gradientShift 6s ease infinite;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-size: 3.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        text-shadow: 0 4px 8px rgba(0,0,0,0.3);
    }
    
    @keyframes gradientShift {
        0% { background-position: 0% 50%; }
        50% { background-position: 100% 50%; }
        100% { background-position: 0% 50%; }
    }
    
    /* Subtitle with better contrast */
    .subtitle {
        text-align: center;
        color: #444444 !important;
        font-size: 1.2rem;
        font-weight: 400;
        margin-bottom: 2rem;
    }
    
    /* Tab styling with better visibility */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        background: rgba(255, 255, 255, 0.9);
        padding: 1rem;
        border-radius: 15px;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(0, 0, 0, 0.1);
    }
    
    .stTabs [data-baseweb="tab"] {
        background: rgba(255, 255, 255, 0.8);
        border-radius: 10px;
        padding: 12px 24px;
        border: 1px solid rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
        color: #333333 !important;
        font-weight: 500;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(45deg, #FF6B6B, #4ECDC4) !important;
        color: white !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        transform: translateY(-2px);
    }
    
    .stTabs [aria-selected="true"] * {
        color: white !important;
    }
    
    /* Tab content area */
    .stTabs [data-baseweb="tab-panel"] {
        background: rgba(255, 255, 255, 0.95);
        border-radius: 15px;
        padding: 2rem;
        margin-top: 1rem;
        border: 1px solid rgba(0, 0, 0, 0.1);
    }
    
    /* Headers in content */
    h1, h2, h3, h4, h5, h6 {
        color: #2c3e50 !important;
    }
    
    /* Regular text */
    p, span, div {
        color: #444444 !important;
    }
    
    /* Metric cards with better contrast */
    [data-testid="metric-container"] {
        background: linear-gradient(145deg, rgba(255, 255, 255, 0.9), rgba(255, 255, 255, 0.8)) !important;
        border: 1px solid rgba(0, 0, 0, 0.1);
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        backdrop-filter: blur(10px);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    
    [data-testid="metric-container"]:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 40px rgba(0, 0, 0, 0.15);
    }
    
    [data-testid="metric-container"] * {
        color: #2c3e50 !important;
    }
    
    /* Button styling with better visibility */
    .stButton > button {
        background: linear-gradient(45deg, #FF6B6B, #4ECDC4) !important;
        color: white !important;
        border: none;
        border-radius: 25px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.3);
        background: linear-gradient(45deg, #FF5252, #26C6DA) !important;
    }
    
    .stButton > button * {
        color: white !important;
    }
    
    /* File uploader with better visibility */
    .stFileUploader {
        background: rgba(255, 255, 255, 0.9) !important;
        border: 2px dashed #4ECDC4;
        border-radius: 15px;
        padding: 2rem;
        text-align: center;
        transition: all 0.3s ease;
    }
    
    .stFileUploader:hover {
        border-color: #FF6B6B;
        background: rgba(255, 255, 255, 0.95) !important;
    }
    
    .stFileUploader * {
        color: #444444 !important;
    }
    
    /* Dataframe styling */
    .stDataFrame {
        background: rgba(255, 255, 255, 0.95) !important;
        border-radius: 15px;
        overflow: hidden;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        border: 1px solid rgba(0, 0, 0, 0.1);
    }
    
    /* Alert messages with better contrast */
    .stAlert {
        border-radius: 15px;
        border: none;
        backdrop-filter: blur(10px);
    }
    
    .stSuccess {
        background: rgba(76, 175, 80, 0.1) !important;
        border: 1px solid rgba(76, 175, 80, 0.3) !important;
        color: #2e7d32 !important;
    }
    
    .stSuccess * {
        color: #2e7d32 !important;
    }
    
    .stError {
        background: rgba(244, 67, 54, 0.1) !important;
        border: 1px solid rgba(244, 67, 54, 0.3) !important;
        color: #c62828 !important;
    }
    
    .stError * {
        color: #c62828 !important;
    }
    
    .stInfo {
        background: rgba(33, 150, 243, 0.1) !important;
        border: 1px solid rgba(33, 150, 243, 0.3) !important;
        color: #1565c0 !important;
    }
    
    .stInfo * {
        color: #1565c0 !important;
    }
    
    .stWarning {
        background: rgba(255, 152, 0, 0.1) !important;
        border: 1px solid rgba(255, 152, 0, 0.3) !important;
        color: #ef6c00 !important;
    }
    
    .stWarning * {
        color: #ef6c00 !important;
    }
    
    /* Custom styled sections */
    .custom-header {
        color: #2c3e50 !important;
        text-align: center;
        margin-bottom: 2rem;
        font-weight: 600;
    }
    
    .custom-subtext {
        color: #666666 !important;
        text-align: center;
        margin-bottom: 2rem;
        font-size: 1.1rem;
    }
    
    /* Charts background */
    .stPlotlyChart {
        background: rgba(255, 255, 255, 0.95) !important;
        border-radius: 15px;
        padding: 1rem;
        backdrop-filter: blur(10px);
        border: 1px solid rgba(0, 0, 0, 0.1);
    }
    
    /* Custom animations */
    @keyframes fadeInUp {
        from {
            opacity: 0;
            transform: translateY(30px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    .animate-fade-in {
        animation: fadeInUp 0.6s ease-out;
    }
    
    /* Input fields */
    .stTextInput > div > div > input,
    .stSelectbox > div > div > div,
    .stTextArea > div > div > textarea {
        background: rgba(255, 255, 255, 0.9) !important;
        color: #333333 !important;
        border: 1px solid rgba(0, 0, 0, 0.2) !important;
        border-radius: 10px;
    }
    
    /* Scrollbar styling */
    ::-webkit-scrollbar {
        width: 8px;
    }
    
    ::-webkit-scrollbar-track {
        background: rgba(255, 255, 255, 0.3);
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(45deg, #FF6B6B, #4ECDC4);
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(45deg, #FF5252, #26C6DA);
    }
    </style>
    """, unsafe_allow_html=True)

def main():
    # Apply custom CSS first
    apply_custom_css()
    
    st.set_page_config(
        page_title="E-commerce Big Data Pipeline Dashboard",
        page_icon="🚀",
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Beautiful header with gradient text
    st.markdown("""
    <div class="animate-fade-in">
        <h1 class="main-header">🚀 E-commerce Analytics Hub</h1>
        <p class="subtitle">Advanced Big Data Pipeline Dashboard with Real-time Insights & AI-Powered Analytics</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Add some spacing
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Initialize dashboard
    dashboard = IntegratedPipelineDashboard()
    
    # Main content with beautiful tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📤 Data Upload & Processing", 
        "📊 Real-time Insights", 
        "⚡ Power BI Integration", 
        "🔍 Service Monitoring", 
        "�️ Data Explorer"
    ])
    
    with tab1:
        st.markdown("""
        <div class="animate-fade-in">
            <h2 class="custom-header">📤 Upload Dataset & Run Pipeline</h2>
            <p class="custom-subtext">Upload your e-commerce dataset to run through the complete Big Data pipeline</p>
        </div>
        """, unsafe_allow_html=True)
            
        # Create columns for better layout
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            uploaded_file = st.file_uploader(
                "Choose a CSV file",
                type="csv",
                help="Supported files: superstore_dataset.csv, cleaned_superstore_dataset.csv, navigator_ft-data_preview.csv, MOCK_DATA.csv"
            )
        
        if uploaded_file is not None:
            # Success message with animation
            st.markdown("""
            <div class="animate-fade-in" style="margin: 1rem 0;">
                <div style="background: rgba(76, 175, 80, 0.1); 
                           border: 1px solid rgba(76, 175, 80, 0.5); border-radius: 15px; padding: 1rem; text-align: center;">
                    <h4 style="color: #2e7d32; margin: 0;">✅ File Successfully Uploaded</h4>
                    <p style="color: #444444; margin: 0.5rem 0 0 0;">""" + uploaded_file.name + """</p>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            # Preview data using robust reader
            df_preview = robust_csv_reader(uploaded_file)
            if df_preview is not None:
                st.markdown("""
                <div class="animate-fade-in">
                    <h3 class="custom-header">👁️ Data Preview</h3>
                </div>
                """, unsafe_allow_html=True)
                
                # Enhanced dataframe display
                st.dataframe(
                    df_preview.head(), 
                    use_container_width=True,
                    height=300
                )
            
            # Beautiful metrics cards
            st.markdown("""
            <div class="animate-fade-in">
                <h3 class="custom-header">📈 Dataset Overview</h3>
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📊 Total Records", f"{len(df_preview):,}", delta="Live Data")
            with col2:
                st.metric("🏷️ Data Columns", f"{len(df_preview.columns)}", delta="Structured")
            with col3:
                # Use flexible column detection for sales
                preview_col_mapping = detect_column_mappings(df_preview)
                if preview_col_mapping.get('sales'):
                    sales_col = preview_col_mapping['sales']
                    try:
                        sales_sum = pd.to_numeric(df_preview[sales_col], errors='coerce').sum()
                        st.metric("💰 Total Sales", f"${sales_sum:,.2f}", delta="Revenue")
                    except (ValueError, TypeError):
                        st.metric("💰 Total Sales", "N/A", delta="Invalid Data")
                else:
                    st.metric("💰 Total Sales", "No sales column detected", delta="Check Data")
            
            # Beautiful processing button
            st.markdown("<br><br>", unsafe_allow_html=True)
            
            # Center the button
            col1, col2, col3 = st.columns([1, 1, 1])
            with col2:
                if st.button("🚀 Run Complete Pipeline", type="primary", use_container_width=True):
                    with st.spinner("🔄 Processing through Big Data pipeline..."):
                        # Reset file pointer
                        uploaded_file.seek(0)
                        # Use default processing options since sidebar was removed
                        default_processing_options = {
                            'kafka': False,  # Disable by default since services aren't running
                            'iceberg': False,
                            'hdfs': False,
                            'real_time': True  # Keep real-time processing for dashboard
                        }
                        processed_data = dashboard.process_uploaded_data(uploaded_file, default_processing_options)
                        
                        if processed_data is not None:
                            st.balloons()  # Fun animation
                            st.success("🎉 Pipeline processing completed successfully!")
                            st.session_state['processed_data'] = processed_data
                            st.session_state['insights'] = dashboard.get_real_time_insights(processed_data)
                        else:
                            st.error("❌ Pipeline processing failed")
    
    with tab2:
        st.markdown("""
        <div class="animate-fade-in">
            <h2 class="custom-header">📊 Real-time Analytics & Insights</h2>
            <p class="custom-subtext">ℹ️ All insights below are based on cleaned and processed data, not raw uploaded data</p>
        </div>
        """, unsafe_allow_html=True)
        
        if 'processed_data' in st.session_state and 'insights' in st.session_state:
            df = st.session_state['processed_data']
            insights = st.session_state['insights']
            
            # Check dataset compatibility and detect column mappings
            col_mapping = check_dataset_compatibility(df)
            
            # Enhanced Key Metrics Dashboard with beautiful cards
            st.markdown("""
            <div class="animate-fade-in">
                <h3 class="custom-header">🎯 Key Performance Indicators</h3>
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric(
                    "💰 Total Sales",
                    f"${insights['total_sales']:,.0f}",
                    delta=f"{insights['profit_margin']:.1f}% margin"
                )
            with col2:
                st.metric(
                    "  Total Profit",
                    f"${insights['total_profit']:,.0f}",
                    delta=f"{insights['monthly_growth']:.1f}% growth" if 'monthly_growth' in insights else None
                )
            with col3:
                st.metric(
                    "  Total Orders",
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
            st.markdown("###   Business Intelligence Dashboard")
            
            # Row 1: Sales Analysis
            col1, col2 = st.columns(2)
            
            with col1:
                # Flexible category sales chart
                def create_category_sales_chart():
                    if col_mapping.get('category') and col_mapping.get('sales'):
                        st.markdown("####   Sales Performance by Category")
                        category_col = col_mapping['category']
                        sales_col = col_mapping['sales']
                        
                        category_sales = df.groupby(category_col)[sales_col].sum().sort_values(ascending=False)
                        
                        # Create DataFrame for plotly express
                        chart_data = pd.DataFrame({
                            'category': category_sales.index,
                            'sales': category_sales.values
                        })
                        
                        fig = px.bar(
                            chart_data,
                            x='sales',
                            y='category',
                            orientation='h',
                            title="Sales by Category",
                            color='sales',
                            color_continuous_scale='Viridis',
                            text='sales'
                        )
                        fig.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
                        fig.update_layout(height=400, yaxis={'categoryorder': 'total ascending'})
                        st.plotly_chart(fig, use_container_width=True, key="main_category_sales")
                    else:
                        st.warning("Category or Sales column not found in dataset")
                
                safe_chart(create_category_sales_chart)
            
            with col2:
                # Flexible geographic sales chart
                def create_geographic_sales_chart():
                    region_col = col_mapping.get('region')
                    sales_col = col_mapping.get('sales')
                    
                    if region_col and sales_col:
                        st.markdown("#### 🗺️ Geographic Sales Distribution")
                        region_sales = df.groupby(region_col)[sales_col].sum().sort_values(ascending=False).head(10)
                        
                        # Create DataFrame for plotly express
                        chart_data = pd.DataFrame({
                            'region': region_sales.index,
                            'sales': region_sales.values
                        })
                        
                        fig = px.pie(
                            chart_data,
                            values='sales',
                            names='region',
                            title=f"Top 10 {region_col.title()} by Sales",
                            hole=0.4,
                            color_discrete_sequence=px.colors.qualitative.Set3
                        )
                        fig.update_traces(textposition='inside', textinfo='percent+label')
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True, key="main_region_pie")
                    else:
                        st.warning("Region or Sales column not found in dataset")
                
                safe_chart(create_geographic_sales_chart)
            
            # Row 2: Profitability Analysis
            def create_profitability_analysis():
                profit_col = col_mapping.get('profit')
                sales_col = col_mapping.get('sales')
                category_col = col_mapping.get('category')
                
                if profit_col and sales_col:
                    st.markdown("#### 💹 Profitability Analysis")
                    
                    # Create profit margin analysis
                    if category_col:
                        profit_analysis = df.groupby(category_col).agg({
                            sales_col: 'sum',
                            profit_col: 'sum'
                        }).reset_index()
                        profit_analysis['profit_margin'] = (profit_analysis[profit_col] / profit_analysis[sales_col]) * 100
                        
                        # Transform profit margin for bubble sizing (ensure positive values)
                        min_margin = profit_analysis['profit_margin'].min()
                        margin_offset = abs(min_margin) + 1 if min_margin < 0 else 0
                        profit_analysis['margin_size'] = profit_analysis['profit_margin'] + margin_offset
                        
                        fig = px.scatter(profit_analysis, x=sales_col, y=profit_col, 
                                       size='margin_size', color=category_col,
                                       hover_name=category_col,
                                       title="Sales vs Profit by Category (Bubble size = Profit Margin)",
                                       size_max=60)
                        fig.update_layout(height=400, xaxis_title="Sales ($)", yaxis_title="Profit ($)")
                        st.plotly_chart(fig, use_container_width=True, key="main_profit_margin")
                else:
                    st.warning("Profit or Sales columns not found in dataset")
            
            safe_chart(create_profitability_analysis)
            
            # Row 3: Time Series Analysis
            if 'order_date' in df.columns:
                st.markdown("####   Time Series Analysis")
                
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
                    st.plotly_chart(fig, use_container_width=True, key="main_monthly_sales")
                
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
                    st.plotly_chart(fig, use_container_width=True, key="main_quarterly_trend")
            
            # Row 4: Advanced Analytics
            st.markdown("#### 🔍 Advanced Business Insights")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Flexible customer segment analysis
                def create_segment_analysis():
                    category_col = col_mapping.get('category')
                    sales_col = col_mapping.get('sales')
                    profit_col = col_mapping.get('profit')
                    quantity_col = col_mapping.get('quantity')
                    
                    if category_col and sales_col:
                        # Use category as segment if no specific segment column exists
                        segment_data = df.groupby(category_col).agg({
                            sales_col: 'sum',
                            **({profit_col: 'sum'} if profit_col else {}),
                            **({quantity_col: 'sum'} if quantity_col else {})
                        }).reset_index()
                        
                        # Only create chart if we have data
                        if len(segment_data) > 0 and segment_data[sales_col].sum() > 0:
                            fig = px.sunburst(segment_data, 
                                            path=[category_col], 
                                            values=sales_col,
                                            title=f"Sales Distribution by {category_col.title()}",
                                            color=profit_col if profit_col else sales_col,
                                            color_continuous_scale='RdYlBu')
                            fig.update_layout(height=400)
                            st.plotly_chart(fig, use_container_width=True, key="main_product_performance")
                        else:
                            st.warning("No segment data available for analysis")
                    else:
                        st.warning("Category or Sales columns not found for segment analysis")
                
                safe_chart(create_segment_analysis)
            
            with col2:
                # Flexible subcategory/product analysis
                def create_subcategory_analysis():
                    product_col = col_mapping.get('product')
                    profit_col = col_mapping.get('profit')
                    sales_col = col_mapping.get('sales')
                    
                    # Use product if available, otherwise use category
                    analysis_col = product_col or col_mapping.get('category')
                    value_col = profit_col or sales_col
                    
                    if analysis_col and value_col:
                        subcat_data = df.groupby(analysis_col)[value_col].sum().sort_values(ascending=False).head(15)
                        
                        if len(subcat_data) > 0:
                            # Create DataFrame for plotly express
                            chart_data = pd.DataFrame({
                                'category': subcat_data.index,
                                'value': subcat_data.values
                            })
                            
                            fig = px.bar(
                                chart_data,
                                x='category',
                                y='value',
                                title=f"Top 15 {analysis_col.title()} by {value_col.title()}",
                                color='value',
                                color_continuous_scale='plasma'
                            )
                            fig.update_layout(height=400, 
                                            xaxis_title=analysis_col.title(), 
                                            yaxis_title=f"{value_col.title()} ($)", 
                                            xaxis={'tickangle': 45})
                            st.plotly_chart(fig, use_container_width=True, key="main_customer_analysis")
                        else:
                            st.warning("No data available for analysis")
                    else:
                        st.warning("Required columns not found for product/category analysis")
                
                safe_chart(create_subcategory_analysis)
            
            # Performance Summary
            st.markdown("####   Performance Summary")
            summary_col1, summary_col2, summary_col3, summary_col4 = st.columns(4)
            
            with summary_col1:
                if 'customer' in df.columns:
                    unique_customers = df['customer'].nunique()
                    st.metric("👥 Unique Customers", f"{unique_customers:,}")
            
            with summary_col2:
                if 'quantity' in df.columns:
                    total_quantity = df['quantity'].sum()
                    st.metric(" Items Sold", f"{total_quantity:,}")
            
            with summary_col3:
                if 'discount' in df.columns:
                    avg_discount = df['discount'].mean() * 100
                    st.metric("  Avg Discount", f"{avg_discount:.1f}%")
            
            with summary_col4:
                if 'sales' in df.columns and 'quantity' in df.columns:
                    total_quantity = df['quantity'].sum()
                    total_sales = df['sales'].sum()
                    avg_unit_price = total_sales / total_quantity if total_quantity > 0 else 0
                    st.metric(" Avg Unit Price", f"${avg_unit_price:.2f}")

            # ================================
            #   ENHANCED POWERBI-STYLE ANALYTICS
            # ================================
            
            st.markdown("---")
            st.markdown("##   Advanced PowerBI-Style Analytics")
            
            # Row 5: Customer Intelligence Dashboard
            st.markdown("#### 👥 Customer Intelligence Dashboard")
            
            customer_col1, customer_col2, customer_col3 = st.columns(3)
            
            with customer_col1:
                if 'customer' in df.columns and 'sales' in df.columns:
                    # Customer ranking and CLV
                    customer_metrics = df.groupby('customer').agg({
                        'sales': ['sum', 'count', 'mean'],
                        'profit': 'sum',
                        'quantity': 'sum'
                    }).round(2)
                    
                    customer_metrics.columns = ['total_sales', 'order_count', 'avg_order_value', 'total_profit', 'total_quantity']
                    customer_metrics = customer_metrics.reset_index().sort_values('total_sales', ascending=False).head(20)
                    
                    # Transform profit values for marker sizing (ensure positive values)
                    if len(customer_metrics) > 0 and not customer_metrics['total_profit'].isna().all():
                        min_profit = customer_metrics['total_profit'].min()
                        profit_offset = abs(min_profit) + 1 if min_profit < 0 else 0
                        marker_sizes = customer_metrics['total_profit'] + profit_offset
                        
                        # Ensure marker_sizes has valid values
                        marker_sizes = marker_sizes.fillna(1)  # Replace NaN with 1
                        max_marker_size = max(marker_sizes) if len(marker_sizes) > 0 and max(marker_sizes) > 0 else 1
                    else:
                        # Fallback to uniform marker sizes
                        marker_sizes = [10] * len(customer_metrics) if len(customer_metrics) > 0 else [10]
                        max_marker_size = 10
                    
                    # Only create chart if we have data
                    if len(customer_metrics) > 0:
                        # Customer value distribution
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=customer_metrics['order_count'],
                            y=customer_metrics['total_sales'],
                            mode='markers',
                            marker=dict(
                                size=marker_sizes,
                                sizemode='area',
                                sizeref=2.*max_marker_size/(40.**2),
                                sizemin=4,
                                color=customer_metrics['avg_order_value'],
                                colorscale='Viridis',
                                showscale=True,
                                colorbar=dict(title="Avg Order Value")
                            ),
                            text=customer_metrics['customer'],
                            hovertemplate='<b>%{text}</b><br>Orders: %{x}<br>Sales: $%{y:,.0f}<br>Profit: $%{customdata:,.0f}<extra></extra>',
                            customdata=customer_metrics['total_profit']
                        ))
                        
                        fig.update_layout(
                            title="Customer Value Matrix<br><sub>Size = Profit, Color = AOV</sub>",
                            xaxis_title="Number of Orders",
                            yaxis_title="Total Sales ($)",
                            height=400
                        )
                        st.plotly_chart(fig, use_container_width=True, key="main_discount_impact")
                    else:
                        st.warning("No customer data available for value matrix analysis")
            
            with customer_col2:
                if 'customer' in df.columns and 'sales' in df.columns:
                    # Customer segmentation pie chart
                    customer_totals = df.groupby('customer')['sales'].sum()
                    
                    # Create customer segments
                    high_value = customer_totals[customer_totals > customer_totals.quantile(0.8)]
                    medium_value = customer_totals[(customer_totals > customer_totals.quantile(0.5)) & (customer_totals <= customer_totals.quantile(0.8))]
                    low_value = customer_totals[customer_totals <= customer_totals.quantile(0.5)]
                    
                    segment_data = pd.DataFrame({
                        'Segment': ['High Value', 'Medium Value', 'Low Value'],
                        'Count': [len(high_value), len(medium_value), len(low_value)],
                        'Revenue': [high_value.sum(), medium_value.sum(), low_value.sum()]
                    })
                    
                    fig = go.Figure(data=[go.Pie(
                        labels=segment_data['Segment'],
                        values=segment_data['Revenue'],
                        hole=.3,
                        marker_colors=['#ff6b6b', '#4ecdc4', '#45b7d1'],
                        textinfo='label+percent+value',
                        texttemplate='%{label}<br>%{percent}<br>$%{value:,.0f}'
                    )])
                    fig.update_layout(
                        title="Customer Segments by Revenue",
                        height=400,
                        showlegend=True
                    )
                    st.plotly_chart(fig, use_container_width=True, key="main_shipping_analysis")
            
            with customer_col3:
                if 'order_date' in df.columns and 'customer' in df.columns:
                    # Customer acquisition trend
                    df['order_date'] = pd.to_datetime(df['order_date'])
                    first_order = df.groupby('customer')['order_date'].min().reset_index()
                    first_order['month'] = first_order['order_date'].dt.to_period('M')
                    monthly_new_customers = first_order.groupby('month').size().reset_index(name='new_customers')
                    monthly_new_customers['month'] = monthly_new_customers['month'].astype(str)
                    
                    # Calculate cumulative customers
                    monthly_new_customers['cumulative_customers'] = monthly_new_customers['new_customers'].cumsum()
                    
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=monthly_new_customers['month'],
                        y=monthly_new_customers['new_customers'],
                        name='New Customers',
                        marker_color='lightblue',
                        yaxis='y'
                    ))
                    fig.add_trace(go.Scatter(
                        x=monthly_new_customers['month'],
                        y=monthly_new_customers['cumulative_customers'],
                        mode='lines+markers',
                        name='Cumulative',
                        line=dict(color='red', width=3),
                        yaxis='y2'
                    ))
                    
                    fig.update_layout(
                        title="Customer Acquisition Trend",
                        xaxis_title="Month",
                        yaxis=dict(title="New Customers", side="left"),
                        yaxis2=dict(title="Cumulative", side="right", overlaying="y"),
                        height=400,
                        legend=dict(x=0, y=1)
                    )
                    st.plotly_chart(fig, use_container_width=True, key="main_state_performance")

            # Row 6: Product Performance Matrix
            st.markdown("####  Product Performance Matrix")
            
            product_col1, product_col2 = st.columns(2)
            
            with product_col1:
                if 'category' in df.columns and 'subcategory' in df.columns:
                    # Product hierarchy treemap
                    product_hierarchy = df.groupby(['category', 'subcategory']).agg({
                        'sales': 'sum',
                        'profit': 'sum',
                        'quantity': 'sum'
                    }).reset_index()
                    
                    # Check if we have data to display
                    if len(product_hierarchy) > 0 and product_hierarchy['sales'].sum() > 0:
                        fig = px.treemap(
                            product_hierarchy,
                            path=[px.Constant("All Products"), 'category', 'subcategory'],
                            values='sales',
                            color='profit',
                            color_continuous_scale='RdYlBu',
                            title="Product Hierarchy Performance<br><sub>Size = Sales, Color = Profit</sub>"
                        )
                        fig.update_traces(textinfo="label+value+percent parent")
                        fig.update_layout(height=500)
                        st.plotly_chart(fig, use_container_width=True, key="main_subcategory_profit")
                    else:
                        st.warning("⚠️ No product hierarchy data available for visualization.")
                        st.info("💡 Please ensure your dataset contains category and subcategory columns with valid sales data.")
            
            with product_col2:
                if 'sales' in df.columns and 'profit' in df.columns and 'quantity' in df.columns:
                    # Check if product_name column exists
                    if 'product_name' in df.columns:
                        # ABC Analysis (Pareto Chart)
                        product_sales = df.groupby('product_name')['sales'].sum().sort_values(ascending=False).reset_index()
                        
                        # Check if we have data to display
                        if len(product_sales) > 0 and product_sales['sales'].sum() > 0:
                            product_sales['cumulative_sales'] = product_sales['sales'].cumsum()
                            product_sales['cumulative_percent'] = (product_sales['cumulative_sales'] / product_sales['sales'].sum()) * 100
                            product_sales['rank'] = range(1, len(product_sales) + 1)
                            
                            # Take top 50 products for visibility
                            top_products = product_sales.head(50)
                            
                            fig = go.Figure()
                            fig.add_trace(go.Bar(
                                x=top_products['rank'],
                                y=top_products['sales'],
                                name='Sales',
                                marker_color='lightblue',
                                yaxis='y'
                            ))
                            fig.add_trace(go.Scatter(
                                x=top_products['rank'],
                                y=top_products['cumulative_percent'],
                                mode='lines+markers',
                                name='Cumulative %',
                                line=dict(color='red', width=3),
                                yaxis='y2'
                            ))
                            
                            # Add 80% line
                            fig.add_hline(y=80, line_dash="dash", line_color="green", 
                                        annotation_text="80% Line", yref='y2')
                            
                            fig.update_layout(
                                title="ABC Analysis - Top 50 Products<br><sub>Pareto Chart (80/20 Rule)</sub>",
                                xaxis_title="Product Rank",
                                yaxis=dict(title="Sales ($)", side="left"),
                                yaxis2=dict(title="Cumulative %", side="right", overlaying="y", range=[0, 100]),
                                height=500,
                                legend=dict(x=0.7, y=1)
                            )
                            st.plotly_chart(fig, use_container_width=True, key="main_segment_comparison")
                        else:
                            st.warning("⚠️ No product sales data available for ABC analysis.")
                            st.info("💡 Please ensure your dataset contains product names with valid sales data.")
                    else:
                        st.warning("⚠️ Product name column not found.")
                        st.info("💡 Please ensure your dataset contains a 'product_name' column for ABC analysis.")

            # Row 7: Geographic Intelligence
            st.markdown("####  Geographic Intelligence")
            
            geo_col1, geo_col2 = st.columns(2)
            
            with geo_col1:
                if 'region' in df.columns and 'state' in df.columns:
                    # Regional performance heatmap
                    regional_metrics = df.groupby(['region', 'state']).agg({
                        'sales': 'sum',
                        'profit': 'sum',
                        'customer': 'nunique'
                    }).reset_index()
                    
                    # Check if we have data to display
                    if len(regional_metrics) > 0 and regional_metrics['sales'].sum() > 0:
                        fig = px.scatter(
                            regional_metrics,
                            x='sales',
                            y='profit',
                            size='customer',
                            color='region',
                            hover_data=['state'],
                            title="Regional Performance Matrix<br><sub>Size = Unique Customers</sub>",
                            size_max=60
                        )
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True, key="main_order_priority")
                    else:
                        st.warning("⚠️ No regional performance data available for visualization.")
                        st.info("💡 Please ensure your dataset contains region and state columns with valid sales data.")
                else:
                    st.warning("⚠️ Region and state columns not found.")
                    st.info("💡 Please ensure your dataset contains 'region' and 'state' columns for regional analysis.")
            
            with geo_col2:
                if 'city' in df.columns and 'sales' in df.columns:
                    # Top cities performance
                    city_performance = df.groupby('city').agg({
                        'sales': 'sum',
                        'profit': 'sum',
                        'customer': 'nunique'
                    }).reset_index().sort_values('sales', ascending=False).head(15)
                    
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=city_performance['city'],
                        y=city_performance['sales'],
                        name='Sales',
                        marker_color='lightblue',
                        text=city_performance['sales'],
                        texttemplate='$%{text:,.0f}',
                        textposition='outside'
                    ))
                    
                    fig.update_layout(
                        title="Top 15 Cities by Sales Performance",
                        xaxis_title="City",
                        yaxis_title="Sales ($)",
                        height=400,
                        xaxis={'tickangle': 45}
                    )
                    st.plotly_chart(fig, use_container_width=True, key="main_correlation_matrix")

            # Row 8: Financial Analytics Dashboard
            st.markdown("####   Financial Analytics Dashboard")
            
            fin_col1, fin_col2, fin_col3 = st.columns(3)
            
            with fin_col1:
                if 'sales' in df.columns and 'profit' in df.columns:
                    # Profit margin distribution
                    df['profit_margin'] = (df['profit'] / df['sales']) * 100
                    df['profit_margin'] = df['profit_margin'].replace([np.inf, -np.inf], 0).fillna(0)
                    
                    fig = go.Figure(data=[go.Histogram(
                        x=df['profit_margin'],
                        nbinsx=30,
                        marker_color='lightblue',
                        opacity=0.7
                    )])
                    fig.add_vline(x=df['profit_margin'].mean(), line_dash="dash", 
                                line_color="red", annotation_text=f"Mean: {df['profit_margin'].mean():.1f}%")
                    fig.update_layout(
                        title="Profit Margin Distribution",
                        xaxis_title="Profit Margin (%)",
                        yaxis_title="Frequency",
                        height=350
                    )
                    st.plotly_chart(fig, use_container_width=True, key="main_seasonal_trends")
            
            with fin_col2:
                if 'discount' in df.columns and 'profit' in df.columns:
                    # Check if discount column has valid data for cutting
                    discount_data = df['discount'].dropna()
                    if len(discount_data) > 0 and discount_data.max() > discount_data.min():
                        # Discount vs Profit analysis
                        try:
                            discount_profit = df.groupby(pd.cut(df['discount'], bins=10)).agg({
                                'profit': 'mean',
                                'sales': 'count'
                            }).reset_index()
                            discount_profit['discount_range'] = discount_profit['discount'].astype(str)
                            
                            fig = go.Figure()
                            fig.add_trace(go.Bar(
                                x=discount_profit['discount_range'],
                                y=discount_profit['profit'],
                                marker_color='coral',
                                name='Avg Profit'
                            ))
                            
                            fig.update_layout(
                                title="Discount Impact on Profit",
                                xaxis_title="Discount Range",
                                yaxis_title="Average Profit ($)",
                                height=350,
                                xaxis={'tickangle': 45}
                            )
                            st.plotly_chart(fig, use_container_width=True, key="main_city_performance")
                        except ValueError as e:
                            st.warning("⚠️ Unable to create discount analysis chart.")
                            st.info("💡 Discount data may be insufficient for binning analysis.")
                    else:
                        st.warning("⚠️ No valid discount data available for analysis.")
                        st.info("💡 Please ensure your dataset contains numeric discount values with variation.")
            
            with fin_col3:
                if 'order_date' in df.columns and 'sales' in df.columns:
                    # Revenue growth rate
                    df['order_date'] = pd.to_datetime(df['order_date'])
                    monthly_revenue = df.groupby(df['order_date'].dt.to_period('M'))['sales'].sum()
                    growth_rate = monthly_revenue.pct_change() * 100
                    
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=[str(x) for x in growth_rate.index],
                        y=growth_rate.values,
                        mode='lines+markers',
                        line=dict(color='green', width=3),
                        marker=dict(size=8),
                        name='Growth Rate'
                    ))
                    fig.add_hline(y=0, line_dash="dash", line_color="red")
                    
                    fig.update_layout(
                        title="Monthly Revenue Growth Rate",
                        xaxis_title="Month",
                        yaxis_title="Growth Rate (%)",
                        height=350
                    )
                    st.plotly_chart(fig, use_container_width=True, key="main_product_categories")

            # Row 9: Business Intelligence Summary
            st.markdown("####  Business Intelligence Summary")
            
            # Key Insights
            insights_col1, insights_col2 = st.columns(2)
            
            with insights_col1:
                st.markdown("#####   Key Performance Insights")
                
                # Calculate insights
                if 'sales' in df.columns and 'profit' in df.columns:
                    total_revenue = df['sales'].sum()
                    total_profit = df['profit'].sum()
                    profit_margin = (total_profit / total_revenue) * 100 if total_revenue > 0 else 0
                    
                    # Safe category analysis - fixed empty data handling
                    if 'category' in df.columns and len(df) > 0:
                        category_sales = df.groupby('category')['sales'].sum()
                        best_category = category_sales.idxmax() if len(category_sales) > 0 and category_sales.max() > 0 else "N/A"
                        worst_category = category_sales.idxmin() if len(category_sales) > 0 and category_sales.min() >= 0 else "N/A"
                    else:
                        best_category = "N/A"
                        worst_category = "N/A"
                    
                    # Safe monthly analysis
                    if 'order_date' in df.columns and len(df) > 0:
                        try:
                            monthly_sales = df.groupby(df['order_date'].dt.month)['sales'].sum()
                            best_month = monthly_sales.idxmax() if len(monthly_sales) > 0 and monthly_sales.max() > 0 else "N/A"
                        except (AttributeError, ValueError):
                            best_month = "N/A"
                    else:
                        best_month = "N/A"
                    
                    insights = f"""
                     **Overall Profit Margin**: {profit_margin:.1f}%
                    
                    🏆 **Best Performing Category**: {best_category}
                    
                    📉 **Underperforming Category**: {worst_category}
                    
                    📅 **Peak Sales Month**: Month {best_month}
                    
                    👥 **Total Customers**: {df['customer'].nunique():,} unique customers
                    
                      **Average Order Value**: ${df['sales'].mean():.2f}
                    """
                    st.markdown(insights)
            
            with insights_col2:
                st.markdown("#####   Recommendations")
                
                recommendations = """
                  **Growth Opportunities**:
                - Focus marketing on high-value customer segments
                - Expand successful product categories
                - Optimize pricing in underperforming segments
                
                  **Operational Improvements**:
                - Reduce discount dependency for profitability
                - Enhance customer retention programs
                - Streamline supply chain for top cities
                
                 **Strategic Initiatives**:
                - Develop customer loyalty programs
                - Implement dynamic pricing strategies
                - Expand in high-performing regions
                """
                st.markdown(recommendations)
        
        else:
            st.info(" Upload and process a dataset in the 'Data Upload & Processing' tab to see insights here.")
    
    with tab3:
        st.header("  Power BI Integration & Management")
        
        # Get dynamic Power BI directory
        powerbi_dir = st.session_state.get('powerbi_dir')
        
        if not powerbi_dir:
            # Try to detect it from common locations
            # Find Power BI directory using portable paths
            powerbi_dir = find_data_directory()
            
            if not powerbi_dir:
                # Fallback: check possible directories
                possible_dirs = get_base_dirs()
                for dir_path in possible_dirs:
                    if os.path.exists(dir_path):
                        powerbi_dir = dir_path
                        st.session_state.powerbi_dir = powerbi_dir
                        break
        
        if powerbi_dir:
            st.info(f"  Power BI Directory: {powerbi_dir}")
            
            # Power BI Status Section
            st.subheader("🔗 Power BI Connection Status")
            
            # Look for PBIX files in the directory
            pbix_files = [f for f in os.listdir(powerbi_dir) if f.endswith('.pbix')] if os.path.exists(powerbi_dir) else []
            processed_data_file = os.path.join(powerbi_dir, "pipeline_processed_data.csv")
            metadata_file = os.path.join(powerbi_dir, "pipeline_metadata.json")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if pbix_files:
                    st.success("  Power BI File Found")
                    st.text(f"� {pbix_files[0]}")
                else:
                    st.error("❌ Power BI File Not Found")
            
            with col2:
                if os.path.exists(processed_data_file):
                    st.success("  Processed Data Available")
                    # Show file info
                    file_size = os.path.getsize(processed_data_file)
                    st.text(f"  Size: {file_size/1024:.1f} KB")
                    mod_time = datetime.fromtimestamp(os.path.getmtime(processed_data_file))
                    st.text(f"🕒 Modified: {mod_time.strftime('%H:%M:%S')}")
                else:
                    st.warning("  No Processed Data")
            
            with col3:
                if os.path.exists(metadata_file):
                    st.success("  Metadata Available")
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
                    st.warning("  No Metadata")
            
            # Power BI Data Insights
            if os.path.exists(processed_data_file):
                st.subheader("  Power BI Data Insights")
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
                    st.subheader("  Visual Analytics Preview")
                    st.markdown("*This is how your data will look in Power BI dashboards*")
                    
                    # Row 1: Category and Region Analysis
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'category' in powerbi_df.columns and 'sales' in powerbi_df.columns:
                            st.markdown("####   Sales Distribution by Category")
                            category_sales = powerbi_df.groupby('category')['sales'].sum().reset_index()
                            category_sales = category_sales.sort_values('sales', ascending=False)
                            
                            # Create a colorful pie chart
                            fig = px.pie(category_sales, values='sales', names='category', 
                                       title="Sales Distribution by Category",
                                       color_discrete_sequence=px.colors.qualitative.Set3)
                            fig.update_traces(textposition='inside', textinfo='percent+label')
                            fig.update_layout(showlegend=True, height=400)
                            st.plotly_chart(fig, use_container_width=True, key="powerbi_category_pie")
                    
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
                            st.plotly_chart(fig, use_container_width=True, key="powerbi_region_bar")
                    
                    # Row 2: Time Series and State Analysis
                    if 'order_date' in powerbi_df.columns and 'sales' in powerbi_df.columns:
                        st.markdown("####   Sales Trend Analysis")
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
                            st.plotly_chart(fig, use_container_width=True, key="powerbi_monthly_trend")
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
                            st.plotly_chart(fig, use_container_width=True, key="powerbi_top_states")
                    
                    with col2:
                        if 'subcategory' in powerbi_df.columns and 'profit' in powerbi_df.columns:
                            st.markdown("####   Top Subcategories by Profit")
                            subcat_profit = powerbi_df.groupby('subcategory')['profit'].sum().reset_index()
                            top_subcats = subcat_profit.nlargest(10, 'profit')
                            
                            # Check if we have data to display
                            if len(top_subcats) > 0 and top_subcats['profit'].sum() > 0:
                                fig = px.treemap(top_subcats, path=['subcategory'], values='profit',
                                               title="Profit Distribution by Subcategory",
                                               color='profit',
                                               color_continuous_scale='RdYlBu')
                                fig.update_layout(height=400)
                                st.plotly_chart(fig, use_container_width=True, key="powerbi_subcategory_treemap")
                            else:
                                st.warning("⚠️ No subcategory profit data available for treemap.")
                                st.info("💡 Please ensure your dataset contains subcategory data with valid profit values.")
                    
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
                            st.plotly_chart(fig, use_container_width=True, key="powerbi_segment_scatter")
                    
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
                            st.plotly_chart(fig, use_container_width=True, key="powerbi_ship_mode_bar")
                    
                    # Row 5: Advanced Analytics
                    if 'discount' in powerbi_df.columns and 'profit_margin' in powerbi_df.columns:
                        st.markdown("####   Discount vs Profit Margin Analysis")
                        
                        # Check if discount column has valid data for cutting
                        discount_data = powerbi_df['discount'].dropna()
                        if len(discount_data) > 0 and discount_data.max() > discount_data.min():
                            try:
                                # Create discount bins for better visualization
                                powerbi_df['discount_range'] = pd.cut(powerbi_df['discount'], 
                                                                    bins=[0, 0.1, 0.2, 0.3, 1.0], 
                                                                    labels=['0-10%', '11-20%', '21-30%', '30%+'])
                                
                                discount_analysis = powerbi_df.groupby('discount_range').agg({
                                    'profit_margin': 'mean',
                                    'sales': 'sum',
                                    'order_id': 'count'
                                }).reset_index()
                                
                                # Check if we have data after grouping
                                if len(discount_analysis) > 0:
                                    fig = px.bar(discount_analysis, x='discount_range', y='profit_margin',
                                               title="Average Profit Margin by Discount Range",
                                               color='profit_margin',
                                               color_continuous_scale='RdYlGn')
                                    fig.update_layout(height=400, xaxis_title="Discount Range", yaxis_title="Avg Profit Margin")
                                    st.plotly_chart(fig, use_container_width=True, key="powerbi_discount_margin")
                                else:
                                    st.warning("⚠️ No discount analysis data available after grouping.")
                            except ValueError as e:
                                st.warning("⚠️ Unable to create discount analysis chart.")
                                st.info("💡 Discount data may be insufficient for binning analysis.")
                        else:
                            st.warning("⚠️ No valid discount data available for analysis.")
                            st.info("💡 Please ensure your dataset contains numeric discount values with variation.")
                        st.plotly_chart(fig, use_container_width=True, key="service_monitoring_status")
                    
                    # Interactive Data Summary
                    st.markdown("####   Interactive Data Summary")
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
            st.warning("  Power BI directory not found. Upload and process data first to enable Power BI integration.")
        
        # Power BI Management Actions
        st.subheader(" Power BI Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("  Force Power BI Data Refresh", type="primary"):
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
                        
                        st.success("  Power BI data manually refreshed!")
                        st.info("  Open your ECOMMERCE HAMMAD.pbix file in Power BI Desktop and click 'Refresh' to see the updated data.")
                        
                    except Exception as e:
                        st.error(f"❌ Manual refresh failed: {e}")
                else:
                    st.warning("  No processed data available. Please process a dataset first.")
        
        with col2:
            if st.button("🔧 Update Power BI Connection"):
                try:
                    # Run the Power BI automation script
                    result = subprocess.run([
                        "python", "powerbi_automation.py"
                    ], capture_output=True, text=True, cwd=".")
                    
                    if result.returncode == 0:
                        st.success("  Power BI connection updated successfully!")
                        st.code(result.stdout)
                    else:
                        st.error(f"❌ Power BI connection update failed: {result.stderr}")
                        
                except Exception as e:
                    st.error(f"❌ Error updating Power BI connection: {e}")
        
        # Power BI Instructions
        st.subheader("  How to Use Power BI Integration")
        st.markdown("""
        **Automatic Integration Steps:**
        1. 📤 **Upload & Process**: Upload your data in the 'Data Upload & Processing' tab
        2. ✨ **Auto-Update**: The pipeline automatically updates your Power BI data file
        3.   **Refresh Power BI**: Open your `ECOMMERCE HAMMAD.pbix` file in Power BI Desktop
        4. 🔁 **Click Refresh**: In Power BI, click the 'Refresh' button to load new data
        5.   **View Results**: Your Power BI dashboard now shows the cleaned, processed data!
        
        **Key Benefits:**
        -   **No Manual Export**: Data is automatically saved to Power BI location
        -   **Data Cleaning**: All data is cleaned before reaching Power BI
        -   **Real-time Metadata**: Track processing statistics and data quality
        -   **Seamless Workflow**: Upload → Process → Power BI shows results
        """)
    
    with tab4:
        st.header("🔍 Service Monitoring")
        
        st.info("""
        **ℹ️ Service Status Information:**
        - **Running**: Service is confirmed to be active and responding
        - **Stopped**: Service is not running or not accessible  
        - **Port Open (Unknown Service)**: Port is open but service type cannot be verified
        - Most Big Data services require separate installation and configuration
        """)
        
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
                    elif "Unknown Service" in state:
                        st.warning(f"⚠️ {service}: {state}")
                    else:
                        st.warning(f"🟡 {service}: {state}")
                        
                st.markdown("---")
                st.caption("**Note**: For full Big Data functionality, install and configure Hadoop, Kafka, Spark, and Hive separately.")
        
        with col2:
            st.subheader("📊 Pipeline Metrics")
            # Actual metrics based on processed data
            if 'processed_data' in st.session_state:
                df = st.session_state['processed_data']
                st.metric("Records Processed", f"{len(df):,}", delta="Live data")
                if 'insights' in st.session_state:
                    insights = st.session_state['insights']
                    st.metric("Total Sales Value", f"${insights['total_sales']:,.0f}")
                    st.metric("Average Order Value", f"${insights['avg_order_value']:.2f}")
            else:
                st.info("Upload data to see pipeline metrics")
    
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
                label=" Download Processed Data",
                data=csv,
                file_name=f"processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.info("  Process a dataset first to explore the data here.")

if __name__ == "__main__":
    main()
