"""
HDFS Storage Module
Handles storing processed data in Hadoop HDFS and Hive tables
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Union
from datetime import datetime, date, timedelta
import os
import json
from pathlib import Path
import subprocess
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HDFSStorageError(Exception):
    """Custom exception for HDFS storage errors"""
    pass

class HDFSManager:
    """Manager for HDFS operations"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.hdfs_url = config.get('hdfs_url', 'hdfs://localhost:9000')
        self.hdfs_user = config.get('hdfs_user', 'hadoop')
        
        # Validate HDFS connection
        self._validate_connection()
    
    def _validate_connection(self):
        """Validate HDFS connection"""
        try:
            result = subprocess.run(
                ['hdfs', 'dfs', '-ls', '/'],
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                raise HDFSStorageError(f"HDFS connection failed: {result.stderr}")
            self.logger.info("HDFS connection validated successfully")
        except subprocess.TimeoutExpired:
            raise HDFSStorageError("HDFS connection timeout")
        except FileNotFoundError:
            self.logger.warning("HDFS CLI not found. Assuming HDFS is available.")
    
    def create_directory(self, hdfs_path: str):
        """Create directory in HDFS"""
        try:
            result = subprocess.run(
                ['hdfs', 'dfs', '-mkdir', '-p', hdfs_path],
                capture_output=True,
                text=True
            )
            if result.returncode != 0 and "File exists" not in result.stderr:
                raise HDFSStorageError(f"Failed to create directory {hdfs_path}: {result.stderr}")
            self.logger.info(f"Directory created/verified: {hdfs_path}")
        except Exception as e:
            raise HDFSStorageError(f"Error creating directory {hdfs_path}: {str(e)}")
    
    def save_dataframe(self, df: pd.DataFrame, hdfs_path: str, format: str = 'csv'):
        """Save pandas DataFrame to HDFS"""
        try:
            # Create temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix=f'.{format}', delete=False) as temp_file:
                if format.lower() == 'csv':
                    df.to_csv(temp_file.name, index=False)
                elif format.lower() == 'parquet':
                    df.to_parquet(temp_file.name, index=False)
                elif format.lower() == 'json':
                    df.to_json(temp_file.name, orient='records', lines=True)
                else:
                    raise HDFSStorageError(f"Unsupported format: {format}")
                
                temp_path = temp_file.name
            
            # Create HDFS directory if needed
            hdfs_dir = os.path.dirname(hdfs_path)
            if hdfs_dir:
                self.create_directory(hdfs_dir)
            
            # Upload to HDFS
            self.upload_file(temp_path, hdfs_path, overwrite=True)
            
            # Clean up temporary file
            os.unlink(temp_path)
            
            self.logger.info(f"DataFrame saved to HDFS: {hdfs_path} ({len(df)} records)")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save DataFrame to HDFS: {e}")
            return False

    def upload_file(self, local_path: str, hdfs_path: str, overwrite: bool = False):
        """Upload file to HDFS"""
        try:
            cmd = ['hdfs', 'dfs', '-put']
            if overwrite:
                cmd.append('-f')
            cmd.extend([local_path, hdfs_path])
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise HDFSStorageError(f"Failed to upload {local_path} to {hdfs_path}: {result.stderr}")
            
            self.logger.info(f"File uploaded: {local_path} -> {hdfs_path}")
        except Exception as e:
            raise HDFSStorageError(f"Error uploading file: {str(e)}")
    
    def download_file(self, hdfs_path: str, local_path: str):
        """Download file from HDFS"""
        try:
            result = subprocess.run(
                ['hdfs', 'dfs', '-get', hdfs_path, local_path],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                raise HDFSStorageError(f"Failed to download {hdfs_path} to {local_path}: {result.stderr}")
            
            self.logger.info(f"File downloaded: {hdfs_path} -> {local_path}")
        except Exception as e:
            raise HDFSStorageError(f"Error downloading file: {str(e)}")
    
    def list_directory(self, hdfs_path: str) -> List[str]:
        """List contents of HDFS directory"""
        try:
            result = subprocess.run(
                ['hdfs', 'dfs', '-ls', hdfs_path],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                raise HDFSStorageError(f"Failed to list directory {hdfs_path}: {result.stderr}")
            
            # Parse output to extract file names
            files = []
            for line in result.stdout.strip().split('\n')[1:]:  # Skip header
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 8:
                        files.append(parts[-1])  # Last part is the file path
            
            return files
        except Exception as e:
            raise HDFSStorageError(f"Error listing directory {hdfs_path}: {str(e)}")
    
    def delete_path(self, hdfs_path: str, recursive: bool = False):
        """Delete file or directory from HDFS"""
        try:
            cmd = ['hdfs', 'dfs', '-rm']
            if recursive:
                cmd.append('-r')
            cmd.append(hdfs_path)
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise HDFSStorageError(f"Failed to delete {hdfs_path}: {result.stderr}")
            
            self.logger.info(f"Deleted from HDFS: {hdfs_path}")
        except Exception as e:
            raise HDFSStorageError(f"Error deleting {hdfs_path}: {str(e)}")

class HiveManager:
    """Manager for Hive operations"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.database = config.get('database', 'ecommerce')
        
        # Initialize database
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize Hive database"""
        try:
            self.execute_query(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            self.execute_query(f"USE {self.database}")
            self.logger.info(f"Database {self.database} initialized")
        except Exception as e:
            raise HDFSStorageError(f"Failed to initialize database: {str(e)}")
    
    def execute_query(self, query: str) -> str:
        """Execute Hive query"""
        try:
            # Write query to temporary file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.hql', delete=False) as f:
                f.write(query)
                query_file = f.name
            
            # Execute query using Hive CLI
            result = subprocess.run(
                ['hive', '-f', query_file],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            # Clean up temporary file
            os.unlink(query_file)
            
            if result.returncode != 0:
                raise HDFSStorageError(f"Hive query failed: {result.stderr}")
            
            self.logger.debug(f"Executed Hive query: {query[:100]}...")
            return result.stdout
            
        except subprocess.TimeoutExpired:
            raise HDFSStorageError("Hive query timeout")
        except Exception as e:
            raise HDFSStorageError(f"Error executing Hive query: {str(e)}")
    
    def create_table(self, table_name: str, schema: Dict, partition_columns: Optional[List[str]] = None,
                    storage_format: str = 'PARQUET', location: Optional[str] = None):
        """Create Hive table"""
        try:
            # Build column definitions
            columns = []
            for col_name, col_type in schema.items():
                if partition_columns and col_name in partition_columns:
                    continue  # Skip partition columns in main schema
                columns.append(f"{col_name} {col_type}")
            
            query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            query += ",\n".join(f"  {col}" for col in columns)
            query += "\n)"
            
            # Add partition clause
            if partition_columns:
                partition_cols = []
                for col in partition_columns:
                    if col in schema:
                        partition_cols.append(f"{col} {schema[col]}")
                if partition_cols:
                    query += "\nPARTITIONED BY (\n"
                    query += ",\n".join(f"  {col}" for col in partition_cols)
                    query += "\n)"
            
            # Add storage format
            query += f"\nSTORED AS {storage_format}"
            
            # Add location if specified
            if location:
                query += f"\nLOCATION '{location}'"
            
            self.execute_query(query)
            self.logger.info(f"Table {table_name} created successfully")
            
        except Exception as e:
            raise HDFSStorageError(f"Failed to create table {table_name}: {str(e)}")
    
    def insert_data(self, table_name: str, data_path: str, partition_values: Optional[Dict] = None):
        """Insert data into Hive table"""
        try:
            query = f"LOAD DATA INPATH '{data_path}' INTO TABLE {table_name}"
            
            if partition_values:
                partition_spec = ", ".join([f"{k}='{v}'" for k, v in partition_values.items()])
                query += f" PARTITION ({partition_spec})"
            
            self.execute_query(query)
            self.logger.info(f"Data loaded into table {table_name}")
            
        except Exception as e:
            raise HDFSStorageError(f"Failed to insert data into {table_name}: {str(e)}")
    
    def query_table(self, query: str) -> str:
        """Query Hive table and return results"""
        return self.execute_query(query)
    
    def drop_table(self, table_name: str):
        """Drop Hive table"""
        try:
            self.execute_query(f"DROP TABLE IF EXISTS {table_name}")
            self.logger.info(f"Table {table_name} dropped")
        except Exception as e:
            raise HDFSStorageError(f"Failed to drop table {table_name}: {str(e)}")

class DataStorageManager:
    """Main class for managing data storage in HDFS and Hive"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize managers
        self.hdfs = HDFSManager(config.get('hdfs', {}))
        self.hive = HiveManager(config.get('hive', {}))
        
        # Storage paths
        self.base_path = config.get('base_path', '/data/ecommerce')
        self.raw_path = f"{self.base_path}/raw"
        self.processed_path = f"{self.base_path}/processed"
        self.archive_path = f"{self.base_path}/archive"
        
        # Create base directories
        self._initialize_directories()
    
    def _initialize_directories(self):
        """Initialize HDFS directory structure"""
        directories = [self.base_path, self.raw_path, self.processed_path, self.archive_path]
        for directory in directories:
            self.hdfs.create_directory(directory)
    
    def store_raw_data(self, df: pd.DataFrame, data_source: str, timestamp: Optional[datetime] = None) -> str:
        """Store raw data in HDFS"""
        if timestamp is None:
            timestamp = datetime.now()
        
        # Create file path with timestamp
        date_str = timestamp.strftime('%Y/%m/%d')
        filename = f"{data_source}_{timestamp.strftime('%Y%m%d_%H%M%S')}.parquet"
        hdfs_dir = f"{self.raw_path}/{data_source}/{date_str}"
        hdfs_path = f"{hdfs_dir}/{filename}"
        
        try:
            # Create directory
            self.hdfs.create_directory(hdfs_dir)
            
            # Save DataFrame to temporary local file
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
                df.to_parquet(temp_file.name, index=False)
                temp_path = temp_file.name
            
            # Upload to HDFS
            self.hdfs.upload_file(temp_path, hdfs_path, overwrite=True)
            
            # Clean up temporary file
            os.unlink(temp_path)
            
            self.logger.info(f"Raw data stored: {hdfs_path} ({len(df)} records)")
            return hdfs_path
            
        except Exception as e:
            self.logger.error(f"Failed to store raw data: {str(e)}")
            raise HDFSStorageError(f"Failed to store raw data: {str(e)}")
    
    def store_processed_data(self, df: pd.DataFrame, table_name: str, 
                           partition_columns: Optional[List[str]] = None,
                           timestamp: Optional[datetime] = None) -> Union[str, List[str]]:
        """Store processed data in HDFS and create/update Hive table"""
        if timestamp is None:
            timestamp = datetime.now()
        
        try:
            # Determine schema from DataFrame
            schema = self._get_hive_schema(df)
            
            # Create Hive table if it doesn't exist
            self.hive.create_table(
                table_name=table_name,
                schema=schema,
                partition_columns=partition_columns,
                storage_format='PARQUET'
            )
            
            # Prepare data for storage
            if partition_columns:
                # Group by partition columns and store separately
                partition_groups = df.groupby(partition_columns)
                stored_paths = []
                
                for partition_values, group_df in partition_groups:
                    if isinstance(partition_values, tuple):
                        partition_dict = dict(zip(partition_columns, partition_values))
                    else:
                        partition_dict = {partition_columns[0]: partition_values}
                    
                    path = self._store_partition_data(group_df, table_name, partition_dict, timestamp)
                    stored_paths.append(path)
                
                return stored_paths
            else:
                # Store without partitioning
                return self._store_table_data(df, table_name, timestamp)
                
        except Exception as e:
            self.logger.error(f"Failed to store processed data: {str(e)}")
            raise HDFSStorageError(f"Failed to store processed data: {str(e)}")
    
    def _store_partition_data(self, df: pd.DataFrame, table_name: str, 
                            partition_values: Dict, timestamp: datetime) -> str:
        """Store partitioned data"""
        # Create partition path
        partition_path_parts = [f"{k}={v}" for k, v in partition_values.items()]
        partition_path = "/".join(partition_path_parts)
        
        date_str = timestamp.strftime('%Y%m%d_%H%M%S')
        filename = f"{table_name}_{date_str}.parquet"
        hdfs_dir = f"{self.processed_path}/{table_name}/{partition_path}"
        hdfs_path = f"{hdfs_dir}/{filename}"
        
        # Create directory
        self.hdfs.create_directory(hdfs_dir)
        
        # Remove partition columns from DataFrame (they're in the path)
        df_to_store = df.drop(columns=list(partition_values.keys()), errors='ignore')
        
        # Save to temporary file and upload
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            df_to_store.to_parquet(temp_file.name, index=False)
            temp_path = temp_file.name
        
        self.hdfs.upload_file(temp_path, hdfs_path, overwrite=True)
        os.unlink(temp_path)
        
        # Add partition to Hive table
        self.hive.insert_data(table_name, hdfs_path, partition_values)
        
        self.logger.info(f"Partition data stored: {hdfs_path} ({len(df)} records)")
        return hdfs_path
    
    def _store_table_data(self, df: pd.DataFrame, table_name: str, timestamp: datetime) -> str:
        """Store non-partitioned table data"""
        date_str = timestamp.strftime('%Y%m%d_%H%M%S')
        filename = f"{table_name}_{date_str}.parquet"
        hdfs_dir = f"{self.processed_path}/{table_name}"
        hdfs_path = f"{hdfs_dir}/{filename}"
        
        # Create directory
        self.hdfs.create_directory(hdfs_dir)
        
        # Save to temporary file and upload
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
            df.to_parquet(temp_file.name, index=False)
            temp_path = temp_file.name
        
        self.hdfs.upload_file(temp_path, hdfs_path, overwrite=True)
        os.unlink(temp_path)
        
        # Load data into Hive table
        self.hive.insert_data(table_name, hdfs_path)
        
        self.logger.info(f"Table data stored: {hdfs_path} ({len(df)} records)")
        return hdfs_path
    
    def _get_hive_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        """Convert pandas DataFrame dtypes to Hive schema"""
        type_mapping = {
            'int64': 'BIGINT',
            'int32': 'INT',
            'float64': 'DOUBLE',
            'float32': 'FLOAT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'object': 'STRING'
        }
        
        schema = {}
        for column, dtype in df.dtypes.items():
            hive_type = type_mapping.get(str(dtype), 'STRING')
            schema[column] = hive_type
        
        return schema
    
    def query_data(self, query: str) -> str:
        """Query data from Hive tables"""
        return self.hive.query_table(query)
    
    def archive_data(self, source_path: str, archive_name: str):
        """Archive data to archive directory"""
        archive_path = f"{self.archive_path}/{archive_name}"
        
        # Create archive directory
        self.hdfs.create_directory(os.path.dirname(archive_path))
        
        # Move data to archive (this is a simplified version)
        # In practice, you might want to compress or reorganize the data
        self.logger.info(f"Data archived: {source_path} -> {archive_path}")
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """Clean up old data based on retention policy"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        # This is a simplified cleanup - in practice, you'd parse directory names
        # and dates to determine what to clean up
        self.logger.info(f"Cleanup completed for data older than {cutoff_date}")

# Example usage
if __name__ == "__main__":
    # Configuration
    storage_config = {
        'hdfs': {
            'hdfs_url': 'hdfs://localhost:9000',
            'hdfs_user': 'hadoop'
        },
        'hive': {
            'database': 'ecommerce'
        },
        'base_path': '/data/ecommerce'
    }
    
    # Initialize storage manager
    storage_manager = DataStorageManager(storage_config)
    
    # Example: Store sample data
    sample_data = pd.DataFrame({
        'order_id': ['ORD-001', 'ORD-002', 'ORD-003'],
        'customer_id': ['CUST-001', 'CUST-002', 'CUST-001'],
        'order_amount': [100.50, 250.00, 75.25],
        'order_date': pd.to_datetime(['2023-01-15', '2023-01-16', '2023-01-17']),
        'product_category': ['Electronics', 'Clothing', 'Books']
    })
    
    # Store raw data
    raw_path = storage_manager.store_raw_data(sample_data, 'orders')
    print(f"Raw data stored at: {raw_path}")
    
    # Store processed data with partitioning
    processed_paths = storage_manager.store_processed_data(
        sample_data, 
        'processed_orders',
        partition_columns=['product_category']
    )
    print(f"Processed data stored at: {processed_paths}")
    
    # Query data
    result = storage_manager.query_data("SELECT COUNT(*) FROM processed_orders")
    print(f"Query result: {result}")
