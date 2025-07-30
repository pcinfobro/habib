"""
Pipeline Orchestrator
Main orchestration script that can run the entire pipeline
"""

import logging
import sys
import argparse
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
from pathlib import Path

# Import pipeline modules
from data_ingestion import DataIngestionManager
from data_processing import DataProcessor
from hdfs_storage import DataStorageManager
from kafka_streaming import EcommerceStreamProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    """Main orchestrator for the e-commerce data pipeline"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize components (make them optional for Streamlit)
        try:
            self.ingestion_manager = DataIngestionManager()
        except:
            self.ingestion_manager = None
            
        self.processor = DataProcessor(self.config.get('processing', {}))
        
        try:
            self.storage_manager = DataStorageManager(self.config.get('storage', {}))
        except:
            self.storage_manager = None
        
        # Pipeline statistics
        self.stats = {
            'start_time': None,
            'end_time': None,
            'sources_processed': 0,
            'total_records_ingested': 0,
            'total_records_processed': 0,
            'errors': []
        }
    
    def run_pipeline(self, df: pd.DataFrame, processing_options: Dict) -> pd.DataFrame:
        """Run pipeline for Streamlit dashboard"""
        self.stats['start_time'] = datetime.now()
        self.logger.info(f"Starting pipeline execution for {len(df)} records")
        
        try:
            # Process data
            processed_df = self.processor.process(df)
            self.stats['total_records_processed'] = len(processed_df)
            
            # Store data if storage manager available
            if self.storage_manager and processing_options.get('hdfs', False):
                try:
                    self.storage_manager.store_processed_data(processed_df, 'streamlit_upload')
                except Exception as e:
                    self.logger.warning(f"Storage failed: {e}")
            
            self.stats['end_time'] = datetime.now()
            self.logger.info("Pipeline execution completed successfully")
            
            return processed_df
            
        except Exception as e:
            self.stats['errors'].append(str(e))
            self.logger.error(f"Pipeline execution failed: {e}")
            raise
    
    def _load_config(self, config_path: str) -> Dict:
        """Load pipeline configuration"""
        import json
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            self.logger.info(f"Configuration loaded from {config_path}")
            return config
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            raise
    
    def run_batch_pipeline(self, sources: Optional[List[str]] = None) -> bool:
        """Run the complete batch pipeline"""
        self.stats['start_time'] = datetime.now()
        self.logger.info("Starting batch pipeline execution")
        
        try:
            # Determine which sources to process
            if sources is None:
                sources = list(self.config['data_sources'].keys())
            
            success = True
            
            for source_name in sources:
                try:
                    self.logger.info(f"Processing source: {source_name}")
                    
                    # Step 1: Ingest data
                    raw_data = self._ingest_source_data(source_name)
                    if raw_data is None:
                        continue
                    
                    # Step 2: Process data
                    processed_data = self._process_source_data(source_name, raw_data)
                    if processed_data is None:
                        continue
                    
                    # Step 3: Store data
                    self._store_source_data(source_name, processed_data)
                    
                    # Step 4: Validate data quality
                    self._validate_data_quality(source_name, processed_data)
                    
                    self.stats['sources_processed'] += 1
                    self.logger.info(f"Successfully processed source: {source_name}")
                    
                except Exception as e:
                    error_msg = f"Failed to process source {source_name}: {str(e)}"
                    self.logger.error(error_msg)
                    self.stats['errors'].append(error_msg)
                    success = False
            
            # Final pipeline validation
            if success:
                success = self._run_final_validation()
            
            self.stats['end_time'] = datetime.now()
            self._log_pipeline_summary()
            
            return success
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            self.stats['errors'].append(str(e))
            return False
    
    def _ingest_source_data(self, source_name: str) -> Optional[pd.DataFrame]:
        """Ingest data from a specific source"""
        try:
            if self.ingestion_manager is None:
                self.logger.error(f"Ingestion manager not initialized for {source_name}")
                return None
                
            source_config = self.config['data_sources'][source_name]
            source_type = source_config['type']
            config = source_config['config']
            
            self.logger.info(f"Ingesting data from {source_name} ({source_type})")
            
            df = self.ingestion_manager.ingest_data(source_type, config)
            
            self.stats['total_records_ingested'] += len(df)
            self.logger.info(f"Ingested {len(df)} records from {source_name}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Data ingestion failed for {source_name}: {str(e)}")
            return None
    
    def _process_source_data(self, source_name: str, raw_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Process data from a specific source"""
        try:
            self.logger.info(f"Processing data for {source_name}")
            
            # Apply source-specific processing configuration if available
            source_processing_config = self.config.get('source_processing', {}).get(source_name, {})
            if source_processing_config:
                # Create a processor with source-specific config
                source_processor = DataProcessor(source_processing_config)
                processed_data = source_processor.process(raw_data)
            else:
                # Use default processor
                processed_data = self.processor.process(raw_data)
            
            self.stats['total_records_processed'] += len(processed_data)
            self.logger.info(f"Processed {len(processed_data)} records for {source_name}")
            
            return processed_data
            
        except Exception as e:
            self.logger.error(f"Data processing failed for {source_name}: {str(e)}")
            return None
    
    def _store_source_data(self, source_name: str, processed_data: pd.DataFrame):
        """Store processed data for a specific source"""
        try:
            if self.storage_manager is None:
                self.logger.warning(f"Storage manager not initialized, skipping storage for {source_name}")
                return
                
            self.logger.info(f"Storing data for {source_name}")
            
            # Store raw data in HDFS
            raw_path = self.storage_manager.store_raw_data(processed_data, source_name)
            
            # Store processed data in Hive
            table_name = f"processed_{source_name}"
            
            # Get partition configuration for this source
            partition_config = self.config.get('partitioning', {}).get(source_name, {})
            partition_columns = partition_config.get('columns')
            
            processed_paths = self.storage_manager.store_processed_data(
                processed_data, table_name, partition_columns
            )
            
            self.logger.info(f"Data stored successfully for {source_name}")
            
        except Exception as e:
            self.logger.error(f"Data storage failed for {source_name}: {str(e)}")
            raise
    
    def _validate_data_quality(self, source_name: str, data: pd.DataFrame):
        """Validate data quality for a specific source"""
        try:
            quality_config = self.config.get('data_quality', {}).get(source_name, {})
            
            if not quality_config:
                self.logger.info(f"No quality checks configured for {source_name}")
                return
            
            self.logger.info(f"Running data quality checks for {source_name}")
            
            # Check minimum record count
            min_records = quality_config.get('min_records', 0)
            if len(data) < min_records:
                raise ValueError(f"Insufficient records: {len(data)} < {min_records}")
            
            # Check required columns
            required_columns = quality_config.get('required_columns', [])
            missing_columns = set(required_columns) - set(data.columns)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Check null percentages
            null_thresholds = quality_config.get('null_thresholds', {})
            for column, max_null_pct in null_thresholds.items():
                if column in data.columns:
                    null_pct = (data[column].isnull().sum() / len(data)) * 100
                    if null_pct > max_null_pct:
                        raise ValueError(f"Too many nulls in {column}: {null_pct:.2f}% > {max_null_pct}%")
            
            self.logger.info(f"Data quality checks passed for {source_name}")
            
        except Exception as e:
            self.logger.error(f"Data quality check failed for {source_name}: {str(e)}")
            raise
    
    def _run_final_validation(self) -> bool:
        """Run final pipeline validation"""
        try:
            self.logger.info("Running final pipeline validation")
            
            if self.storage_manager is None:
                self.logger.warning("Storage manager not initialized, skipping final validation")
                return True
            
            # Check that all expected tables exist and have data
            validation_queries = self.config.get('final_validation', {}).get('queries', [])
            
            for query_config in validation_queries:
                query = query_config['query']
                expected_min = query_config.get('expected_min', 1)
                
                try:
                    result = self.storage_manager.query_data(query)
                    # Parse result (simplified)
                    count = int(result.strip().split('\n')[-1])
                    
                    if count < expected_min:
                        self.logger.error(f"Validation failed: {query} returned {count} < {expected_min}")
                        return False
                    
                    self.logger.info(f"Validation passed: {query} returned {count}")
                    
                except Exception as e:
                    self.logger.error(f"Validation query failed: {query} - {str(e)}")
                    return False
            
            self.logger.info("Final pipeline validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Final validation failed: {str(e)}")
            return False
    
    def _log_pipeline_summary(self):
        """Log pipeline execution summary"""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        self.logger.info("=== Pipeline Execution Summary ===")
        self.logger.info(f"Start time: {self.stats['start_time']}")
        self.logger.info(f"End time: {self.stats['end_time']}")
        self.logger.info(f"Duration: {duration}")
        self.logger.info(f"Sources processed: {self.stats['sources_processed']}")
        self.logger.info(f"Total records ingested: {self.stats['total_records_ingested']}")
        self.logger.info(f"Total records processed: {self.stats['total_records_processed']}")
        self.logger.info(f"Errors: {len(self.stats['errors'])}")
        
        if self.stats['errors']:
            self.logger.error("Errors encountered:")
            for error in self.stats['errors']:
                self.logger.error(f"  - {error}")
    
    def run_streaming_pipeline(self):
        """Run the streaming pipeline"""
        try:
            self.logger.info("Starting streaming pipeline")
            
            kafka_config = self.config.get('streaming', {}).get('kafka', {})
            processor = EcommerceStreamProcessor(kafka_config)
            
            # Start processing
            input_topics = kafka_config.get('input_topics', ['raw-orders', 'raw-customers', 'raw-products'])
            processor.start_processing(input_topics)
            
            self.logger.info("Streaming pipeline started successfully")
            
            # In a real implementation, this would run indefinitely
            # For demo purposes, we'll return the processor
            return processor
            
        except Exception as e:
            self.logger.error(f"Streaming pipeline failed: {str(e)}")
            raise
    
    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status"""
        return {
            'stats': self.stats,
            'config_loaded': bool(self.config),
            'components_initialized': all([
                self.ingestion_manager,
                self.processor,
                self.storage_manager
            ])
        }

def main():
    """Main entry point for the pipeline orchestrator"""
    parser = argparse.ArgumentParser(description='E-commerce Data Pipeline Orchestrator')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--mode', choices=['batch', 'streaming'], default='batch', help='Pipeline mode')
    parser.add_argument('--sources', nargs='*', help='Specific sources to process (batch mode only)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    
    args = parser.parse_args()
    
    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    try:
        # Initialize orchestrator
        orchestrator = PipelineOrchestrator(args.config)
        
        if args.mode == 'batch':
            success = orchestrator.run_batch_pipeline(args.sources)
            sys.exit(0 if success else 1)
        
        elif args.mode == 'streaming':
            processor = orchestrator.run_streaming_pipeline()
            # Keep running until interrupted
            try:
                import time
                while True:
                    time.sleep(60)
                    logger.info(f"Streaming metrics: {processor.get_metrics()}")
            except KeyboardInterrupt:
                logger.info("Stopping streaming pipeline...")
                processor.stop_processing()
        
    except Exception as e:
        logger.error(f"Pipeline orchestrator failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
