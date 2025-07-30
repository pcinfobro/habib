"""
Simplified local pipeline runner for development and testing
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# Add the scripts directory to path
sys.path.append(str(Path(__file__).parent / "scripts"))

from scripts.data_ingestion import DataIngestionManager
from scripts.data_processing import DataProcessor
from local_storage import LocalStorageManager
from local_config import LOCAL_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class LocalPipelineRunner:
    """Simplified pipeline runner for local development"""
    
    def __init__(self):
        self.config = LOCAL_CONFIG
        self.ingestion_manager = DataIngestionManager()
        self.processor = DataProcessor(self.config['processing'])
        self.storage_manager = LocalStorageManager(self.config['storage']['local'])
        
    def create_sample_data(self):
        """Create sample data files for testing"""
        logger.info("Creating sample data files...")
        
        # Create sample orders CSV
        orders_data = {
            'order_id': ['ORD-001', 'ORD-002', 'ORD-003', 'ORD-004', 'ORD-005'],
            'customer_id': ['CUST-001', 'CUST-002', 'CUST-001', 'CUST-003', 'CUST-002'],
            'order_amount': [150.50, 275.00, 89.99, 450.00, 125.75],
            'order_date': ['2023-01-15', '2023-01-16', '2023-01-17', '2023-01-18', '2023-01-19'],
            'product_id': ['PROD-001', 'PROD-002', 'PROD-003', 'PROD-001', 'PROD-002'],
            'quantity': [2, 1, 3, 1, 2]
        }
        orders_df = pd.DataFrame(orders_data)
        orders_df.to_csv('data/input/orders.csv', index=False)
        
        # Create sample customers CSV
        customers_data = {
            'customer_id': ['CUST-001', 'CUST-002', 'CUST-003'],
            'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
            'customer_email': ['john@email.com', 'jane@email.com', 'bob@email.com'],
            'customer_phone': ['555-1234', '555-5678', '555-9012'],
            'registration_date': ['2022-01-01', '2022-02-15', '2022-03-10']
        }
        customers_df = pd.DataFrame(customers_data)
        customers_df.to_csv('data/input/customers.csv', index=False)
        
        # Create sample products JSON
        products_data = {
            "products": [
                {
                    "product_id": "PROD-001",
                    "product_name": "Wireless Headphones",
                    "price": 75.25,
                    "category": "Electronics",
                    "description": "High-quality wireless headphones"
                },
                {
                    "product_id": "PROD-002",
                    "product_name": "Running Shoes",
                    "price": 275.00,
                    "category": "Sports",
                    "description": "Professional running shoes"
                },
                {
                    "product_id": "PROD-003",
                    "product_name": "Coffee Maker",
                    "price": 29.99,
                    "category": "Home",
                    "description": "Automatic coffee maker"
                }
            ]
        }
        
        import json
        with open('data/input/products.json', 'w') as f:
            json.dump(products_data, f, indent=2)
        
        logger.info("Sample data files created successfully!")
    
    def run_pipeline(self):
        """Run the complete local pipeline"""
        logger.info("Starting local pipeline execution...")
        
        try:
            # Create sample data if it doesn't exist
            if not Path('data/input/orders.csv').exists():
                self.create_sample_data()
            
            results = {}
            
            # Process each data source
            for source_name, source_config in self.config['data_sources'].items():
                logger.info(f"Processing source: {source_name}")
                
                try:
                    # Step 1: Ingest data
                    df = self.ingestion_manager.ingest_data(
                        source_config['type'], 
                        source_config['config']
                    )
                    logger.info(f"Ingested {len(df)} records from {source_name}")
                    
                    # Step 2: Process data
                    processed_df = self.processor.process(df)
                    logger.info(f"Processed {len(processed_df)} records for {source_name}")
                    
                    # Step 3: Store data
                    raw_path = self.storage_manager.store_raw_data(df, source_name)
                    processed_path = self.storage_manager.store_processed_data(
                        processed_df, f"processed_{source_name}"
                    )
                    
                    results[source_name] = {
                        'raw_records': len(df),
                        'processed_records': len(processed_df),
                        'raw_path': raw_path,
                        'processed_path': processed_path
                    }
                    
                except Exception as e:
                    logger.error(f"Error processing {source_name}: {str(e)}")
                    results[source_name] = {'error': str(e)}
            
            # Log summary
            logger.info("=== Pipeline Execution Summary ===")
            for source, result in results.items():
                if 'error' in result:
                    logger.error(f"{source}: {result['error']}")
                else:
                    logger.info(f"{source}: {result['raw_records']} -> {result['processed_records']} records")
            
            return results
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            raise

def main():
    """Main entry point"""
    runner = LocalPipelineRunner()
    results = runner.run_pipeline()
    
    print("\n" + "="*50)
    print("PIPELINE EXECUTION COMPLETED")
    print("="*50)
    
    for source, result in results.items():
        if 'error' not in result:
            print(f"\n{source.upper()}:")
            print(f"  Raw records: {result['raw_records']}")
            print(f"  Processed records: {result['processed_records']}")
            print(f"  Raw data: {result['raw_path']}")
            print(f"  Processed data: {result['processed_path']}")

if __name__ == "__main__":
    main()
