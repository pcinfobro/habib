"""
Power BI Integration Script
Bridges the data pipeline with Power BI dashboard by processing the superstore dataset
and outputting it in a format compatible with your existing Power BI dashboard.
"""

import pandas as pd
import logging
import sys
from pathlib import Path
from datetime import datetime
import shutil

# Add the scripts directory to path
sys.path.append(str(Path(__file__).parent / "scripts"))

from scripts.data_processing import DataProcessor
from local_storage import LocalStorageManager
from local_config import LOCAL_CONFIG

import os

# Ensure log directory exists
os.makedirs('data/logs', exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/powerbi_integration.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PowerBIIntegration:
    """Integration class to process data for Power BI consumption"""
    
    def __init__(self):
        self.superstore_path = Path("../ECOMMERCE HAMMAD/cleaned_superstore_dataset.csv")
        self.powerbi_output_path = Path("../ECOMMERCE HAMMAD/pipeline_processed_data.csv")
        self.processor = DataProcessor()
        self.storage_manager = LocalStorageManager(LOCAL_CONFIG['storage']['local'])
        
    def load_superstore_data(self):
        """Load the superstore dataset"""
        try:
            logger.info(f"Loading superstore data from {self.superstore_path}")
            df = pd.read_csv(self.superstore_path)
            logger.info(f"Successfully loaded {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error loading superstore data: {e}")
            raise
    
    def process_for_powerbi(self, df):
        """Process the data specifically for Power BI consumption"""
        logger.info("Processing data for Power BI...")
        
        # Create a copy for processing
        processed_df = df.copy()
        
        # Data cleaning and transformation
        logger.info("Applying data transformations...")
        
        # 1. Standardize date formats
        if 'order_date' in processed_df.columns:
            processed_df['order_date'] = pd.to_datetime(processed_df['order_date'], errors='coerce')
        if 'ship_date' in processed_df.columns:
            processed_df['ship_date'] = pd.to_datetime(processed_df['ship_date'], errors='coerce')
        
        # 2. Handle missing values
        processed_df = processed_df.dropna(subset=['order_id'])  # Remove rows without order ID
        
        # 3. Calculate additional metrics for Power BI
        if 'sales' in processed_df.columns and 'profit' in processed_df.columns:
            # Convert to numeric, handling any non-numeric values
            processed_df['sales'] = pd.to_numeric(processed_df['sales'], errors='coerce')
            processed_df['profit'] = pd.to_numeric(processed_df['profit'], errors='coerce')
            
            # Calculate cost and profit ratio
            processed_df['cost'] = processed_df['sales'] - processed_df['profit']
            processed_df['profit_ratio'] = processed_df['profit'] / processed_df['sales'].replace(0, 1)  # Avoid division by zero
        
        # 4. Create additional categorical features
        if 'order_date' in processed_df.columns:
            processed_df['year'] = processed_df['order_date'].dt.year
            processed_df['month'] = processed_df['order_date'].dt.month
            processed_df['quarter'] = processed_df['order_date'].dt.quarter
            processed_df['day_of_week'] = processed_df['order_date'].dt.day_name()
        
        # 5. Clean text fields
        text_columns = ['customer', 'product_name', 'category', 'subcategory', 'city', 'state']
        for col in text_columns:
            if col in processed_df.columns:
                processed_df[col] = processed_df[col].astype(str).str.strip()
        
        # 6. Handle outliers in financial metrics
        financial_cols = ['sales', 'profit', 'discount']
        for col in financial_cols:
            if col in processed_df.columns:
                # Convert to numeric first
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
                
                # Remove NaN values for quantile calculation
                valid_data = processed_df[col].dropna()
                if len(valid_data) > 0:
                    Q1 = valid_data.quantile(0.25)
                    Q3 = valid_data.quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    # Cap outliers instead of removing them
                    processed_df[col] = processed_df[col].clip(lower_bound, upper_bound)
        
        logger.info(f"Processing completed. Final dataset has {len(processed_df)} records")
        return processed_df
    
    def save_for_powerbi(self, df):
        """Save the processed data for Power BI consumption"""
        try:
            # Save to the Power BI folder
            logger.info(f"Saving processed data to {self.powerbi_output_path}")
            df.to_csv(self.powerbi_output_path, index=False)
            
            # Also save to pipeline output directory with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            pipeline_output_path = Path(f"data/output/powerbi_data_{timestamp}.csv")
            df.to_csv(pipeline_output_path, index=False)
            
            logger.info("Data saved successfully to both locations")
            return str(self.powerbi_output_path)
            
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            raise
    
    def create_metadata_file(self, df):
        """Create a metadata file describing the processed dataset"""
        metadata = {
            'processing_timestamp': datetime.now().isoformat(),
            'record_count': len(df),
            'columns': list(df.columns),
            'date_range': {
                'start_date': df['order_date'].min().isoformat() if 'order_date' in df.columns else None,
                'end_date': df['order_date'].max().isoformat() if 'order_date' in df.columns else None
            },
            'summary_stats': {
                'total_sales': float(df['sales'].sum()) if 'sales' in df.columns else None,
                'total_profit': float(df['profit'].sum()) if 'profit' in df.columns else None,
                'unique_customers': int(df['customer'].nunique()) if 'customer' in df.columns else None,
                'unique_products': int(df['product_name'].nunique()) if 'product_name' in df.columns else None
            }
        }
        
        # Save metadata
        metadata_path = Path("../ECOMMERCE HAMMAD/pipeline_metadata.json")
        import json
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Metadata saved to {metadata_path}")
        return metadata
    
    def run_integration(self):
        """Run the complete integration process"""
        try:
            logger.info("=== Starting Power BI Integration ===")
            
            # Load data
            df = self.load_superstore_data()
            
            # Process data
            processed_df = self.process_for_powerbi(df)
            
            # Save processed data
            output_path = self.save_for_powerbi(processed_df)
            
            # Create metadata
            metadata = self.create_metadata_file(processed_df)
            
            logger.info("=== Power BI Integration Completed Successfully ===")
            logger.info(f"Processed data saved to: {output_path}")
            logger.info(f"Records processed: {len(processed_df)}")
            logger.info(f"Total sales: ${metadata['summary_stats']['total_sales']:,.2f}" if metadata['summary_stats']['total_sales'] else "N/A")
            logger.info(f"Total profit: ${metadata['summary_stats']['total_profit']:,.2f}" if metadata['summary_stats']['total_profit'] else "N/A")
            
            return True
            
        except Exception as e:
            logger.error(f"Integration failed: {e}")
            return False

def main():
    """Main execution function"""
    integration = PowerBIIntegration()
    success = integration.run_integration()
    
    if success:
        print("\n" + "="*50)
        print("SUCCESS: Data pipeline integration completed!")
        print("="*50)
        print("Next steps:")
        print("1. Open your Power BI file (ECOMMERCE HAMMAD.pbix)")
        print("2. Go to Transform Data > Data Source Settings")
        print("3. Change the data source to point to: pipeline_processed_data.csv")
        print("4. Refresh your dashboard to see the processed data")
        print("="*50)
    else:
        print("\n" + "="*50)
        print("ERROR: Integration failed. Check the logs for details.")
        print("="*50)

if __name__ == "__main__":
    main()
