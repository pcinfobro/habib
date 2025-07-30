import pandas as pd
import logging
import sys
from pathlib import Path
from datetime import datetime
import json
import subprocess

sys.path.append(str(Path(__file__).parent / "scripts"))

from scripts.data_processing import DataProcessor
from local_storage import LocalStorageManager
from local_config import LOCAL_CONFIG
import os

os.makedirs('data/logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/production_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ProductionPipelineRunner:
    
    def __init__(self):
        self.datasets = {
            'superstore_raw': '../bigdatasolution-20250730T183140Z-1-001/bigdatasolution/superstore_dataset.csv',
            'superstore_cleaned': '../ECOMMERCE HAMMAD/cleaned_superstore_dataset.csv',
            'navigator_data': '../bigdatasolution-20250730T183140Z-1-001/bigdatasolution/navigator_ft-6804b89276a1fac3abde9f48-data_preview.csv',
            'mock_data': '../bigdatasolution-20250730T183140Z-1-001/bigdatasolution/MOCK_DATA.csv'
        }
        self.processor = DataProcessor()
        self.storage_manager = LocalStorageManager(LOCAL_CONFIG['storage']['local'])
        self.results = {}
        
    def load_and_analyze_dataset(self, name, file_path):
        try:
            logger.info(f"Processing dataset: {name}")
            logger.info(f"Loading from: {file_path}")
            
            df = pd.read_csv(file_path)
            original_count = len(df)
            logger.info(f"Loaded {original_count} records with {len(df.columns)} columns")
            
            profile = {
                'dataset_name': name,
                'file_path': file_path,
                'record_count': original_count,
                'column_count': len(df.columns),
                'columns': list(df.columns),
                'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
                'data_types': df.dtypes.to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'duplicate_count': df.duplicated().sum()
            }
            
            if 'order_date' in df.columns:
                try:
                    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
                    profile['date_range'] = {
                        'start': df['order_date'].min().isoformat() if not df['order_date'].isna().all() else None,
                        'end': df['order_date'].max().isoformat() if not df['order_date'].isna().all() else None
                    }
                except:
                    profile['date_range'] = 'Invalid date format'
            
            financial_metrics = {}
            if 'sales' in df.columns:
                try:
                    df['sales'] = pd.to_numeric(df['sales'], errors='coerce')
                    financial_metrics['total_sales'] = float(df['sales'].sum())
                    financial_metrics['avg_sales'] = float(df['sales'].mean())
                    financial_metrics['max_sales'] = float(df['sales'].max())
                except:
                    financial_metrics['sales_error'] = 'Invalid sales data'
                    
            if 'profit' in df.columns:
                try:
                    df['profit'] = pd.to_numeric(df['profit'], errors='coerce')
                    financial_metrics['total_profit'] = float(df['profit'].sum())
                    financial_metrics['avg_profit'] = float(df['profit'].mean())
                    financial_metrics['profit_margin'] = float(df['profit'].sum() / df['sales'].sum() * 100) if 'sales' in df.columns else None
                except:
                    financial_metrics['profit_error'] = 'Invalid profit data'
            
            profile['financial_metrics'] = financial_metrics
            
            processed_df = self.enhance_dataset(df.copy(), name)
            processed_count = len(processed_df)
            
            profile['processed_count'] = processed_count
            profile['records_removed'] = original_count - processed_count
            profile['data_quality_score'] = round((processed_count / original_count) * 100, 2)
            
            output_path = f"data/output/processed_{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            os.makedirs('data/output', exist_ok=True)
            processed_df.to_csv(output_path, index=False)
            profile['output_file'] = output_path
            
            logger.info(f" {name}: {processed_count}/{original_count} records processed ({profile['data_quality_score']}% quality)")
            if financial_metrics.get('total_sales'):
                logger.info(f" Total Sales: ${financial_metrics['total_sales']:,.2f}")
            if financial_metrics.get('total_profit'):
                logger.info(f" Total Profit: ${financial_metrics['total_profit']:,.2f}")
            
            return profile, processed_df
            
        except Exception as e:
            logger.error(f" Error processing {name}: {e}")
            return None, None
    def enhance_dataset(self, df, dataset_name):
        """Apply dataset-specific enhancements"""
        logger.info(f"Enhancing dataset: {dataset_name}")
        
        df = df.drop_duplicates()
        
        date_columns = ['order_date', 'ship_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        financial_columns = ['sales', 'profit', 'discount', 'quantity']
        for col in financial_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'sales' in df.columns and 'profit' in df.columns:
            df['cost'] = df['sales'] - df['profit']
            df['profit_margin_calc'] = (df['profit'] / df['sales'].replace(0, 1)) * 100
            
        if 'order_date' in df.columns:
            df = df.dropna(subset=['order_date'])
            df['year'] = df['order_date'].dt.year
            df['month'] = df['order_date'].dt.month
            df['quarter'] = df['order_date'].dt.quarter
            df['day_of_week'] = df['order_date'].dt.day_name()
            
        text_columns = ['customer', 'product_name', 'category', 'subcategory', 'city', 'state']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                
        if 'order_id' in df.columns:
            df = df.dropna(subset=['order_id'])
            
        return df
    
    def generate_combined_report(self):
        logger.info("Generating comprehensive analysis report...")
        
        report = {
            'processing_timestamp': datetime.now().isoformat(),
            'total_datasets': len(self.results),
            'datasets': self.results,
            'summary': {}
        }
        
        total_records = sum([r['record_count'] for r in self.results.values() if r])
        total_processed = sum([r['processed_count'] for r in self.results.values() if r])
        total_sales = sum([r['financial_metrics'].get('total_sales', 0) for r in self.results.values() if r])
        total_profit = sum([r['financial_metrics'].get('total_profit', 0) for r in self.results.values() if r])
        
        report['summary'] = {
            'total_raw_records': total_records,
            'total_processed_records': total_processed,
            'overall_quality_score': round((total_processed / total_records) * 100, 2) if total_records > 0 else 0,
            'combined_sales': total_sales,
            'combined_profit': total_profit,
            'overall_profit_margin': round((total_profit / total_sales) * 100, 2) if total_sales > 0 else 0
        }
        
        report_path = f"data/output/production_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs('data/output', exist_ok=True)
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
            
        powerbi_report_path = "../ECOMMERCE HAMMAD/production_analysis_report.json"
        with open(powerbi_report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
            
        logger.info(f"📊 Report saved to: {report_path}")
        return report
    
    def run_production_pipeline(self):
        logger.info("🚀 STARTING PRODUCTION DATA PIPELINE")
        logger.info("="*60)
        
        start_time = datetime.now()
        
        for name, file_path in self.datasets.items():
            if Path(file_path).exists():
                profile, processed_df = self.load_and_analyze_dataset(name, file_path)
                if profile:
                    self.results[name] = profile
            else:
                logger.warning(f"⚠️ Dataset not found: {file_path}")
        
        report = self.generate_combined_report()
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        logger.info("="*60)
        logger.info(" PRODUCTION PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*60)
        logger.info(f" Datasets Processed: {len(self.results)}")
        logger.info(f" Total Records: {report['summary']['total_raw_records']:,}")
        logger.info(f" Processed Records: {report['summary']['total_processed_records']:,}")
        logger.info(f" Quality Score: {report['summary']['overall_quality_score']}%")
        logger.info(f" Combined Sales: ${report['summary']['combined_sales']:,.2f}")
        logger.info(f" Combined Profit: ${report['summary']['combined_profit']:,.2f}")
        logger.info(f" Profit Margin: {report['summary']['overall_profit_margin']}%")
        logger.info(f"Execution Time: {execution_time:.2f} seconds")
        logger.info("="*60)
        
        return report

def main():
    pipeline = ProductionPipelineRunner()
    report = pipeline.run_production_pipeline()
    
    print("\n" + "="*60)
    print(" SUCCESS: PRODUCTION PIPELINE COMPLETED!")
    print("="*60)
    print(" BUSINESS INTELLIGENCE SUMMARY:")
    print(f"   • Total Records Processed: {report['summary']['total_processed_records']:,}")
    print(f"   • Combined Revenue: ${report['summary']['combined_sales']:,.2f}")
    print(f"   • Combined Profit: ${report['summary']['combined_profit']:,.2f}")
    print(f"   • Overall Profit Margin: {report['summary']['overall_profit_margin']}%")
    print(f"   • Data Quality Score: {report['summary']['overall_quality_score']}%")
    print("="*60)
    print(" Output Files Generated:")
    for name, result in pipeline.results.items():
        if result:
            print(f"   • {name}: {result['output_file']}")
    print("   • Comprehensive Report: production_analysis_report.json")
    print("="*60)
    print(" Ready for Power BI Integration!")
    print("="*60)
    
    try:
        logger.info("Running Power BI integration script...")
        subprocess.run(["python", "powerbi_integration.py"], check=True)
        logger.info("Power BI integration completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running Power BI integration: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
