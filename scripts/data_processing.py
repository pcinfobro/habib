"""
Data Processing Module
Handles data cleaning, transformation, and validation
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timedelta
import re
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingStats:
    """Statistics for data processing operations"""
    original_rows: int
    processed_rows: int
    duplicates_removed: int
    missing_values_handled: int
    outliers_removed: int
    validation_errors: int

class DataProcessor:
    """Main data processing class"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stats = None
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Main processing pipeline"""
        self.logger.info(f"Starting data processing for {len(df)} records")
        original_rows = len(df)
        
        # Initialize stats
        self.stats = ProcessingStats(
            original_rows=original_rows,
            processed_rows=0,
            duplicates_removed=0,
            missing_values_handled=0,
            outliers_removed=0,
            validation_errors=0
        )
        
        try:
            # Step 1: Remove duplicates
            df = self._remove_duplicates(df)
            
            # Step 2: Handle missing values
            df = self._handle_missing_values(df)
            
            # Step 3: Data type conversion and validation
            df = self._validate_and_convert_types(df)
            
            # Step 4: Remove outliers
            df = self._remove_outliers(df)
            
            # Step 5: Standardize data formats
            df = self._standardize_formats(df)
            
            # Step 6: Add derived columns
            df = self._add_derived_columns(df)
            
            # Update final stats
            self.stats.processed_rows = len(df)
            
            self.logger.info(f"Data processing completed. Final records: {len(df)}")
            self._log_processing_stats()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error during data processing: {str(e)}")
            raise
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records"""
        initial_count = len(df)
        
        # Get duplicate columns from config or use all columns
        subset_cols = self.config.get('duplicate_subset')
        keep = self.config.get('duplicate_keep', 'first')
        
        df_cleaned = df.drop_duplicates(subset=subset_cols, keep=keep)
        
        duplicates_removed = initial_count - len(df_cleaned)
        self.stats.duplicates_removed = duplicates_removed
        
        if duplicates_removed > 0:
            self.logger.info(f"Removed {duplicates_removed} duplicate records")
        
        return df_cleaned
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values based on configuration"""
        missing_before = df.isnull().sum().sum()
        
        missing_config = self.config.get('missing_values', {})
        
        for column, strategy in missing_config.items():
            if column not in df.columns:
                continue
            
            if strategy == 'drop':
                df = df.dropna(subset=[column])
            elif strategy == 'forward_fill':
                df[column] = df[column].fillna(method='ffill')
            elif strategy == 'backward_fill':
                df[column] = df[column].fillna(method='bfill')
            elif strategy == 'mean':
                if df[column].dtype in ['int64', 'float64']:
                    df[column] = df[column].fillna(df[column].mean())
            elif strategy == 'median':
                if df[column].dtype in ['int64', 'float64']:
                    df[column] = df[column].fillna(df[column].median())
            elif strategy == 'mode':
                df[column] = df[column].fillna(df[column].mode().iloc[0] if not df[column].mode().empty else 'Unknown')
            elif isinstance(strategy, (str, int, float)):
                df[column] = df[column].fillna(strategy)
        
        # Handle remaining missing values with default strategy
        default_strategy = self.config.get('default_missing_strategy', 'drop')
        if default_strategy == 'drop':
            df = df.dropna()
        elif default_strategy == 'fill_unknown':
            df = df.fillna('Unknown')
        
        missing_after = df.isnull().sum().sum()
        self.stats.missing_values_handled = missing_before - missing_after
        
        if self.stats.missing_values_handled > 0:
            self.logger.info(f"Handled {self.stats.missing_values_handled} missing values")
        
        return df
    
    def _validate_and_convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and convert data types"""
        type_config = self.config.get('data_types', {})
        validation_errors = 0
        
        for column, target_type in type_config.items():
            if column not in df.columns:
                continue
            
            try:
                if target_type == 'datetime':
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                elif target_type == 'numeric':
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif target_type == 'category':
                    df[column] = df[column].astype('category')
                elif target_type == 'string':
                    df[column] = df[column].astype(str)
                
                # Count conversion errors (NaN values after conversion)
                if target_type in ['datetime', 'numeric']:
                    validation_errors += df[column].isnull().sum()
                
            except Exception as e:
                self.logger.warning(f"Failed to convert {column} to {target_type}: {str(e)}")
                validation_errors += 1
        
        self.stats.validation_errors = validation_errors
        
        if validation_errors > 0:
            self.logger.warning(f"Encountered {validation_errors} validation errors")
        
        return df
    
    def _remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove outliers using IQR method"""
        outlier_config = self.config.get('outliers', {})
        initial_count = len(df)
        
        for column in outlier_config.get('columns', []):
            if column not in df.columns or df[column].dtype not in ['int64', 'float64']:
                continue
            
            method = outlier_config.get('method', 'iqr')
            
            if method == 'iqr':
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
            
            elif method == 'zscore':
                z_threshold = outlier_config.get('z_threshold', 3)
                z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
                df = df[z_scores <= z_threshold]
        
        outliers_removed = initial_count - len(df)
        self.stats.outliers_removed = outliers_removed
        
        if outliers_removed > 0:
            self.logger.info(f"Removed {outliers_removed} outlier records")
        
        return df
    
    def _standardize_formats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize data formats"""
        format_config = self.config.get('standardization', {})
        
        # Email standardization
        if 'email_columns' in format_config:
            for col in format_config['email_columns']:
                if col in df.columns:
                    df[col] = df[col].str.lower().str.strip()
        
        # Phone number standardization
        if 'phone_columns' in format_config:
            for col in format_config['phone_columns']:
                if col in df.columns:
                    df[col] = df[col].astype(str).apply(self._standardize_phone)
        
        # Text standardization
        if 'text_columns' in format_config:
            for col in format_config['text_columns']:
                if col in df.columns:
                    df[col] = df[col].str.strip().str.title()
        
        # Currency standardization
        if 'currency_columns' in format_config:
            for col in format_config['currency_columns']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace('$', '').str.replace(',', '')
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def _standardize_phone(self, phone: str) -> str:
        """Standardize phone number format"""
        if pd.isna(phone):
            return phone
        
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', str(phone))
        
        # Format as (XXX) XXX-XXXX for 10-digit numbers
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        else:
            return phone  # Return original if can't standardize
    
    def _add_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived columns based on configuration"""
        derived_config = self.config.get('derived_columns', {})
        
        # Add processing timestamp
        if derived_config.get('add_processing_timestamp', True):
            df['processing_timestamp'] = datetime.now()
        
        # Add data source identifier
        if 'data_source' in derived_config:
            df['data_source'] = derived_config['data_source']
        
        # Custom derived columns
        for col_name, formula in derived_config.get('custom_columns', {}).items():
            try:
                # Simple formula evaluation (extend as needed)
                if isinstance(formula, str):
                    df[col_name] = df.eval(formula)
                elif callable(formula):
                    df[col_name] = df.apply(formula, axis=1)
            except Exception as e:
                self.logger.warning(f"Failed to create derived column {col_name}: {str(e)}")
        
        return df
    
    def _log_processing_stats(self):
        """Log processing statistics"""
        self.logger.info("=== Data Processing Statistics ===")
        self.logger.info(f"Original rows: {self.stats.original_rows}")
        self.logger.info(f"Final rows: {self.stats.processed_rows}")
        self.logger.info(f"Duplicates removed: {self.stats.duplicates_removed}")
        self.logger.info(f"Missing values handled: {self.stats.missing_values_handled}")
        self.logger.info(f"Outliers removed: {self.stats.outliers_removed}")
        self.logger.info(f"Validation errors: {self.stats.validation_errors}")
        
        if self.stats.original_rows > 0:
            retention_rate = (self.stats.processed_rows / self.stats.original_rows) * 100
            self.logger.info(f"Data retention rate: {retention_rate:.2f}%")
    
    def get_processing_stats(self) -> ProcessingStats:
        """Get processing statistics"""
        return self.stats

# Example usage
if __name__ == "__main__":
    # Example configuration
    processing_config = {
        'duplicate_subset': ['customer_id', 'order_id'],
        'missing_values': {
            'customer_name': 'Unknown',
            'order_amount': 'mean',
            'order_date': 'drop',
            'product_category': 'mode'
        },
        'data_types': {
            'order_date': 'datetime',
            'order_amount': 'numeric',
            'customer_id': 'string'
        },
        'outliers': {
            'columns': ['order_amount', 'quantity'],
            'method': 'iqr'
        },
        'standardization': {
            'email_columns': ['customer_email'],
            'phone_columns': ['customer_phone'],
            'currency_columns': ['order_amount']
        },
        'derived_columns': {
            'add_processing_timestamp': True,
            'data_source': 'ecommerce_api',
            'custom_columns': {
                'order_year': 'order_date.dt.year',
                'high_value_order': 'order_amount > 1000'
            }
        }
    }
    
    # Test with sample data
    sample_data = pd.DataFrame({
        'customer_id': ['C001', 'C002', 'C001', 'C003'],
        'customer_name': ['John Doe', None, 'John Doe', 'Jane Smith'],
        'customer_email': ['JOHN@EMAIL.COM', 'jane@email.com', 'john@email.com', None],
        'order_amount': [100.50, 250.00, 100.50, 1500.00],
        'order_date': ['2023-01-15', '2023-01-16', '2023-01-15', '2023-01-17']
    })
    
    processor = DataProcessor(processing_config)
    processed_data = processor.process(sample_data)
    
    print("Processed data:")
    print(processed_data)
    print("\nProcessing stats:")
    print(processor.get_processing_stats())
