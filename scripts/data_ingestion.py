"""
Data Ingestion Module
Handles ingestion from CSV, JSON, and REST API sources
"""

import pandas as pd
import requests
import json
import logging
from typing import Dict, List, Optional, Union
from pathlib import Path
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataIngestionError(Exception):
    """Custom exception for data ingestion errors"""
    pass

class DataIngestor:
    """Base class for data ingestion"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def ingest(self) -> pd.DataFrame:
        """Abstract method to be implemented by subclasses"""
        raise NotImplementedError

class CSVIngestor(DataIngestor):
    """Ingest data from CSV files"""
    
    def ingest(self) -> pd.DataFrame:
        try:
            file_path = self.config.get('file_path')
            if not file_path or not Path(file_path).exists():
                raise DataIngestionError(f"CSV file not found: {file_path}")
            
            self.logger.info(f"Ingesting CSV data from {file_path}")
            
            # Read CSV with error handling
            df = pd.read_csv(
                file_path,
                encoding=self.config.get('encoding', 'utf-8'),
                sep=self.config.get('separator', ','),
                na_values=self.config.get('na_values', ['', 'NULL', 'null', 'N/A'])
            )
            
            self.logger.info(f"Successfully ingested {len(df)} records from CSV")
            return df
            
        except Exception as e:
            self.logger.error(f"Error ingesting CSV data: {str(e)}")
            raise DataIngestionError(f"CSV ingestion failed: {str(e)}")

class JSONIngestor(DataIngestor):
    """Ingest data from JSON files"""
    
    def ingest(self) -> pd.DataFrame:
        try:
            file_path = self.config.get('file_path')
            if not file_path or not Path(file_path).exists():
                raise DataIngestionError(f"JSON file not found: {file_path}")
            
            self.logger.info(f"Ingesting JSON data from {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # If it's a dict with a data key, use that
                if 'data' in data:
                    df = pd.DataFrame(data['data'])
                else:
                    # Convert dict to single row DataFrame
                    df = pd.DataFrame([data])
            else:
                raise DataIngestionError("Unsupported JSON structure")
            
            self.logger.info(f"Successfully ingested {len(df)} records from JSON")
            return df
            
        except Exception as e:
            self.logger.error(f"Error ingesting JSON data: {str(e)}")
            raise DataIngestionError(f"JSON ingestion failed: {str(e)}")

class APIIngestor(DataIngestor):
    """Ingest data from REST APIs"""
    
    def ingest(self) -> pd.DataFrame:
        try:
            url = self.config.get('url')
            if not url:
                raise DataIngestionError("API URL not provided")
            
            self.logger.info(f"Ingesting data from API: {url}")
            
            # Prepare request parameters
            headers = self.config.get('headers', {})
            params = self.config.get('params', {})
            auth = self.config.get('auth')
            timeout = self.config.get('timeout', 30)
            
            # Make API request
            response = requests.get(
                url,
                headers=headers,
                params=params,
                auth=auth,
                timeout=timeout
            )
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            
            # Handle pagination if configured
            all_data = []
            if self.config.get('paginated', False):
                all_data.extend(self._handle_pagination(data))
            else:
                # Extract data based on configuration
                data_key = self.config.get('data_key', 'data')
                if data_key and data_key in data:
                    all_data = data[data_key]
                else:
                    all_data = data if isinstance(data, list) else [data]
            
            df = pd.DataFrame(all_data)
            self.logger.info(f"Successfully ingested {len(df)} records from API")
            return df
            
        except requests.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise DataIngestionError(f"API ingestion failed: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error ingesting API data: {str(e)}")
            raise DataIngestionError(f"API ingestion failed: {str(e)}")
    
    def _handle_pagination(self, initial_data: Dict) -> List[Dict]:
        """Handle paginated API responses"""
        all_records = []
        current_data = initial_data
        
        data_key = self.config.get('data_key', 'data')
        next_key = self.config.get('next_key', 'next')
        
        while current_data:
            # Extract records from current page
            if data_key in current_data:
                all_records.extend(current_data[data_key])
            
            # Check for next page
            next_url = current_data.get(next_key)
            if not next_url:
                break
            
            # Fetch next page
            try:
                response = requests.get(next_url, timeout=self.config.get('timeout', 30))
                response.raise_for_status()
                current_data = response.json()
            except Exception as e:
                self.logger.warning(f"Failed to fetch next page: {str(e)}")
                break
        
        return all_records

class DataIngestionManager:
    """Manager class to handle different data sources"""
    
    def __init__(self):
        self.ingestors = {
            'csv': CSVIngestor,
            'json': JSONIngestor,
            'api': APIIngestor
        }
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def ingest_data(self, source_type: str, config: Dict) -> pd.DataFrame:
        """Ingest data from specified source type"""
        if source_type not in self.ingestors:
            raise DataIngestionError(f"Unsupported source type: {source_type}")
        
        ingestor_class = self.ingestors[source_type]
        ingestor = ingestor_class(config)
        
        return ingestor.ingest()
    
    def register_ingestor(self, source_type: str, ingestor_class):
        """Register a new ingestor for future data sources"""
        self.ingestors[source_type] = ingestor_class
        self.logger.info(f"Registered new ingestor for source type: {source_type}")

# Example usage and testing
if __name__ == "__main__":
    # Example configurations
    csv_config = {
        'file_path': 'data/ecommerce_data.csv',
        'encoding': 'utf-8',
        'separator': ',',
        'na_values': ['', 'NULL', 'null', 'N/A']
    }
    
    json_config = {
        'file_path': 'data/products.json'
    }
    
    api_config = {
        'url': 'https://api.example.com/products',
        'headers': {'Authorization': 'Bearer token'},
        'data_key': 'products',
        'paginated': True,
        'next_key': 'next_page_url',
        'timeout': 30
    }
    
    # Initialize manager
    manager = DataIngestionManager()
    
    # Test ingestion (uncomment to test with actual data)
    # try:
    #     csv_data = manager.ingest_data('csv', csv_config)
    #     print(f"CSV data shape: {csv_data.shape}")
    #     
    #     json_data = manager.ingest_data('json', json_config)
    #     print(f"JSON data shape: {json_data.shape}")
    #     
    #     api_data = manager.ingest_data('api', api_config)
    #     print(f"API data shape: {api_data.shape}")
    # except DataIngestionError as e:
    #     print(f"Ingestion error: {e}")
