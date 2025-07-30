"""
Local storage manager for development without Hadoop/Hive
"""

import pandas as pd
import logging
from typing import Dict, Optional
from datetime import datetime
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class LocalStorageManager:
    """Local file system storage manager for development"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.output_dir = Path(config.get('output_dir', 'data/output'))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.format = config.get('format', 'parquet')
        
    def store_raw_data(self, df: pd.DataFrame, data_source: str, 
                      timestamp: Optional[datetime] = None) -> str:
        """Store raw data locally"""
        if timestamp is None:
            timestamp = datetime.now()
        
        filename = f"raw_{data_source}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        
        if self.format == 'parquet':
            filepath = self.output_dir / f"{filename}.parquet"
            df.to_parquet(filepath, index=False)
        else:
            filepath = self.output_dir / f"{filename}.csv"
            df.to_csv(filepath, index=False)
        
        logger.info(f"Raw data stored: {filepath} ({len(df)} records)")
        return str(filepath)
    
    def store_processed_data(self, df: pd.DataFrame, table_name: str,
                           timestamp: Optional[datetime] = None) -> str:
        """Store processed data locally"""
        if timestamp is None:
            timestamp = datetime.now()
        
        filename = f"{table_name}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        
        if self.format == 'parquet':
            filepath = self.output_dir / f"{filename}.parquet"
            df.to_parquet(filepath, index=False)
        else:
            filepath = self.output_dir / f"{filename}.csv"
            df.to_csv(filepath, index=False)
        
        logger.info(f"Processed data stored: {filepath} ({len(df)} records)")
        return str(filepath)
    
    def load_data(self, filepath: str) -> pd.DataFrame:
        """Load data from local storage"""
        filepath = Path(filepath)
        
        if filepath.suffix == '.parquet':
            return pd.read_parquet(filepath)
        else:
            return pd.read_csv(filepath)
