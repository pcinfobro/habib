"""
Local development configuration
Simplified config for running on local machine without Hadoop/Kafka
"""

import os
from pathlib import Path

# Create local directories
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
LOGS_DIR = DATA_DIR / "logs"

# Create directories if they don't exist
for directory in [DATA_DIR, INPUT_DIR, OUTPUT_DIR, LOGS_DIR]:
    directory.mkdir(exist_ok=True)

LOCAL_CONFIG = {
    "data_sources": {
        "orders": {
            "type": "csv",
            "config": {
                "file_path": str(INPUT_DIR / "orders.csv"),
                "encoding": "utf-8",
                "separator": ",",
                "na_values": ["", "NULL", "null", "N/A"]
            }
        },
        "customers": {
            "type": "csv",  # Changed from API to CSV for local testing
            "config": {
                "file_path": str(INPUT_DIR / "customers.csv"),
                "encoding": "utf-8"
            }
        },
        "products": {
            "type": "json",
            "config": {
                "file_path": str(INPUT_DIR / "products.json")
            }
        }
    },
    "processing": {
        "duplicate_subset": ["customer_id", "order_id"],
        "missing_values": {
            "customer_name": "Unknown",
            "order_amount": "mean",
            "order_date": "drop",
            "product_category": "mode"
        },
        "data_types": {
            "order_date": "datetime",
            "order_amount": "numeric",
            "customer_id": "string"
        },
        "standardization": {
            "email_columns": ["customer_email"],
            "text_columns": ["customer_name", "product_name"]
        },
        "derived_columns": {
            "add_processing_timestamp": True,
            "data_source": "local_pipeline"
        }
    },
    "storage": {
        "local": {
            "output_dir": str(OUTPUT_DIR),
            "format": "parquet"  # or "csv"
        }
    }
}
