#!/usr/bin/env python3
"""
Enterprise PowerBI Automation Service
=====================================
Complete automated pipeline for PowerBI dashboard creation and refresh
"""

import os
import json
import time
import logging
import schedule
import pandas as pd
import requests
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text
import psycopg2
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/data/powerbi_automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PowerBIAutomationService:
    """Complete PowerBI automation service with enterprise features"""
    
    def __init__(self):
        self.app = Flask(__name__)
        self.setup_routes()
        
        # Database connection
        self.db_url = os.getenv('DATABASE_URL', 'postgresql://powerbi:powerbi123@postgres-powerbi:5432/powerbi')
        self.engine = create_engine(self.db_url)
        
        # PowerBI credentials
        self.client_id = os.getenv('POWERBI_CLIENT_ID')
        self.client_secret = os.getenv('POWERBI_CLIENT_SECRET')
        self.tenant_id = os.getenv('POWERBI_TENANT_ID')
        
        # Initialize database
        self.init_database()
        
        # Schedule automated tasks
        self.schedule_tasks()
        
        logger.info("PowerBI Automation Service initialized successfully")

    def init_database(self):
        """Initialize PowerBI database with required tables"""
        try:
            with self.engine.connect() as conn:
                # Create datasets table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS datasets (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        source_type VARCHAR(100) NOT NULL,
                        source_path TEXT NOT NULL,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        record_count INTEGER DEFAULT 0,
                        status VARCHAR(50) DEFAULT 'active',
                        metadata JSONB
                    )
                """))
                
                # Create dashboards table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS dashboards (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        powerbi_id VARCHAR(255),
                        dataset_id INTEGER REFERENCES datasets(id),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_refreshed TIMESTAMP,
                        refresh_status VARCHAR(50) DEFAULT 'pending',
                        dashboard_config JSONB
                    )
                """))
                
                # Create refresh_logs table
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS refresh_logs (
                        id SERIAL PRIMARY KEY,
                        dashboard_id INTEGER REFERENCES dashboards(id),
                        refresh_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status VARCHAR(50) NOT NULL,
                        duration_seconds INTEGER,
                        error_message TEXT,
                        records_processed INTEGER
                    )
                """))
                
                conn.commit()
                logger.info("Database tables initialized successfully")
                
        except Exception as e:
            logger.error(f"Database initialization failed: {str(e)}")

    def setup_routes(self):
        """Setup Flask API routes"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'service': 'PowerBI Automation Service'
            })
        
        @self.app.route('/datasets', methods=['GET'])
        def get_datasets():
            return jsonify(self.get_all_datasets())
        
        @self.app.route('/datasets', methods=['POST'])
        def create_dataset():
            data = request.json
            result = self.register_dataset(
                name=data['name'],
                source_type=data['source_type'],
                source_path=data['source_path'],
                metadata=data.get('metadata', {})
            )
            return jsonify(result)
        
        @self.app.route('/dashboards', methods=['GET'])
        def get_dashboards():
            return jsonify(self.get_all_dashboards())
        
        @self.app.route('/dashboards', methods=['POST'])
        def create_dashboard():
            data = request.json
            result = self.create_powerbi_dashboard(
                name=data['name'],
                dataset_id=data['dataset_id'],
                config=data.get('config', {})
            )
            return jsonify(result)
        
        @self.app.route('/refresh/<int:dashboard_id>', methods=['POST'])
        def refresh_dashboard(dashboard_id):
            result = self.refresh_powerbi_dashboard(dashboard_id)
            return jsonify(result)
        
        @self.app.route('/refresh/all', methods=['POST'])
        def refresh_all_dashboards():
            result = self.refresh_all_dashboards()
            return jsonify(result)

    def register_dataset(self, name: str, source_type: str, source_path: str, metadata: Dict = None) -> Dict:
        """Register a new dataset for PowerBI integration"""
        try:
            # Analyze dataset
            if source_type == 'csv':
                df = pd.read_csv(source_path)
                record_count = len(df)
                
                # Generate metadata
                if not metadata:
                    metadata = {
                        'columns': list(df.columns),
                        'dtypes': df.dtypes.astype(str).to_dict(),
                        'sample_data': df.head(3).to_dict('records'),
                        'null_counts': df.isnull().sum().to_dict(),
                        'analyzed_at': datetime.now().isoformat()
                    }
            
            # Insert into database
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    INSERT INTO datasets (name, source_type, source_path, record_count, metadata)
                    VALUES (:name, :source_type, :source_path, :record_count, :metadata)
                    RETURNING id
                """), {
                    'name': name,
                    'source_type': source_type,
                    'source_path': source_path,
                    'record_count': record_count,
                    'metadata': json.dumps(metadata)
                })
                
                dataset_id = result.fetchone()[0]
                conn.commit()
                
                logger.info(f"Dataset '{name}' registered successfully with ID: {dataset_id}")
                
                return {
                    'success': True,
                    'dataset_id': dataset_id,
                    'record_count': record_count,
                    'metadata': metadata
                }
                
        except Exception as e:
            logger.error(f"Failed to register dataset '{name}': {str(e)}")
            return {'success': False, 'error': str(e)}

    def export_powerbi_dataset(self, dataset_id: int, dashboard_id: int):
        """Export dataset in PowerBI compatible format"""
        try:
            with self.engine.connect() as conn:
                # Get dataset info
                result = conn.execute(text("""
                    SELECT name, source_path, source_type FROM datasets WHERE id = :dataset_id
                """), {'dataset_id': dataset_id})
                
                dataset_info = result.fetchone()
                if not dataset_info:
                    return False
                
                name, source_path, source_type = dataset_info
                
                # Read and process data
                if source_type == 'csv':
                    df = pd.read_csv(source_path)
                else:
                    logger.error(f"Unsupported source type: {source_type}")
                    return False
                
                # Create PowerBI compatible table
                table_name = f'processed_data_{name.lower().replace(" ", "_")}'
                
                # Clean column names for PowerBI
                df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') 
                             for col in df.columns]
                
                # Export to database
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                
                # Also export as CSV for direct PowerBI import
                export_path = f'/app/data/powerbi_{name.lower().replace(" ", "_")}.csv'
                df.to_csv(export_path, index=False)
                
                logger.info(f"Dataset exported successfully: {export_path}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to export dataset {dataset_id}: {str(e)}")
            return False

    def run(self, host='0.0.0.0', port=5000, debug=False):
        """Run the PowerBI automation service"""
        logger.info(f"Starting PowerBI Automation Service on {host}:{port}")
        self.app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    service = PowerBIAutomationService()
    service.run(debug=True)
