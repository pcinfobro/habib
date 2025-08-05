#!/usr/bin/env python3
"""
PowerBI Automation Service
=========================
Main service for PowerBI integration and automation
"""

from flask import Flask, jsonify, request
import os
import pandas as pd
import logging
import json
from datetime import datetime

# Import the main PowerBI service
from powerbi_enterprise_service import PowerBIService

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize PowerBI service
powerbi_service = PowerBIService()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'PowerBI Automation'
    })

@app.route('/process-data', methods=['POST'])
def process_data():
    """Process data and send to PowerBI"""
    try:
        data = request.get_json()
        result = powerbi_service.process_and_send(data)
        return jsonify({'status': 'success', 'result': result})
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/datasets', methods=['GET'])
def list_datasets():
    """List available PowerBI datasets"""
    try:
        datasets = powerbi_service.list_datasets()
        return jsonify({'datasets': datasets})
    except Exception as e:
        logging.error(f"Error listing datasets: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
