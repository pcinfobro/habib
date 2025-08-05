#!/usr/bin/env python3
"""
Simple PowerBI API Demo - Working Version
"""

from flask import Flask, jsonify, request
import json
import datetime

app = Flask(__name__)

# Mock PowerBI status
powerbi_status = {
    "service": "PowerBI Automation API",
    "status": "running",
    "last_updated": datetime.datetime.now().isoformat(),
    "datasets": [
        {"id": "dataset-001", "name": "Ecommerce Sales", "status": "active"},
        {"id": "dataset-002", "name": "Customer Analytics", "status": "refreshing"}
    ],
    "dashboards": [
        {"id": "dashboard-001", "name": "Sales Overview", "views": 1245},
        {"id": "dashboard-002", "name": "Customer Insights", "views": 892}
    ]
}

@app.route('/')
def home():
    return jsonify({
        "message": "PowerBI Automation Service - LIVE",
        "endpoints": [
            "/status - Service status",
            "/datasets - List datasets", 
            "/dashboards - List dashboards",
            "/refresh/<dataset_id> - Refresh dataset"
        ]
    })

@app.route('/status')
def status():
    return jsonify(powerbi_status)

@app.route('/datasets')
def datasets():
    return jsonify(powerbi_status["datasets"])

@app.route('/dashboards') 
def dashboards():
    return jsonify(powerbi_status["dashboards"])

@app.route('/refresh/<dataset_id>', methods=['POST'])
def refresh_dataset(dataset_id):
    return jsonify({
        "message": f"Refresh triggered for dataset {dataset_id}",
        "status": "initiated",
        "timestamp": datetime.datetime.now().isoformat()
    })

if __name__ == '__main__':
    print("🚀 Starting PowerBI API Service on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
