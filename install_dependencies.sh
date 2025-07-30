#!/bin/bash

echo "Installing dependencies for local development..."

# Activate virtual environment
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install core dependencies
pip install pandas numpy pyarrow requests python-dateutil click pytest jupyter

# Optional: Install Kafka for streaming (requires Java)
# pip install kafka-python

# Optional: Install Airflow for orchestration testing
# pip install apache-airflow

echo "Dependencies installed successfully!"
echo "To activate the virtual environment, run: source venv/bin/activate"
