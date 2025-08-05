#!/bin/bash
# Start All Services Script for Streamlit Container
# This script starts the Streamlit dashboard

echo "🚀 Starting Streamlit Dashboard..."

# Start Streamlit in the background
streamlit run streamlit_dashboard.py --server.port=8501 --server.address=0.0.0.0 &

# Keep the container running
wait
