"""
Simple test to verify streamlit dashboard imports and basic functionality
"""

import sys
import os

# Add scripts to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

print("Testing imports...")

# Test basic imports
try:
    import streamlit as st
    print("✅ Streamlit imported successfully")
except ImportError as e:
    print(f"❌ Streamlit import failed: {e}")

try:
    import pandas as pd
    import plotly.express as px
    print("✅ Pandas and Plotly imported successfully")
except ImportError as e:
    print(f"❌ Pandas/Plotly import failed: {e}")

# Test pipeline module imports
try:
    from scripts.data_processing import DataProcessor
    print("✅ DataProcessor imported successfully")
except ImportError as e:
    print(f"⚠️ DataProcessor import failed: {e}")

try:
    from scripts.hdfs_storage import HDFSManager
    print("✅ HDFSManager imported successfully")
except ImportError as e:
    print(f"⚠️ HDFSManager import failed: {e}")

try:
    from scripts.kafka_streaming import KafkaStreamProcessor
    print("✅ KafkaStreamProcessor imported successfully")
except ImportError as e:
    print(f"⚠️ KafkaStreamProcessor import failed: {e}")

try:
    from scripts.pipeline_orchestrator import PipelineOrchestrator
    print("✅ PipelineOrchestrator imported successfully")
except ImportError as e:
    print(f"⚠️ PipelineOrchestrator import failed: {e}")

# Test basic functionality
try:
    # Test DataProcessor
    from scripts.data_processing import DataProcessor
    processor = DataProcessor()
    print("✅ DataProcessor can be instantiated")
except Exception as e:
    print(f"⚠️ DataProcessor instantiation failed: {e}")

print("\n🎉 Import test completed!")
print("\nIf all ✅ then the dashboard should work.")
print("If there are ⚠️ warnings, those features will be disabled but the dashboard will still run.")
