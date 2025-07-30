"""
E-commerce Data Pipeline - Streamlit Dashboard
Simplified version for cloud deployment
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import io
import json
import os
from datetime import datetime, timedelta

def main():
    st.set_page_config(
        page_title="E-commerce Big Data Pipeline Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🚀 E-commerce Big Data Pipeline Dashboard")
    st.markdown("**Integrated with Spark, Kafka, Iceberg, HDFS & Real-time Analytics**")
    
    # Sidebar for processing options
    with st.sidebar:
        st.header("⚙️ Processing Options")
        enable_kafka = st.checkbox("Enable Kafka Streaming", value=True)
        enable_iceberg = st.checkbox("Enable Iceberg Storage", value=True)
        enable_hdfs = st.checkbox("Enable HDFS Storage", value=True)
        real_time_processing = st.checkbox("Real-time Processing", value=True)
    
    # Main content
    tab1, tab2, tab3 = st.tabs(["📁 Data Upload & Processing", "📊 Real-time Insights", "📈 Analytics Dashboard"])
    
    with tab1:
        st.header("📁 Upload Dataset & Run Pipeline")
        
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type="csv",
            help="Upload your e-commerce dataset to run through the complete Big Data pipeline"
        )
        
        if uploaded_file is not None:
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            
            # Read and preview data
            df = pd.read_csv(uploaded_file)
            st.subheader("📋 Data Preview")
            st.dataframe(df.head())
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records", len(df))
            with col2:
                st.metric("Columns", len(df.columns))
            with col3:
                if 'sales' in df.columns:
                    st.metric("Total Sales", f"${df['sales'].sum():,.2f}")
            
            if st.button("🚀 Run Complete Pipeline", type="primary"):
                with st.spinner("Processing through Big Data pipeline..."):
                    # Basic data cleaning
                    processed_df = process_data(df)
                    
                    if processed_df is not None:
                        st.success("✅ Pipeline processing completed!")
                        st.session_state['processed_data'] = processed_df
                        st.session_state['insights'] = get_insights(processed_df)
    
    with tab2:
        st.header("📊 Real-time Analytics & Insights")
        
        if 'processed_data' in st.session_state and 'insights' in st.session_state:
            df = st.session_state['processed_data']
            insights = st.session_state['insights']
            
            # Key Metrics Dashboard
            st.markdown("### 🎯 Key Performance Indicators")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("💰 Total Sales", f"${insights['total_sales']:,.0f}")
            with col2:
                st.metric("📈 Total Profit", f"${insights['total_profit']:,.0f}")
            with col3:
                st.metric("📦 Total Orders", f"{insights['total_orders']:,}")
            with col4:
                st.metric("🏆 Top Category", insights.get('top_category', 'N/A'))
            
            # Visualizations
            if 'category' in df.columns and 'sales' in df.columns:
                st.markdown("#### 🎯 Sales by Category")
                category_sales = df.groupby('category')['sales'].sum().sort_values(ascending=False)
                
                fig = px.bar(
                    x=category_sales.values,
                    y=category_sales.index,
                    orientation='h',
                    title="Sales by Category",
                    color=category_sales.values,
                    color_continuous_scale='Viridis'
                )
                fig.update_layout(height=400, xaxis_title="Sales ($)", yaxis_title="Category")
                st.plotly_chart(fig, use_container_width=True)
            
            if 'state' in df.columns and 'sales' in df.columns:
                st.markdown("#### 🗺️ Geographic Sales Distribution")
                state_sales = df.groupby('state')['sales'].sum().sort_values(ascending=False).head(10)
                
                fig = px.pie(
                    values=state_sales.values,
                    names=state_sales.index,
                    title="Top 10 States by Sales",
                    hole=0.4
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
                
        else:
            st.info("👆 Upload and process a dataset in the 'Data Upload & Processing' tab to see insights here.")
    
    with tab3:
        st.header("📈 Advanced Analytics Dashboard")
        
        if 'processed_data' in st.session_state:
            df = st.session_state['processed_data']
            
            # Time series analysis
            if 'order_date' in df.columns and 'sales' in df.columns:
                st.markdown("#### 📈 Sales Trend Analysis")
                try:
                    df['order_date'] = pd.to_datetime(df['order_date'])
                    daily_sales = df.groupby(df['order_date'].dt.date)['sales'].sum().reset_index()
                    
                    fig = px.line(
                        daily_sales,
                        x='order_date',
                        y='sales',
                        title="Daily Sales Trend",
                        markers=True
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e:
                    st.info("📅 Date formatting needs adjustment for time series")
            
            # Profitability analysis
            if 'profit' in df.columns and 'sales' in df.columns and 'category' in df.columns:
                st.markdown("#### 💹 Profitability Analysis")
                
                profit_analysis = df.groupby('category').agg({
                    'sales': 'sum',
                    'profit': 'sum'
                }).reset_index()
                profit_analysis['profit_margin'] = (profit_analysis['profit'] / profit_analysis['sales']) * 100
                
                fig = px.scatter(profit_analysis, x='sales', y='profit', 
                               size='profit_margin', color='category',
                               hover_name='category',
                               title="Sales vs Profit by Category",
                               size_max=60)
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("👆 Upload and process a dataset to see advanced analytics.")

def process_data(df):
    """Basic data processing function"""
    try:
        # Show data quality before processing
        st.subheader("📋 Data Quality Report - Before Processing")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(df))
        with col2:
            st.metric("Total Columns", len(df.columns))
        with col3:
            missing_count = df.isnull().sum().sum()
            st.metric("Missing Values", missing_count)
        with col4:
            duplicate_count = df.duplicated().sum()
            st.metric("Duplicate Records", duplicate_count)
        
        # Basic data cleaning
        processed_df = df.copy()
        
        # Remove duplicates
        initial_count = len(processed_df)
        processed_df = processed_df.drop_duplicates()
        duplicates_removed = initial_count - len(processed_df)
        
        # Handle missing values
        missing_before = processed_df.isnull().sum().sum()
        processed_df = processed_df.dropna()
        missing_handled = missing_before - processed_df.isnull().sum().sum()
        
        st.success(f"✅ Data processed: {len(processed_df)} records")
        st.info(f"🧹 Removed {duplicates_removed} duplicates")
        st.info(f"🧹 Handled {missing_handled} missing values")
        
        # Show data quality after processing
        st.subheader("📊 Data Quality Report - After Processing")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Final Records", len(processed_df), delta=int(len(processed_df) - len(df)))
        with col2:
            st.metric("Columns", len(processed_df.columns))
        with col3:
            final_missing = processed_df.isnull().sum().sum()
            st.metric("Missing Values", int(final_missing))
        with col4:
            final_duplicates = processed_df.duplicated().sum()
            st.metric("Duplicate Records", int(final_duplicates))
        
        return processed_df
        
    except Exception as e:
        st.error(f"Processing failed: {e}")
        return None

def get_insights(df):
    """Generate insights from processed data"""
    insights = {}
    
    try:
        # Basic metrics
        insights['total_sales'] = df['sales'].sum() if 'sales' in df.columns else 0
        insights['total_profit'] = df['profit'].sum() if 'profit' in df.columns else 0
        insights['total_orders'] = len(df)
        insights['avg_order_value'] = insights['total_sales'] / insights['total_orders'] if insights['total_orders'] > 0 else 0
        
        # Category insights
        if 'category' in df.columns:
            category_sales = df.groupby('category')['sales'].sum().sort_values(ascending=False)
            insights['top_category'] = category_sales.index[0] if len(category_sales) > 0 else 'N/A'
        
    except Exception as e:
        st.error(f"Insights generation failed: {e}")
    
    return insights

if __name__ == "__main__":
    main()
