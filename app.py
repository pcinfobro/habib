"""
E-commerce Data Pipeline - Minimal Streamlit Dashboard
Ultra-simplified version for reliable cloud deployment
"""

import streamlit as st
import pandas as pd

def main():
    st.set_page_config(
        page_title="E-commerce Pipeline Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("🚀 E-commerce Data Pipeline Dashboard")
    st.markdown("**Big Data Analytics Platform**")
    
    # Main content
    tab1, tab2 = st.tabs(["📁 Data Upload", "📊 Analytics"])
    
    with tab1:
        st.header("📁 Upload Your Dataset")
        
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type="csv",
            help="Upload your e-commerce dataset"
        )
        
        if uploaded_file is not None:
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            
            # Read and preview data
            df = pd.read_csv(uploaded_file)
            st.subheader("📋 Data Preview")
            st.dataframe(df.head(10))
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records", len(df))
            with col2:
                st.metric("Columns", len(df.columns))
            with col3:
                if 'sales' in df.columns:
                    st.metric("Total Sales", f"${df['sales'].sum():,.2f}")
                elif len(df.select_dtypes(include=['number']).columns) > 0:
                    numeric_col = df.select_dtypes(include=['number']).columns[0]
                    st.metric(f"Total {numeric_col}", f"{df[numeric_col].sum():,.2f}")
            
            if st.button("🚀 Process Data", type="primary"):
                with st.spinner("Processing data..."):
                    # Basic data cleaning
                    processed_df = process_data(df)
                    
                    if processed_df is not None:
                        st.success("✅ Data processing completed!")
                        st.session_state['processed_data'] = processed_df
    
    with tab2:
        st.header("📊 Data Analytics")
        
        if 'processed_data' in st.session_state:
            df = st.session_state['processed_data']
            
            st.markdown("### 📈 Data Overview")
            
            # Show basic statistics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Records", len(df))
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                numeric_cols = df.select_dtypes(include=['number']).columns
                st.metric("Numeric Columns", len(numeric_cols))
            with col4:
                missing_values = df.isnull().sum().sum()
                st.metric("Missing Values", missing_values)
            
            # Data summary
            st.subheader("📊 Data Summary")
            st.dataframe(df.describe())
            
            # Sample data
            st.subheader("🔍 Sample Data")
            st.dataframe(df.head(20))
            
            # Column analysis
            st.subheader("📋 Column Information")
            col_info = pd.DataFrame({
                'Column': df.columns,
                'Type': df.dtypes,
                'Non-Null Count': df.count(),
                'Null Count': df.isnull().sum()
            })
            st.dataframe(col_info)
            
        else:
            st.info("👆 Upload and process a dataset in the 'Data Upload' tab to see analytics here.")

def process_data(df):
    """Basic data processing function"""
    try:
        # Show original data info
        st.info(f"Original data: {len(df)} records, {len(df.columns)} columns")
        
        # Basic cleaning
        processed_df = df.copy()
        
        # Remove duplicates
        initial_count = len(processed_df)
        processed_df = processed_df.drop_duplicates()
        duplicates_removed = initial_count - len(processed_df)
        
        # Handle missing values (simple approach)
        missing_before = processed_df.isnull().sum().sum()
        # Drop rows with too many missing values
        processed_df = processed_df.dropna(thresh=len(processed_df.columns)*0.5)
        missing_after = processed_df.isnull().sum().sum()
        missing_handled = missing_before - missing_after
        
        st.success(f"✅ Processed {len(processed_df)} records")
        if duplicates_removed > 0:
            st.info(f"🧹 Removed {duplicates_removed} duplicate records")
        if missing_handled > 0:
            st.info(f"🧹 Handled {missing_handled} missing values")
        
        return processed_df
        
    except Exception as e:
        st.error(f"Processing failed: {e}")
        return df

if __name__ == "__main__":
    main()
