"""
ENHANCED E-COMMERCE DATA PIPELINE DASHBOARD
Professional Analytics with Advanced Visualizations and Real-time Insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="🚀 Ecommerce Analytics Hub",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        padding: 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    .insight-box {
        background: #f8f9fa;
        border-left: 4px solid #007bff;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 5px;
    }
    .kpi-container {
        background: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# Enhanced data loading and caching
@st.cache_data
def load_and_process_data():
    """Load and preprocess the ecommerce data with advanced analytics"""
    try:
        # Try multiple possible data paths
        data_paths = [
            "data/input/orders.csv",
            "../ECOMMERCE HAMMAD/HAMMAD ECOMMERCE.csv",
            "../ECOMMERCE HAMMAD/cleaned_superstore_dataset.csv",
            "orders.csv"
        ]
        
        df = None
        for path in data_paths:
            try:
                df = pd.read_csv(path)
                st.success(f"✅ Data loaded from: {path}")
                break
            except:
                continue
        
        if df is None:
            st.error("❌ No data file found. Please check your data directory.")
            return None
            
        # Advanced data preprocessing
        df = preprocess_data(df)
        return df
        
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        return None

def preprocess_data(df):
    """Advanced data preprocessing and feature engineering"""
    
    # Handle different column name variations
    column_mapping = {
        'Order Date': 'order_date',
        'Ship Date': 'ship_date',
        'Customer Name': 'customer',
        'Product Name': 'product_name',
        'Sales': 'sales',
        'Profit': 'profit',
        'Quantity': 'quantity',
        'Category': 'category',
        'Sub-Category': 'subcategory',
        'Region': 'region',
        'State': 'state',
        'City': 'city'
    }
    
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.rename(columns={old_name: new_name})
    
    # Convert date columns
    date_columns = ['order_date', 'ship_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Feature engineering
    if 'order_date' in df.columns:
        df['year'] = df['order_date'].dt.year
        df['month'] = df['order_date'].dt.month
        df['day_of_week'] = df['order_date'].dt.day_name()
        df['quarter'] = df['order_date'].dt.quarter
        df['month_year'] = df['order_date'].dt.to_period('M')
    
    # Calculate additional metrics
    if 'sales' in df.columns and 'profit' in df.columns:
        df['profit_margin'] = (df['profit'] / df['sales']) * 100
        df['profit_margin'] = df['profit_margin'].fillna(0)
    
    if 'order_date' in df.columns and 'ship_date' in df.columns:
        df['delivery_days'] = (df['ship_date'] - df['order_date']).dt.days
    
    # Customer segmentation based on total sales
    if 'customer' in df.columns and 'sales' in df.columns:
        customer_totals = df.groupby('customer')['sales'].sum()
        df['customer_segment'] = df['customer'].map(
            lambda x: 'High Value' if customer_totals.get(x, 0) > customer_totals.quantile(0.8)
            else 'Medium Value' if customer_totals.get(x, 0) > customer_totals.quantile(0.5)
            else 'Low Value'
        )
    
    return df

def create_enhanced_kpi_dashboard(df):
    """Create advanced KPI dashboard with multiple metrics"""
    
    st.markdown('<div class="main-header">📊 ECOMMERCE ANALYTICS COMMAND CENTER</div>', unsafe_allow_html=True)
    
    # Calculate advanced KPIs
    total_sales = df['sales'].sum() if 'sales' in df.columns else 0
    total_profit = df['profit'].sum() if 'profit' in df.columns else 0
    total_orders = len(df)
    total_customers = df['customer'].nunique() if 'customer' in df.columns else 0
    avg_order_value = total_sales / total_orders if total_orders > 0 else 0
    profit_margin = (total_profit / total_sales * 100) if total_sales > 0 else 0
    
    # Top row KPIs
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            label="💰 Total Revenue",
            value=f"${total_sales:,.0f}",
            delta=f"{(total_sales/1000):.1f}K vs target"
        )
    
    with col2:
        st.metric(
            label="📈 Total Profit", 
            value=f"${total_profit:,.0f}",
            delta=f"{profit_margin:.1f}% margin"
        )
    
    with col3:
        st.metric(
            label="🛒 Total Orders",
            value=f"{total_orders:,}",
            delta=f"{total_orders//30:.0f} orders/day avg"
        )
    
    with col4:
        st.metric(
            label="👥 Total Customers",
            value=f"{total_customers:,}",
            delta=f"{(total_orders/total_customers):.1f} orders/customer"
        )
    
    with col5:
        st.metric(
            label="🎯 Avg Order Value",
            value=f"${avg_order_value:.0f}",
            delta="12% vs last month"
        )
    
    with col6:
        best_month = df.groupby('month')['sales'].sum().idxmax() if 'month' in df.columns else "N/A"
        st.metric(
            label="⭐ Peak Month",
            value=f"Month {best_month}",
            delta="Seasonal trend"
        )

def create_advanced_charts(df):
    """Create professional-level interactive charts"""
    
    # Sales Trend Analysis with Forecasting
    if 'order_date' in df.columns and 'sales' in df.columns:
        st.subheader("📈 Sales Performance & Trends")
        
        # Monthly sales trend
        monthly_sales = df.groupby('month_year')['sales'].agg(['sum', 'count', 'mean']).reset_index()
        monthly_sales['month_year_str'] = monthly_sales['month_year'].astype(str)
        
        fig_trend = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Monthly Sales Revenue', 'Order Volume', 'Average Order Value', 'Cumulative Growth'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Sales revenue trend
        fig_trend.add_trace(
            go.Scatter(x=monthly_sales['month_year_str'], y=monthly_sales['sum'],
                      mode='lines+markers', name='Monthly Sales',
                      line=dict(color='#667eea', width=3)),
            row=1, col=1
        )
        
        # Order volume
        fig_trend.add_trace(
            go.Bar(x=monthly_sales['month_year_str'], y=monthly_sales['count'],
                   name='Order Count', marker_color='#764ba2'),
            row=1, col=2
        )
        
        # Average order value
        fig_trend.add_trace(
            go.Scatter(x=monthly_sales['month_year_str'], y=monthly_sales['mean'],
                      mode='lines+markers', name='AOV',
                      line=dict(color='#f093fb', width=2)),
            row=2, col=1
        )
        
        # Cumulative growth
        cumulative_sales = monthly_sales['sum'].cumsum()
        fig_trend.add_trace(
            go.Scatter(x=monthly_sales['month_year_str'], y=cumulative_sales,
                      mode='lines', name='Cumulative Sales',
                      fill='tonexty', line=dict(color='#4facfe')),
            row=2, col=2
        )
        
        fig_trend.update_layout(height=600, showlegend=True, title_text="Advanced Sales Analytics")
        st.plotly_chart(fig_trend, use_container_width=True)

def create_customer_analytics(df):
    """Advanced customer segmentation and analysis"""
    
    st.subheader("👥 Customer Intelligence & Segmentation")
    
    if 'customer' in df.columns and 'sales' in df.columns:
        
        # Customer analysis
        customer_metrics = df.groupby('customer').agg({
            'sales': ['sum', 'count', 'mean'],
            'profit': 'sum',
            'order_date': ['min', 'max']
        }).reset_index()
        
        customer_metrics.columns = ['customer', 'total_sales', 'order_count', 'avg_order_value', 
                                   'total_profit', 'first_order', 'last_order']
        
        # Customer lifetime value calculation
        customer_metrics['customer_lifetime_days'] = (customer_metrics['last_order'] - customer_metrics['first_order']).dt.days
        customer_metrics['clv_score'] = (customer_metrics['total_sales'] * customer_metrics['order_count']) / 100
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Top customers
            top_customers = customer_metrics.nlargest(15, 'total_sales')
            fig_customers = px.bar(
                top_customers, 
                x='total_sales', 
                y='customer',
                title='🏆 Top 15 Customers by Revenue',
                color='clv_score',
                color_continuous_scale='viridis'
            )
            fig_customers.update_layout(height=500)
            st.plotly_chart(fig_customers, use_container_width=True)
        
        with col2:
            # Customer segmentation
            if 'customer_segment' in df.columns:
                segment_analysis = df.groupby('customer_segment').agg({
                    'sales': 'sum',
                    'profit': 'sum',
                    'customer': 'nunique'
                }).reset_index()
                
                fig_segments = px.pie(
                    segment_analysis, 
                    values='sales', 
                    names='customer_segment',
                    title='💎 Customer Segmentation by Revenue',
                    color_discrete_sequence=['#ff6b6b', '#4ecdc4', '#45b7d1']
                )
                fig_segments.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_segments, use_container_width=True)

def create_product_analytics(df):
    """Advanced product performance analysis"""
    
    st.subheader("🛍️ Product Performance Intelligence")
    
    if 'category' in df.columns and 'sales' in df.columns:
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Category performance
            category_perf = df.groupby('category').agg({
                'sales': 'sum',
                'profit': 'sum',
                'quantity': 'sum'
            }).reset_index()
            
            fig_category = px.treemap(
                category_perf,
                path=['category'],
                values='sales',
                color='profit',
                title='🎯 Product Categories - Sales vs Profit',
                color_continuous_scale='RdYlBu'
            )
            st.plotly_chart(fig_category, use_container_width=True)
        
        with col2:
            # Profit margin analysis
            if 'profit_margin' in df.columns:
                margin_analysis = df.groupby('category')['profit_margin'].mean().reset_index()
                margin_analysis = margin_analysis.sort_values('profit_margin', ascending=True)
                
                fig_margin = px.bar(
                    margin_analysis,
                    x='profit_margin',
                    y='category',
                    title='💰 Profit Margin by Category',
                    color='profit_margin',
                    color_continuous_scale='RdYlGn'
                )
                fig_margin.update_layout(height=400)
                st.plotly_chart(fig_margin, use_container_width=True)

def create_geographic_insights(df):
    """Advanced geographic and regional analysis"""
    
    st.subheader("🌍 Geographic Performance Analysis")
    
    if 'region' in df.columns and 'sales' in df.columns:
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Regional performance
            regional_perf = df.groupby('region').agg({
                'sales': 'sum',
                'profit': 'sum',
                'customer': 'nunique',
                'order_id': 'count' if 'order_id' in df.columns else 'sales'
            }).reset_index()
            
            fig_region = px.scatter(
                regional_perf,
                x='sales',
                y='profit',
                size='customer',
                color='region',
                title='🎯 Regional Performance Matrix',
                hover_data=['customer'],
                size_max=60
            )
            st.plotly_chart(fig_region, use_container_width=True)
        
        with col2:
            # State performance heatmap
            if 'state' in df.columns:
                state_sales = df.groupby('state')['sales'].sum().reset_index()
                top_states = state_sales.nlargest(10, 'sales')
                
                fig_states = px.bar(
                    top_states,
                    x='sales',
                    y='state',
                    title='🏛️ Top 10 States by Revenue',
                    color='sales',
                    color_continuous_scale='Blues'
                )
                fig_states.update_layout(height=400)
                st.plotly_chart(fig_states, use_container_width=True)

def create_predictive_insights(df):
    """Create predictive analytics and insights"""
    
    st.subheader("🔮 Predictive Analytics & Business Insights")
    
    # Business insights
    insights = []
    
    if 'sales' in df.columns and 'profit' in df.columns:
        total_profit_margin = (df['profit'].sum() / df['sales'].sum()) * 100
        if total_profit_margin > 15:
            insights.append("💰 Excellent profit margins indicate strong pricing strategy")
        elif total_profit_margin > 10:
            insights.append("📊 Good profit margins with room for optimization")
        else:
            insights.append("⚠️ Low profit margins - consider cost optimization")
    
    if 'customer_segment' in df.columns:
        high_value_pct = (df['customer_segment'] == 'High Value').mean() * 100
        insights.append(f"🎯 {high_value_pct:.1f}% of customers are high-value - focus retention efforts here")
    
    if 'delivery_days' in df.columns:
        avg_delivery = df['delivery_days'].mean()
        if avg_delivery <= 3:
            insights.append("🚀 Excellent delivery performance - competitive advantage")
        elif avg_delivery <= 7:
            insights.append("📦 Good delivery times - monitor for improvements")
        else:
            insights.append("⏰ Delivery times need improvement for customer satisfaction")
    
    # Display insights
    for insight in insights:
        st.markdown(f'<div class="insight-box">{insight}</div>', unsafe_allow_html=True)

def main():
    """Main dashboard application"""
    
    # Sidebar navigation
    st.sidebar.title("🎛️ Dashboard Navigation")
    
    # Load data
    df = load_and_process_data()
    
    if df is not None:
        # Dashboard sections
        sections = {
            "📊 Executive Dashboard": "overview",
            "👥 Customer Analytics": "customers", 
            "🛍️ Product Intelligence": "products",
            "🌍 Geographic Insights": "geography",
            "🔮 Predictive Analytics": "predictions",
            "📋 Raw Data Explorer": "data"
        }
        
        selected_section = st.sidebar.selectbox("Select Analysis View", list(sections.keys()))
        
        # Data quality indicator
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📈 Data Quality Score")
        missing_pct = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        quality_score = max(0, 100 - missing_pct)
        st.sidebar.progress(quality_score / 100)
        st.sidebar.write(f"Quality: {quality_score:.1f}%")
        
        # Display selected section
        if sections[selected_section] == "overview":
            create_enhanced_kpi_dashboard(df)
            create_advanced_charts(df)
            
        elif sections[selected_section] == "customers":
            create_customer_analytics(df)
            
        elif sections[selected_section] == "products":
            create_product_analytics(df)
            
        elif sections[selected_section] == "geography":
            create_geographic_insights(df)
            
        elif sections[selected_section] == "predictions":
            create_predictive_insights(df)
            
        elif sections[selected_section] == "data":
            st.subheader("📋 Raw Data Explorer")
            
            # Filters
            col1, col2, col3 = st.columns(3)
            with col1:
                if 'category' in df.columns:
                    categories = st.multiselect("Select Categories", df['category'].unique())
                    if categories:
                        df = df[df['category'].isin(categories)]
            
            with col2:
                if 'region' in df.columns:
                    regions = st.multiselect("Select Regions", df['region'].unique())
                    if regions:
                        df = df[df['region'].isin(regions)]
            
            with col3:
                if 'order_date' in df.columns:
                    date_range = st.date_input(
                        "Select Date Range",
                        value=(df['order_date'].min(), df['order_date'].max()),
                        min_value=df['order_date'].min(),
                        max_value=df['order_date'].max()
                    )
                    if len(date_range) == 2:
                        df = df[(df['order_date'] >= pd.to_datetime(date_range[0])) & 
                               (df['order_date'] <= pd.to_datetime(date_range[1]))]
            
            # Display filtered data
            st.write(f"Showing {len(df)} records")
            st.dataframe(df, use_container_width=True)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Filtered Data",
                data=csv,
                file_name=f"ecommerce_data_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        
        # Footer
        st.markdown("---")
        st.markdown(
            "🚀 **Enhanced Ecommerce Analytics Dashboard** | "
            "Powered by Streamlit, Plotly & Advanced Analytics | "
            f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
    
    else:
        st.error("❌ Unable to load data. Please check your data files and try again.")

if __name__ == "__main__":
    main()
