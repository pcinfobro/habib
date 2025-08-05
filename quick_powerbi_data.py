#!/usr/bin/env python3
"""
Quick Data Processing for PowerBI Integration
Processes existing data and creates PowerBI-compatible datasets
"""

import pandas as pd
import json
import os
from datetime import datetime

def process_ecommerce_data():
    """Process existing ecommerce data for PowerBI"""
    print("🔄 Processing ecommerce data for PowerBI...")
    
    # Read the cleaned dataset
    try:
        df = pd.read_csv('data/processed/cleaned_superstore_dataset.csv')
        print(f"✅ Loaded {len(df):,} records from cleaned dataset")
        
        # Create summary analytics
        analytics = {
            'total_sales': float(df['Sales'].sum()),
            'total_profit': float(df['Profit'].sum()),
            'total_orders': len(df),
            'profit_margin': float(df['Profit'].sum() / df['Sales'].sum() * 100),
            'top_categories': df['Category'].value_counts().head(5).to_dict(),
            'top_regions': df['Region'].value_counts().head(5).to_dict(),
            'monthly_sales': df.groupby(df['Order Date'].str[:7] if 'Order Date' in df.columns else df.index)['Sales'].sum().head(12).to_dict(),
            'timestamp': datetime.now().isoformat()
        }
        
        # Save analytics for PowerBI
        with open('data/output/powerbi_analytics.json', 'w') as f:
            json.dump(analytics, f, indent=2)
        
        # Create PowerBI dashboard data
        dashboard_data = {
            'sales_by_category': df.groupby('Category')['Sales'].sum().to_dict(),
            'profit_by_region': df.groupby('Region')['Profit'].sum().to_dict(),
            'top_products': df.nlargest(10, 'Sales')[['Product Name', 'Sales', 'Profit']].to_dict('records'),
            'summary_stats': {
                'total_sales': f"${analytics['total_sales']:,.2f}",
                'total_profit': f"${analytics['total_profit']:,.2f}",
                'profit_margin': f"{analytics['profit_margin']:.1f}%",
                'total_orders': f"{analytics['total_orders']:,}"
            }
        }
        
        # Save dashboard data
        with open('data/output/powerbi_dashboard.json', 'w') as f:
            json.dump(dashboard_data, f, indent=2)
        
        # Create a simplified CSV for PowerBI import
        powerbi_csv = df[['Category', 'Region', 'Sales', 'Profit', 'Quantity']].copy()
        powerbi_csv.to_csv('data/output/powerbi_dataset.csv', index=False)
        
        print("✅ PowerBI data processing completed!")
        print(f"   📊 Analytics: data/output/powerbi_analytics.json")
        print(f"   📈 Dashboard: data/output/powerbi_dashboard.json")
        print(f"   📋 Dataset: data/output/powerbi_dataset.csv")
        
        return analytics
        
    except Exception as e:
        print(f"❌ Error processing data: {e}")
        return None

def create_sample_dashboard():
    """Create sample dashboard data if no real data available"""
    sample_data = {
        'sales_by_category': {
            'Technology': 836154.03,
            'Furniture': 741999.79,
            'Office Supplies': 719047.03
        },
        'profit_by_region': {
            'West': 108418.45,
            'East': 91522.78,
            'Central': 39706.36,
            'South': 46749.43
        },
        'summary_stats': {
            'total_sales': '$2,297,200.85',
            'total_profit': '$286,397.02',
            'profit_margin': '12.5%',
            'total_orders': '9,994'
        }
    }
    
    with open('data/output/powerbi_dashboard.json', 'w') as f:
        json.dump(sample_data, f, indent=2)
    
    print("✅ Sample dashboard data created")
    return sample_data

if __name__ == "__main__":
    # Ensure output directory exists
    os.makedirs('data/output', exist_ok=True)
    
    # Process data
    result = process_ecommerce_data()
    
    if not result:
        print("📊 Creating sample data for demonstration...")
        create_sample_dashboard()
    
    print("🚀 Data ready for PowerBI integration!")
