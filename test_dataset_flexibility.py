"""
Dataset Flexibility Test for E-commerce Pipeline Dashboard

This script tests how well the dashboard handles different column structures
"""

import pandas as pd
import numpy as np

# Create test datasets with different column naming conventions

# Dataset 1: Standard E-commerce (current format)
def create_standard_dataset():
    np.random.seed(42)
    data = {
        'order_id': range(1000, 1500),
        'customer': [f'Customer_{i}' for i in range(500)],
        'category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home'], 500),
        'sales': np.random.uniform(50, 1000, 500),
        'profit': np.random.uniform(-50, 200, 500),
        'quantity': np.random.randint(1, 5, 500),
        'order_date': pd.date_range('2024-01-01', periods=500, freq='D'),
        'state': np.random.choice(['CA', 'NY', 'TX', 'FL', 'WA'], 500)
    }
    return pd.DataFrame(data)

# Dataset 2: Retail with different column names
def create_retail_dataset():
    np.random.seed(43)
    data = {
        'transaction_id': range(2000, 2300),
        'client_name': [f'Client_{i}' for i in range(300)],
        'product_type': np.random.choice(['Tech', 'Fashion', 'Food', 'Health'], 300),
        'revenue': np.random.uniform(25, 800, 300),
        'margin': np.random.uniform(-25, 150, 300),
        'units': np.random.randint(1, 8, 300),
        'purchase_date': pd.date_range('2024-02-01', periods=300, freq='D'),
        'region': np.random.choice(['West', 'East', 'North', 'South', 'Central'], 300)
    }
    return pd.DataFrame(data)

# Dataset 3: Business with minimal columns
def create_business_dataset():
    np.random.seed(44)
    data = {
        'id': range(3000, 3200),
        'buyer': [f'Business_{i}' for i in range(200)],
        'segment': np.random.choice(['A', 'B', 'C', 'D'], 200),
        'amount': np.random.uniform(100, 2000, 200),
        'earnings': np.random.uniform(-100, 400, 200),
        'date': pd.date_range('2024-03-01', periods=200, freq='D'),
        'location': np.random.choice(['Urban', 'Suburban', 'Rural'], 200)
    }
    return pd.DataFrame(data)

if __name__ == "__main__":
    # Save test datasets
    datasets = {
        'standard_ecommerce.csv': create_standard_dataset(),
        'retail_data.csv': create_retail_dataset(),
        'business_sales.csv': create_business_dataset()
    }
    
    for filename, df in datasets.items():
        df.to_csv(f'data/input/{filename}', index=False)
        print(f"Created {filename} with {len(df)} rows and columns: {list(df.columns)}")
    
    print("\nDataset flexibility test files created successfully!")
    print("Upload these files to test dashboard compatibility.")
