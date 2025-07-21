import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Set random seed for reproducibility
np.random.seed(42)

# Generate 500 records
n_records = 500

# Create date range for the past 90 days
end_date = datetime.now().date()
start_date = end_date - timedelta(days=90)
date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days)]

# Generate data
data = {
    'date': [random.choice(date_range).strftime('%Y-%m-%d') for _ in range(n_records)],
    'store_id': [f"S{random.randint(1, 10):03d}" for _ in range(n_records)],
    'product_id': [f"P{random.randint(1, 50):03d}" for _ in range(n_records)],
    'quantity': np.random.randint(1, 20, n_records),
    'unit_price': np.round(np.random.uniform(5, 100, n_records), 2),
    'customer_age': np.random.randint(12, 75, n_records)
}

# Create DataFrame
df = pd.DataFrame(data)

# Add some realistic patterns
# More sales on weekends
for i, row in df.iterrows():
    date_obj = datetime.strptime(row['date'], '%Y-%m-%d')
    if date_obj.weekday() >= 5:  # Weekend
        df.at[i, 'quantity'] = min(df.at[i, 'quantity'] * 1.5, 20)

# Popular products sell more
popular_products = [f"P{i:03d}" for i in range(1, 6)]
for i, row in df.iterrows():
    if row['product_id'] in popular_products:
        df.at[i, 'quantity'] = min(df.at[i, 'quantity'] * 1.3, 20)

# Save to CSV
df.to_csv('sales_data.csv', index=False)

print(f"Created sales_data.csv with {n_records} records")
