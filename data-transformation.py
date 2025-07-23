"""
Tasks:

Create a data processing class with methods for each transformation step
Implement data cleaning:
    Convert date strings to datetime objects
    Handle any missing values
    Remove outliers (e.g., quantities > 100 or unit prices > $1000)
Create time-based features:
    Extract day of week, month, and quarter from the date
    Create a "weekend" flag (True for Saturday/Sunday)
Segment customers into age groups:
    Under 18
    18-25
    26-35
    36-50
    Over 50
Calculate aggregated metrics:
    Daily total sales
    Weekly revenue by store
    Average quantity by product and age group
    Revenue trends by month and quarter
Output the processed data to multiple files based on different aggregation levels
"""

import pandas as pd
import numpy as np
from datetime import datetime

class SalesDataProcessor:
    def __init__(self, filepath):
        self.raw_data = pd.read_csv(filepath)
        self.processed_data = None

    def clean_data(self):
        # Create a copy of the raw data
        df = self.raw_data.copy()

        # Convert date strings to datetime
        df['date'] = pd.to_datetime(df['date'])

        # Handle missing values
        df.fillna(0, inplace=True)
        

        # Remove outliers
        df = df[(df['quantity'] <= 100) & (df['unit_price'] <= 1000)]

        self.processed_data = df
        return self

    def create_time_features(self):
        # Check if data has been cleaned
        if self.processed_data is None:
            self.clean_data()

        # Extract time-based features
        # Extract day of week, month, and quarter
        self.processed_data['day_of_week'] = self.processed_data['date'].dt.dayofweek
        self.processed_data['month'] = self.processed_data['date'].dt.month
        self.processed_data['quarter'] = self.processed_data['date'].dt.quarter
        # Create a "weekend" flag
        self.processed_data['is_weekend'] = self.processed_data['day_of_week'].isin([5, 6])
        # Return the updated instance
        return self

    # Implement other methods...

    # Segment customers into age groups
    def segment_customers(self):
        if self.processed_data is None:
            self.clean_data()

        bins = [0, 18, 25, 35, 50, np.inf]
        labels = ['Under 18', '18-25', '26-35', '36-50', 'Over 50']
        self.processed_data['age_group'] = pd.cut(self.processed_data['customer_age'], bins=bins, labels=labels)

        return self
    
    # Calculate aggregated metrics
    def calculate_metrics(self):   
        if self.processed_data is None:
            self.clean_data()

        # Daily total sales
        self.processed_data['revenue'] = self.processed_data['quantity'] * self.processed_data['unit_price']
        daily_sales = self.processed_data.groupby('date')['revenue'].sum().reset_index()
        daily_sales.to_csv('daily_sales.csv', index=False)

        # Weekly revenue by store
        weekly_revenue = (
            self.processed_data
            .resample('W-Mon', on='date')[['revenue']]
            .sum()
            .reset_index()
            .merge(self.processed_data[['date', 'store_id']], on='date')
            .groupby(['store_id', 'date'])['revenue']
            .sum()
            .reset_index()
        )
        weekly_revenue.to_csv('weekly_revenue_by_store.csv', index=False)

        # Average quantity by product and age group
        avg_quantity = self.processed_data.groupby(['product_id', 'age_group'])['quantity'].mean().reset_index()
        avg_quantity.to_csv('avg_quantity_by_product_and_age_group.csv', index=False)

        # Revenue trends by month and quarter
        monthly_revenue = self.processed_data.resample('ME', on='date')[['revenue']].sum()
        monthly_revenue.to_csv('monthly_revenue_trends.csv')

        quarterly_revenue = self.processed_data.resample('QE', on='date')[['revenue']].sum()
        quarterly_revenue.to_csv('quarterly_revenue_trends.csv')

        return self
    
    # Rename this method
    def get_processed_data(self):
        if self.processed_data is None:
            raise ValueError("Data has not been processed yet. Call process() method first.")
        return self.processed_data

    def process(self):
        # Run the entire pipeline
        return (self.clean_data()
                    .create_time_features()
                    .segment_customers()
                    .calculate_metrics()
                    .get_processed_data())

# Usage
processor = SalesDataProcessor('sales_data.csv')
processed_data = processor.process()