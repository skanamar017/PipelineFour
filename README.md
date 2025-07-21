# PipelineFour - Data Engineering Exercises

**don't try this lab until you have been briefed on using pandas**

## Exercise 1: Basic Data Analysis with Pandas

**Objective:** Read a tabular data file and perform basic statistical analysis on its columns.

**Dataset:** `sales_data.csv` (500 lines) with the following columns:
- `date` (string): Transaction date in YYYY-MM-DD format
- `store_id` (string): Store identifier (e.g., "S001", "S002")
- `product_id` (string): Product identifier (e.g., "P123", "P456")
- `quantity` (integer): Number of items sold
- `unit_price` (float): Price per item in dollars
- `customer_age` (integer): Age of the customer

**Tasks:**
1. Read the CSV file using pandas
2. Display the first 10 rows of the dataset
3. Generate summary statistics for all numerical columns (count, mean, std, min, max)
4. Calculate the total revenue (quantity * unit_price) for each transaction
5. Find the average revenue per store
6. Determine the most frequently purchased product
7. Create a histogram of customer ages
8. Calculate correlation between quantity purchased and customer age

**Example starter code:**
```python
import pandas as pd
import matplotlib.pyplot as plt

# Read the data
df = pd.read_csv('sales_data.csv')

# Display first 10 rows
print(df.head(10))

# Generate summary statistics for numerical columns
print(df.describe())

# Calculate total revenue
df['revenue'] = df['quantity'] * df['unit_price']

# Continue with the remaining tasks...
```

## Exercise 2: Data Transformation and Aggregation Pipeline

**Objective:** Build a data processing pipeline that transforms, filters, and aggregates sales data.

**Dataset:** Same `sales_data.csv` from Exercise 1

**Tasks:**
1. Create a data processing class with methods for each transformation step
2. Implement data cleaning:
   - Convert date strings to datetime objects
   - Handle any missing values
   - Remove outliers (e.g., quantities > 100 or unit prices > $1000)
3. Create time-based features:
   - Extract day of week, month, and quarter from the date
   - Create a "weekend" flag (True for Saturday/Sunday)
4. Segment customers into age groups:
   - Under 18
   - 18-25
   - 26-35
   - 36-50
   - Over 50
5. Calculate aggregated metrics:
   - Daily total sales
   - Weekly revenue by store
   - Average quantity by product and age group
   - Revenue trends by month and quarter
6. Output the processed data to multiple files based on different aggregation levels

**Example starter code:**
```python
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
        # ...

        # Remove outliers
        # ...

        self.processed_data = df
        return self

    def create_time_features(self):
        # Check if data has been cleaned
        if self.processed_data is None:
            self.clean_data()

        # Extract time-based features
        # ...

        return self

    # Implement other methods...

    def process(self):
        # Run the entire pipeline
        return (self.clean_data()
                    .create_time_features()
                    .segment_customers()
                    .calculate_metrics()
                    .processed_data)

# Usage
processor = SalesDataProcessor('sales_data.csv')
processed_data = processor.process()
```

## Exercise 3: ETL Pipeline with Incremental Loading and Data Quality Checks

**Objective:** Create a robust ETL pipeline that processes daily sales data incrementally, applies transformations, performs data quality checks, and loads the results into a database.

**Datasets:**
- Initial `sales_history.csv` (500 lines, same structure as before)
- Daily update files: `sales_YYYY-MM-DD.csv` (smaller files with new transactions)

**Tasks:**
1. Design an ETL class with separate extract, transform, and load methods
2. Implement the extract phase:
   - Track which files have been processed using a metadata table or file
   - Read new data files only
   - Support reading from multiple file formats (CSV, JSON, Excel)
3. Implement the transform phase:
   - Apply all transformations from Exercise 2
   - Add data quality checks:
     - Schema validation (correct columns and data types)
     - Range checks for numerical fields
     - Format validation for dates and IDs
     - Duplicate detection
   - Log all data quality issues without failing the pipeline
4. Implement the load phase:
   - Create database tables if they don't exist
   - Support both insert and upsert operations
   - Maintain slowly changing dimensions for store information
   - Create summary tables for faster reporting
5. Add pipeline orchestration:
   - Run the pipeline on a schedule
   - Implement error handling and retries
   - Create logs for monitoring
   - Generate a data quality report after each run

**Example starter code:**
```python
import pandas as pd
import sqlite3
import os
import json
import logging
from datetime import datetime, timedelta

class SalesETL:
    def __init__(self, data_dir, db_path, metadata_path):
        self.data_dir = data_dir
        self.db_path = db_path
        self.metadata_path = metadata_path
        self.conn = None
        self.metadata = self._load_metadata()
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            filename='etl_pipeline.log',
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('SalesETL')

    def _load_metadata(self):
        # Load or create metadata tracking file
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r') as f:
                return json.load(f)
        else:
            return {
                'last_processed_date': None,
                'processed_files': [],
                'last_run_timestamp': None,
                'error_count': 0
            }

    def _save_metadata(self):
        with open(self.metadata_path, 'w') as f:
            json.dump(self.metadata, f, indent=2)

    def connect_db(self):
        self.conn = sqlite3.connect(self.db_path)
        return self.conn

    def extract(self):
        # Find files that haven't been processed yet
        all_files = [f for f in os.listdir(self.data_dir)
                    if f.startswith('sales_') and f.endswith('.csv')]

        new_files = [f for f in all_files
                    if f not in self.metadata['processed_files']]

        if not new_files:
            self.logger.info("No new files to process")
            return None

        # Read and combine all new files
        dfs = []
        for file in new_files:
            file_path = os.path.join(self.data_dir, file)
            try:
                df = pd.read_csv(file_path)
                df['source_file'] = file
                dfs.append(df)
                self.logger.info(f"Extracted data from {file}")
            except Exception as e:
                self.logger.error(f"Error extracting from {file}: {str(e)}")
                self.metadata['error_count'] += 1

        if not dfs:
            return None

        return pd.concat(dfs, ignore_index=True)

    def transform(self, df):
        # Implement transformations and data quality checks
        # ...

        return transformed_df, quality_report

    def load(self, df):
        # Implement database loading logic
        # ...

        return rows_inserted, rows_updated

    def run(self):
        self.logger.info("Starting ETL pipeline")
        self.metadata['last_run_timestamp'] = datetime.now().isoformat()

        # Connect to database
        self.connect_db()

        # Extract
        raw_data = self.extract()
        if raw_data is None:
            self.logger.info("No data to process")
            self._save_metadata()
            return

        # Transform
        transformed_data, quality_report = self.transform(raw_data)

        # Load
        rows_inserted, rows_updated = self.load(transformed_data)

        # Update metadata
        for file in raw_data['source_file'].unique():
            self.metadata['processed_files'].append(file)

        self.metadata['last_processed_date'] = datetime.now().isoformat()
        self._save_metadata()

        self.logger.info(f"ETL complete. Inserted: {rows_inserted}, Updated: {rows_updated}")

        # Generate report
        self._generate_report(quality_report)

        # Close connection
        if self.conn:
            self.conn.close()

# Usage
etl = SalesETL(
    data_dir='./data',
    db_path='./sales.db',
    metadata_path='./etl_metadata.json'
)
etl.run()
```

## Sample Data Generation

Here's a Python script to generate the sample dataset for these exercises:

```python
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
```

This data generation script
will create a realistic sales dataset that students can use for all three exercises.
