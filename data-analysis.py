import pandas as pd
import matplotlib.pyplot as plt




"""
Objective: Read a tabular data file and perform basic statistical analysis on its columns.

Tasks:

Read the CSV file using pandas
Display the first 10 rows of the dataset
Generate summary statistics for all numerical columns (count, mean, std, min, max)
Calculate the total revenue (quantity * unit_price) for each transaction
Find the average revenue per store
Determine the most frequently purchased product
Create a histogram of customer ages
Calculate correlation between quantity purchased and customer age
"""



# Read the data
df = pd.read_csv('sales_data.csv')

# Display first 10 rows
print(df.head(10))

# Generate summary statistics for numerical columns
print(df.describe())

# Calculate total revenue
df['revenue'] = df['quantity'] * df['unit_price']

# Continue with the remaining tasks...

#Average revenue per store
avg_revenue = df.groupby('store_id')['revenue'].mean()
print("\nAverage Revenue per Store:")
print(avg_revenue)

# Most frequently purchased product
top_product = df.groupby('product_id')['revenue'].count().sort_values(ascending=False).head(1)
print("\nTop Product:")
print(top_product)

#Create a histogram of customer ages
plt.figure(figsize=(10, 6))
plt.hist(df['customer_age'], bins=20, color='blue', alpha=0.7)
plt.title('Histogram of Customer Ages')
plt.xlabel('Age')
plt.ylabel('Frequency')
plt.grid(axis='y', alpha=0.75)
plt.savefig('customer_age_histogram.png')
plt.close()
plt.show()

# Calculate correlation between quantity purchased and customer age
correlation = df['quantity'].corr(df['customer_age'])
print("\nCorrelation between Quantity Purchased and Customer Age:")
print(correlation)

# Save the modified DataFrame to a new CSV file
df.to_csv('sales_data_with_revenue.csv', index=False)
