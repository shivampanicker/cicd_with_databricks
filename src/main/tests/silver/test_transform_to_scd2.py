# Databricks notebook source
# Import the function from the first notebook
%run "./transform_to_scd2"

# Read in the source data
customer_df = spark.read.format("delta").option("header", True).load("/FileStore/tables/Customer.csv")

# Call the function to transform the customer data into a dimension table with SCD type 2
customer_dim_df = transform_to_scd2(customer_df)

# Define unit tests for the SCD type 2 transformation
def test_scd2_transform():
  # Test that the dimension table has the correct columns
  assert set(customer_dim_df.columns) == set(dim_cols)
  # Test that each customer_id has only one record with the most recent end_date
  assert customer_dim_df.groupBy('customer_id').agg(max('end_date')).filter(col('max(end_date)') != end_date).count() == 0

# Run the unit tests
test_scd2_transform()

