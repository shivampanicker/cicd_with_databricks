# Databricks notebook source
# Load the data into the bronze layer
dbutils.notebook.run("bronze_layer_etl", timeout_seconds=600)

# Test the bronze layer
bronze_df = spark.read.format("delta").load("/mnt/bronze")
assert bronze_df.count() == 10000  # Assumes that there are 10000 rows in the original data source

# Test the silver layer
dbutils.notebook.run("silver_layer_etl", timeout_seconds=600)
silver_df = spark.read.format("delta").load("/mnt/silver")
assert silver_df.count() == 10000  # Assumes that there are 10000 rows in the original data source
assert silver_df.columns == ["customer_id", "order_id", "order_date", "product_id", "quantity", "price", "currency"]
# Assumes that the standardization logic adds the "currency" column to the data

# Test the gold layer
dbutils.notebook.run("gold_layer_etl", timeout_seconds=600)
gold_df = spark.read.format("delta").load("/mnt/gold")
assert gold_df.count() == 100  # Assumes that the aggregation logic groups the data by customer_id
assert gold_df.columns == ["customer_id", "total_orders", "total_quantity", "total_revenue"]
# Assumes that the aggregation logic calculates the total number of orders, total quantity, and total revenue for each customer

# Print success message
print("All integration tests passed!")
