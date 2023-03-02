# Databricks notebook source
pip install pytest

# COMMAND ----------

import pytest

# COMMAND ----------

abs_path = "/Repos/shivam.panicker@databricks.com/cicd_with_databricks/src/main/tests"

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")]

# COMMAND ----------

bronze_path = f'/FileStore/{username}_bronze_db_test/bronze_customers'
silver_path = f'/FileStore/{username}_silver_db_test/silver_customers'
gold_path = f'/FileStore/{username}_silver_db_test/gold_customers'

# COMMAND ----------

source_dataset = 'customers'
target_path = f'/FileStore/{username}_bronze_db_test/'

# COMMAND ----------

# MAGIC %run ../src/main/python/bronze/load_data_into_bronze

# COMMAND ----------

# MAGIC %run ../src/main/python/silver/transform_to_scd2

# COMMAND ----------

# Load the data into the bronze layer
#dbutils.notebook.run(abs_path+"/../python/silver/transform_to_scd2", 600, {})
#dbutils.notebook.run(abs_path+"/../python/bronze/load_data_into_bronze", 600, {})

# Test the bronze layer
dbutils.fs.rm(bronze_path, True)
load_data_to_bronze(source_dataset, target_path)

bronze_df = spark.read.format("delta").load(bronze_path+"bronze_customers")
assert bronze_df.count() == 10000  # Assumes that there are 10000 rows in the original data source

# Test the silver layer
customer_df = spark.read.format("delta").option("header", True).load(input_path)

spark.sql(f"drop table if exists {user}_silver_db_test.silver_customers")
# Call the function to transform the customer data into a dimension table with SCD type 2
transform_to_scd2(customer_df, mode="test")

silver_df = spark.read.format("delta").load(silver_path)

assert silver_df.count() == 10000  # Assumes that there are 10000 rows in the original data source
assert silver_df.columns == ["customer_id", "customer_name", "state", "company", "phone_numer", "start_date", "end_date"]
# Assumes that the standardization logic adds the "currency" column to the data

# # Test the gold layer
# dbutils.notebook.run("gold_layer_etl", timeout_seconds=600)
# gold_df = spark.read.format("delta").load("/mnt/gold")
# assert gold_df.count() == 100  # Assumes that the aggregation logic groups the data by customer_id
# assert gold_df.columns == ["customer_id", "total_orders", "total_quantity", "total_revenue"]
# # Assumes that the aggregation logic calculates the total number of orders, total quantity, and total revenue for each customer

# Print success message
print("All integration tests passed!")

# COMMAND ----------

dbutils.notebook.run(abs_path+"/cleanup_tests", 300, {})
