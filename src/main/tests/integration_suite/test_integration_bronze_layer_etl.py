# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")].replace('.', '_')

# COMMAND ----------

source_dataset = 'customers'
target_path = f'/FileStore/{username}_bronze_db_test/'

# COMMAND ----------

# MAGIC %run ../../python/bronze/load_data_into_bronze

# COMMAND ----------

# Test the bronze layer

# Load the data into the bronze layer
load_data_to_bronze(source_dataset, target_path)

bronze_df = spark.read.format("delta").load(target_path+"bronze_customers")
assert bronze_df.count() == 10000  # Assumes that there are 10000 rows in the original data source
