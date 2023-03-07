# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")]

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

source_dataset = 'customers'
target_path = f'/FileStore/{username}_bronze_db/' + env + '/'

# COMMAND ----------

# MAGIC %run ../../python/bronze/load_data_into_bronze $env=env

# COMMAND ----------

# Test the bronze layer

# Load the data into the bronze layer
load_data_to_bronze(source_dataset, target_path)

customers_bronze_df = spark.read.format("delta").load(target_path+"/bronze_customers")
assert customers_bronze_df.count() == 1000  # Assumes that there are 1000 rows in the original data source

# COMMAND ----------

source_dataset = 'products'

# COMMAND ----------

load_data_to_bronze(source_dataset, target_path)

products_bronze_df = spark.read.format("delta").load(target_path+"/bronze_customers")
assert products_bronze_df.count() == 1000  # Assumes that there are 1000 rows in the original data source

# COMMAND ----------

source_dataset = 'orders'

# COMMAND ----------

load_data_to_bronze(source_dataset, target_path)

orders_bronze_df = spark.read.format("delta").load(target_path+"/bronze_customers")
assert orders_bronze_df.count() == 1000  # Assumes that there are 1000 rows in the original data source

# COMMAND ----------

source_dataset = 'sales'

# COMMAND ----------

load_data_to_bronze(source_dataset, target_path)

sales_bronze_df = spark.read.format("delta").load(target_path+"/bronze_customers")
assert sales_bronze_df.count() == 1000  # Assumes that there are 1000 rows in the original data source
