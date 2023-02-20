# Databricks notebook source
# MAGIC %run ./load_data_into_bronze

# COMMAND ----------

# Call the load_data_to_bronze function
dataset = dbutils.widgets.text("source_dataset","orders")

dataset = dbutils.widgets.get("source_dataset")

# Set the target location for the delta table
target_path = f"/FileStore/{username}/bronze_db/"

load_data_to_bronze(dataset, target_path)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/shivam_panicker@databricks_com/test_bronze_db/

# COMMAND ----------


