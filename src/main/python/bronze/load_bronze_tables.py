# Databricks notebook source
# MAGIC %run ../setup/initiate_setup

# COMMAND ----------

# MAGIC %run ./load_data_into_bronze

# COMMAND ----------

# Call the load_data_to_bronze function
dataset = dbutils.widgets.text("source_dataset", "customers")

dataset = dbutils.widgets.get("source_dataset")

# Set the target location for the delta table
target_path = f"/FileStore/{username}_bronze_db/"

load_data_to_bronze(dataset, target_path)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/shivam_panicker@databricks_com_bronze_db/bronze_customers

# COMMAND ----------


