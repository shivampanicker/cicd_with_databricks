# Databricks notebook source
# MAGIC %run ../setup/cleanup

# COMMAND ----------

# MAGIC %run ../setup/initiate_setup

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %run ./load_data_into_bronze $env=env

# COMMAND ----------

# Call the load_data_to_bronze function

dataset = dbutils.widgets.get("source_dataset")

# Set the target location for the delta table
target_path = f"/FileStore/{username}_bronze_db/"

load_data_to_bronze(dataset, target_path, env)
