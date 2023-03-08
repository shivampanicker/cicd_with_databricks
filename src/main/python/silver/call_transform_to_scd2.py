# Databricks notebook source
pip install faker

# COMMAND ----------

username = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
    .replace(".", "_")
)

# COMMAND ----------

input_path = f"/FileStore/{username}_bronze_db/"

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %run ../silver/transform_to_scd2

# COMMAND ----------

source_dataset_df = spark.read.format("delta").load(input_path + "bronze_customers")
transform_to_scd2(source_dataset_df, "prod")

# COMMAND ----------

# MAGIC %run ../setup/generate_retail_data

# COMMAND ----------

generate_customer_data_day_2(env)

# COMMAND ----------

# MAGIC %run ../bronze/load_data_into_bronze $env=env

# COMMAND ----------

# Set the target location for the delta table
target_path = f"/FileStore/{username}_bronze_db/"

load_data_to_bronze("customers", target_path, env)

# COMMAND ----------

input_path = f"/FileStore/{username}_bronze_db/"

# COMMAND ----------

# MAGIC %run ../silver/transform_to_scd2

# COMMAND ----------

source_dataset_df = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .load(input_path + "bronze_customers")
)

transform_to_scd2(source_dataset_df, "prod")
