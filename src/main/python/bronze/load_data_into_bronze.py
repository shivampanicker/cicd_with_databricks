# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace('.','_')

# COMMAND ----------

from pyspark.sql.functions import *

# Define the file path and database name
dbfs_path = f'/FileStore/{username}/retail_dataset/'
db_name = "bronze_db"

# Define the options for the autoloader
bronze_options = {
  "mode": "DROPMALFORMED",
  "header": True
}

# COMMAND ----------

def load_data_to_bronze(source_dataset: str, target_path: str) -> None:

  # Ingest the data into the bronze layer
  spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", target_path + "_checkpoints").load(dbfs_path+source_dataset) \
    .writeStream.option("checkpointLocation", target_path + "_checkpoints") \
    .trigger(once=True)\
    .start(target_path+source_dataset).awaitTermination()

# COMMAND ----------


