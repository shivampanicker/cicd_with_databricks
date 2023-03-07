# Databricks notebook source
from pyspark.sql.functions import *

username = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
    .replace(".", "_")
)
user = username[: username.index("@")]

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------


dbfs_path = f"/FileStore/{username}/retail_dataset/" + env + "/"
bronze_db = f"{user}_bronze_db"
# Define the options for the autoloader
bronze_options = {"mode": "DROPMALFORMED", "header": True}

# COMMAND ----------


def load_data_to_bronze(source_dataset: str, target_path: str, env: str) -> None:
    # Ingest the data into the bronze layer
    spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option(
        "cloudFiles.schemaLocation", target_path + "_checkpoints/" + source_dataset
    ).option("header", "true").load(dbfs_path + source_dataset).writeStream.option(
        "checkpointLocation", target_path + "_checkpoints/" + source_dataset
    ).trigger(
        once=True
    ).start(
        target_path + "bronze_" + source_dataset
    ).awaitTermination()
