# Databricks notebook source
database_name = spark.conf.get("com.databricks.training.spark.dbName")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
output_dir = f"/FileStore/{username}/retail_dataset"

# COMMAND ----------

from pyspark.sql.functions import *

# Define the file path and database name
dbfs_path = "/mnt/bronze/"
db_name = "bronze_db"

# Define the schema of the CSV file
customer_schema = "CustomerId INT, FirstName STRING, LastName STRING, Email STRING, Phone STRING, Address STRING, City STRING, State STRING, Zip STRING"
order_schema = "OrderId INT, OrderDate TIMESTAMP, CustomerId INT, Amount DOUBLE, PaymentStatus STRING, FulfillmentStatus STRING, OrderSource STRING"
sales_schema = "SalesId INT, OrderId INT, SalesDate TIMESTAMP, ProductId INT, Quantity INT, Price DOUBLE, Discount DOUBLE"

# Define the options for the autoloader
bronze_options = {
  "mode": "DROPMALFORMED",
  "header": True
}

# COMMAND ----------

def load_data_to_bronze(source_folder: str, bronze_folder: str) -> None:
  
  # Configure the autoloader options
  options = {
    "cloudFiles.useNotifications": "true",
    "cloudFiles.notificationTopicArn": "<your_topic_arn>",
    "cloudFiles.region": "<aws_region>",
    "cloudFiles.format": "csv",
    "cloudFiles.schemaString": schema.json(),
    "cloudFiles.partitionColumns": "order_date",
    "cloudFiles.partitionBy": "order_date"
  }

  # Set the mount point for the raw data
  mount_point = "/mnt/raw/retail_data"

  # Set the path to the raw data files
  path = mount_point + "/sales*.csv"

  # Set the target location for the delta table
  target_path = "/mnt/bronze/sales"

  # Ingest the data into the bronze layer
  spark.readStream.format("cloudFiles").options(**options).load(path) \
    .writeStream.format("delta").option("path", target_path).option("checkpointLocation", target_path + "/_checkpoints") \
    .start()

# Call the load_data_to_bronze function
load_data_to_bronze(dbutils.widgets.get("source_folder"), dbutils.widgets.get("bronze_folder"))
