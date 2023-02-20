# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace('.','_')

# COMMAND ----------

from pyspark.sql.functions import *

# Define the file path and database name
dbfs_path = f'/FileStore/{username}/retail_dataset/'
db_name = "bronze_db"

# Define the schema of the CSV file
customer_schema = "customer_id INT, customer_name STRING, age INT, gender STRING, city STRING"
order_schema = "order_id INT, customer_id INT, product_id INT, order_date STRING, quantity INT, price INT, discount INT"
sales_schema = "sale_id INT, product_id INT, sale_date TIMESTAMP, quantity INT,price INT, Discount INT"
product_schema = "product_id INT, product_name STRING, category STRING, price INT"
# Define the options for the autoloader
bronze_options = {
  "mode": "DROPMALFORMED",
  "header": True
}

# COMMAND ----------

def load_data_to_bronze(source_dataset: str, target_path: str, schema: str) -> None:
  
  # Configure the autoloader options
#   options = {
#     "cloudFiles.format": "csv",
#     "cloudFiles.schemaLocation": target_path + "/_checkpoints",
#     "cloudFiles.inferColumnTypes": "true"
#   }

#   options = {
#     "cloudFiles.format": "csv",
#   }

  print(dbfs_path+source_dataset)
  
  # Ingest the data into the bronze layer
  spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", target_path + "/_checkpoints").load(dbfs_path+source_dataset) \
    .writeStream.option("checkpointLocation", target_path + "/_checkpoints") \
    .start(target_path)  
  
#   spark.readStream.format("cloudFiles").options(**options).schema(schema).load(dbfs_path+source_dataset) \
#     .writeStream.format("delta").option("checkpointLocation", target_path + "/_checkpoints") \
#     .start(target_path)

# COMMAND ----------

# Call the load_data_to_bronze function
dataset = dbutils.widgets.text("source_dataset","orders")

dataset = dbutils.widgets.get("source_dataset")

# Set the target location for the delta table
target_path = f"/FileStore/{username}/bronze_db/"

load_data_to_bronze(dataset, target_path + dataset, order_schema)

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/shivam_panicker@databricks_com/bronze_db/orders/_checkpoints/_schemas/0

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/shivam_panicker@databricks_com/retail_dataset/orders

# COMMAND ----------

display(spark.read.format('csv').option("header","true").load(dbfs_path+"orders"))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls target_path + "/_checkpoints"
