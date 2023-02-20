# Databricks notebook source
pip install pytest

# COMMAND ----------

from pyspark.sql.functions import rand, randn
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace('.','_')
output_dir = f'/FileStore/{username}/retail_dataset'

# Define schema for the orders dataset
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("discount", IntegerType(), True)
])

# Define schema for the sales dataset
sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("discount", IntegerType(), True)
])

# Define schema for the products dataset
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", IntegerType(), True)
])

# Define schema for the customers dataset
customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("city", StringType(), True)
])

# Generate orders data
orders_data = [(i, i+1, i+2, f"2021-01-{i % 30 + 1}", i % 5 + 1, 100 + i % 100, i % 10) for i in range(10000)]
orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

# Generate sales data
sales_data = [(i, i % 20 + 1, f"2021-01-{i % 30 + 1}", i % 5 + 1, 100 + i % 100, i % 10) for i in range(10000)]
sales_df = spark.createDataFrame(sales_data, schema=sales_schema)

# Generate products data
products_data = [(i, f"Product {i}", f"Category {i % 5}", 100 + i % 100) for i in range(10000)]
products_df = spark.createDataFrame(products_data, schema=products_schema)

# Generate customers data
customers_data = [(i, f"Customer {i}", 18 + i % 50, "Male" if i % 2 == 0 else "Female", "City " + str(i % 10)) for i in range(10000)]
customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

# Write data to CSV files in DBFS
orders_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir+"/orders")
sales_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir+"/sales")
products_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir+"/products")
customers_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir+"/customers")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/shivam_panicker@databricks_com/retail_dataset/orders

# COMMAND ----------


