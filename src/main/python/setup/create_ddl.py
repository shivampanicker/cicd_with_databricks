# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace('.','_')
user = username[:username.index("@")]

# COMMAND ----------

spark.sql(
  f"""CREATE DATABASE IF NOT EXISTS {user}_bronze_db
  LOCATION '/FileStore/{username}_bronze_db/'
""")

# COMMAND ----------

spark.sql(
  f"""CREATE DATABASE IF NOT EXISTS {user}_silver_db
  LOCATION '/FileStore/{username}_silver_db/'
""")

# COMMAND ----------

spark.sql(
  f"""CREATE DATABASE IF NOT EXISTS {user}_gold_db
  LOCATION '/FileStore/{username}_gold_db/'
""")

# COMMAND ----------

spark.sql(
  f"""CREATE TABLE IF NOT EXISTS {user}_bronze_db.bronze_orders
  (
  order_id STRING,
  customer_id STRING,
  order_date STRING,
  order_status STRING,
  _rescued_data STRING
  )
""")

# COMMAND ----------

spark.sql(
  f"""CREATE TABLE IF NOT EXISTS {user}_bronze_db.bronze_sales
  (
  sale_id STRING,
  product_id STRING,
  sale_date STRING,
  sale_amount STRING,
  currency STRING,
  ingest_timestamp STRING,
  _rescued_data STRING
  )
""")

# COMMAND ----------

spark.sql(
  f"""CREATE TABLE IF NOT EXISTS {user}_bronze_db.bronze_products
  (
  product_id STRING,
  product_category STRING,
  product_start_date STRING,
  _rescued_data STRING
  )
""")

# COMMAND ----------

spark.sql(
  f"""CREATE TABLE IF NOT EXISTS {user}_bronze_db.bronze_customers
  (
  customer_id STRING,
  customer_name STRING,
  state STRING,
  company STRING,
  phone_number STRING,
  start_date STRING,
  _rescued_data STRING
  )
  TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

spark.sql(
  f"""CREATE TABLE IF NOT EXISTS {user}_silver_db.silver_orders
  (
  order_id STRING,
  customer_id INT,
  order_date TIMESTAMP,
  order_status STRING
  )
""")

# COMMAND ----------

spark.sql(
  f"""CREATE TABLE IF NOT EXISTS {user}_silver_db.silver_sales
  (
  sale_id STRING,
  product_id INT,
  sale_date DATE,
  sale_amount DOUBLE,
  currency STRING,
  ingest_timestamp TIMESTAMP
  )
""")

# COMMAND ----------

spark.sql(
  f"""CREATE TABLE IF NOT EXISTS {user}_silver_db.silver_products
  (
  product_id INT,
  product_category STRING,
  product_start_date TIMESTAMP
  )
""")

# COMMAND ----------

spark.sql(
  f"""CREATE TABLE IF NOT EXISTS {user}_silver_db.silver_customers
  (
  customer_id INT,
  customer_name STRING,
  state STRING,
  company STRING,
  phone_number STRING,
  start_date TIMESTAMP,
  end_date TIMESTAMP
  )
""")

# COMMAND ----------

spark.sql(
  f"""CREATE TABLE IF NOT EXISTS {user}_gold_db.gold_agg
""")
