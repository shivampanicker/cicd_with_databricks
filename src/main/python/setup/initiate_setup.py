# Databricks notebook source
pip install faker

# COMMAND ----------

# MAGIC %run ./generate_retail_data

# COMMAND ----------

num_of_rows = int(dbutils.widgets.get("num_rows"))

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

generate_orders_data(num_of_rows, env)

# COMMAND ----------

generate_sales_data(num_of_rows, env)

# COMMAND ----------

generate_product_data(num_of_rows, env)

# COMMAND ----------

generate_customer_data_day_0(num_of_rows, env)

# COMMAND ----------

# MAGIC %run ./cleanup

# COMMAND ----------

# MAGIC %run ./create_ddl
