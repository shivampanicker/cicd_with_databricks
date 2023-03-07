# Databricks notebook source
# MAGIC %run ./generate_retail_data

# COMMAND ----------

generate_orders_data(10000)

# COMMAND ----------

generate_sales_data(10000)

# COMMAND ----------

generate_product_data(10000)

# COMMAND ----------

generate_customer_data_day_0(10000)

# COMMAND ----------

# MAGIC %run ./create_ddl

# COMMAND ----------
