# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
user = username[:username.index("@")].replace('.', '_')

# COMMAND ----------

import sys
import os 

sys.path.append(os.path.abspath(f"/Workspace/Repos/{username}/cicd_with_databricks/src/main/python/gold/"))

# COMMAND ----------

from gold_layer_etl import GoldAggregations
from pyspark.sql.functions import col

# COMMAND ----------

silver_db = f"{user}_silver_db_test"

# COMMAND ----------

# Read in the necessary tables
sales_df = spark.read.table(f"{silver_db}.silver_sales")
products_df = spark.read.table(f"{silver_db}.silver_products")
customers_df = spark.read.table(f"{silver_db}.silver_customers")
orders_df = spark.read.table(f"{silver_db}.silver_orders")

# COMMAND ----------

orders_df.createOrReplaceTempView("orders")
products_df.createOrReplaceTempView("products")
customers_df.createOrReplaceTempView("customers")
sales_df.createOrReplaceTempView("sales")

# COMMAND ----------

query1 = GoldAggregations.total_num_orders(spark, "orders")
query2 = GoldAggregations.total_sales_amount_in_usd(spark, "sales")
query3 = GoldAggregations.top_10_best_selling_products(spark, "sales", "products")
query4 = GoldAggregations.num_customers_by_state(spark, "customers")
query5 = GoldAggregations.avg_sales_by_month(spark, "sales")

# COMMAND ----------

assert query1.select("total_orders").collect()[0].total_orders == 1000
assert query2.select("total_sales").collect()[0].total_sales == 2301182.0999999978
assert query3.select("product_id").limit(1).collect()[0].product_id == 401
assert query4.select("total_customers").filter("state = 'Utah'").collect()[0].total_customers == 191
assert query5.select("avg_sales").filter((col('year') == '2022') & (col('month') == '10')).collect()[0].avg_sales == 226.38976744186064

# COMMAND ----------


