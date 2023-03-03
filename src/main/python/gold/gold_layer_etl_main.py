# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")]

# COMMAND ----------

from gold_layer_etl import GoldAggregations

# COMMAND ----------

# Read in the necessary tables
sales_df = spark.read.table(f"{user}_silver_db.silver_sales")
products_df = spark.read.table(f"{user}_silver_db.silver_products")
customers_df = spark.read.table(f"{user}_silver_db.silver_customers")
orders_df = spark.read.table(f"{user}_silver_db.silver_orders")

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

# write the query results to Delta Lake in Gold layer
query1.write.format("delta").mode("overwrite").saveAsTable(f"{user}_gold_db.total_orders")
query2.write.format("delta").mode("overwrite").saveAsTable(f"{user}_gold_db.total_sales")
query3.write.format("delta").mode("overwrite").saveAsTable(f"{user}_gold_db.best_selling_products")
query4.write.format("delta").mode("overwrite").saveAsTable(f"{user}_gold_db.statewise_customers")
query5.write.format("delta").mode("overwrite").saveAsTable(f"{user}_gold_db.monthly_sales")

# COMMAND ----------

display(spark.read.table(f"{user}_gold_db.total_orders"))

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /FileStore/shivam_panicker_bronze_db_test/retail_dataset/customers/

# COMMAND ----------


