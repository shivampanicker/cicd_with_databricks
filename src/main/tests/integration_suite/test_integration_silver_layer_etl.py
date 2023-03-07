# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")].replace('.', '_')

# COMMAND ----------

bronze_path = f"/FileStore/{username}_bronze_db_test/bronze_customers"

# COMMAND ----------

# MAGIC %run ../../python/silver/transform_to_scd2

# COMMAND ----------

# Test the silver layer
customer_df = spark.read.format("delta").option("header", True).load(bronze_path)

spark.sql(f"drop table if exists {user}_silver_db_test.silver_customers")
# Call the function to transform the customer data into a dimension table with SCD type 2
transform_to_scd2(customer_df, mode="test")

silver_df = spark.read.table(f"{user}_silver_db_test.silver_customers")

assert silver_df.count() == 10000  # Assumes that there are 10000 rows in the original data source
assert silver_df.columns == ['customer_id', 'customer_name', 'state', 'company', 'phone_number', 'start_date', 'end_date']

# COMMAND ----------


