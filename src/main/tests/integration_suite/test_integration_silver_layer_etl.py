# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")].replace('.', '_')

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

customers_bronze_path = f"/FileStore/{username}_bronze_db/" + env + "/bronze_customers"
orders_bronze_path = f"/FileStore/{username}_bronze_db/" + env + "/bronze_orders"
sales_bronze_path = f"/FileStore/{username}_bronze_db/" + env + "/bronze_sales"
products_bronze_path = f"/FileStore/{username}_bronze_db/" + env + "/bronze_products"

# COMMAND ----------

output_db = f"{user}_silver_db_test"

# COMMAND ----------

# MAGIC %run ../../python/silver/transform_to_scd2

# COMMAND ----------

# Test the silver layer
customer_df = spark.read.format("delta").option("header", True).load(bronze_path)

spark.sql(f"drop table if exists {user}_silver_db_test.silver_customers")
# Call the function to transform the customer data into a dimension table with SCD type 2
transform_to_scd2(customer_df, mode="test")

silver_df = spark.read.table(f"{output_db}.silver_customers")

assert silver_df.columns == ['customer_id', 'customer_name', 'state', 'company', 'phone_number', 'start_date', 'end_date']

# COMMAND ----------

# MAGIC %run ../../python/silver/standardise_retail_dataset

# COMMAND ----------

# Read the order and sales data from bronze layer
orders_bronze_df = spark.read.format("delta").load(orders_bronze_path)
sales_bronze_df = spark.read.format("delta").load(sales_bronze_path)

# COMMAND ----------

orders_silver_df = transform_to_silver_1(orders_bronze_df)
sales_silver_df = transform_to_silver_2(sales_bronze_df)

# COMMAND ----------

assert orders_silver_df.count == 1000
assert sales_silver_df.count == 1000

# COMMAND ----------

spark.sql(f"drop table if exists {output_db}.silver_orders")
spark.sql(f"drop table if exists {output_db}.silver_sales")

# COMMAND ----------

# Write the standardized data into the silver layer
orders_silver_df.write.format("delta").mode("overwrite").saveAsTable(
    output_db + ".silver_orders"
)

# Write the standardized data into the silver layer
sales_silver_df.write.format("delta").mode("overwrite").saveAsTable(
    output_db + ".silver_sales"
)

# COMMAND ----------

spark.sql(f"drop table if exists {output_db}.silver_products")

# COMMAND ----------

product_bronze_df = spark.read.format("delta").load(products_bronze_path)
product_silver_df = standardize_product_data(product_bronze_df)

# COMMAND ----------

assert product_silver_df.count == 1000

# COMMAND ----------

product_silver_df.write.format("delta").mode("overwrite").saveAsTable(
    output_db + ".silver_products"
)
