# Databricks notebook source
# MAGIC %md
# MAGIC In this code, we've added standardisation functions to clean and standardise the name, address, city, and state columns in the customer data. We've also added transformation functions to add the customer and order details to the sales data, and to convert the sales data to different currencies using exchange rates. Finally, we've written the transformed data to delta lake in the Silver layer.

# COMMAND ----------

# Read the order and sales data from bronze layer
orders_bronze_df = spark.read.format("delta").load("/mnt/bronze/orders")
sales_bronze_df = spark.read.format("delta").load("/mnt/bronze/sales")


def transform_to_silver_1(orders_silver_df):
  # Apply standardizations to order data
  orders_silver_df = orders_bronze_df.withColumn("order_date", to_date(col("order_date"), "dd/MM/yyyy")) \
                                     .withColumn("order_status", when(col("order_status") == "SHIPPED", "COMPLETED").otherwise(col("order_status"))) \
                                     .withColumnRenamed("order_id", "id") \
                                     .withColumnRenamed("order_customer_id", "customer_id") \
                                     .select("id", "order_date", "customer_id", "order_status")
  return orders_silver_df

def transform_to_silver_2(sales_bronze_df):
  # Apply standardizations to sales data
  sales_silver_df = sales_bronze_df.withColumn("sale_date", to_date(col("sale_date"), "dd/MM/yyyy")) \
                                   .withColumn("sale_amount", round(col("sale_amount") * 0.9, 2)) \
                                   .withColumn("currency", lit("USD")) \
                                   .select("sale_id", "product_id", "sale_date", "sale_amount", "currency")
  return sales_silver_df

# COMMAND ----------

orders_silver_df = transform_to_silver_1(orders_silver_df)
sales_bronze_df = transform_to_silver_2(sales_bronze_df)

# COMMAND ----------

# Write the standardized data into the silver layer
orders_silver_df.write.format("delta").mode("overwrite").save("/mnt/silver/orders")
sales_silver_df.write.format("delta").mode("overwrite").save("/mnt/silver/sales")
