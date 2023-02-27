# Databricks notebook source
from pyspark.sql.functions import sum, avg, min, max, count, date_format, year, month
from pyspark.sql.functions import year, quarter, month, sum, avg

# Read in the necessary tables
sales = spark.read.table("silver.sales")
products = spark.read.table("silver.products")
customers = spark.read.table("silver.customers")

# Join the tables
joined = sales.join(products, sales.product_id == products.product_id) \
              .join(customers, sales.customer_id == customers.customer_id)

# Add the desired columns
summary = joined.select(year("date").alias("year"),
                        quarter("date").alias("quarter"),
                        month("date").alias("month"),
                        sum("sales_amount").alias("total_sales"),
                        sum("quantity").alias("total_quantity"),
                        avg("sales_amount").alias("average_price"),
                        "product_category",
                        "customer_country")

# Write the table to the gold layer
summary.write.mode("overwrite").format(
    "delta").saveAsTable("gold.sales_summary")


# COMMAND ----------


# Read data from silver layer
silverDF = spark.read.format("delta").load("/mnt/bronze/retail")

# Perform complex aggregations on silver layer data
goldDF = (silverDF
          .groupBy("Country", "Year", "Month")
          .agg(
              sum("Revenue").alias("TotalRevenue"),
              avg("Revenue").alias("AvgRevenue"),
              min("Revenue").alias("MinRevenue"),
              max("Revenue").alias("MaxRevenue"),
              count("OrderID").alias("TotalOrders"),
              countDistinct("CustomerID").alias("TotalCustomers"),
              sum(when(col("Revenue") > 100, col("Revenue")
                       ).otherwise(0)).alias("RevenueAbove100"),
              date_format("OrderDate", "E").alias("DayOfWeek"),
              year("OrderDate").alias("Year"),
              month("OrderDate").alias("Month")
          )
          )

# Write data to gold layer
goldDF.write.format("delta").mode("overwrite").save("/mnt/gold/retail")
