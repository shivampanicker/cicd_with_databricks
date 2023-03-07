# Databricks notebook source
# MAGIC %md
# MAGIC In this code, we've added standardisation functions to
# clean and standardise the name, address, city, and state columns
# in the customer data. We've also added transformation functions
# to add the customer and order details to the sales data,
# and to convert the sales data to different currencies using exchange rates.
# Finally, we've written the transformed data to delta lake in the Silver layer.

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import (
    to_date,
    col,
    lit,
    round,
    when,
    substring,
    coalesce,
    col,
    upper,
)
username = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
    .replace(".", "_")
)
user = username[: username.index("@")]
output_db = f"{user}_silver_db"

# COMMAND ----------


def transform_to_silver_1(orders_bronze_df):
    # Apply standardizations to order data
    orders_silver_df = (
        orders_bronze_df.withColumn("order_date", col("order_date").cast("Timestamp"))
        .withColumn(
            "order_status",
            when(col("order_status") == "shipped", "completed").otherwise(
                col("order_status")
            ),
        )
        .withColumn("customer_id", col("customer_id").cast("Integer"))
        .select("order_id", "order_date", "customer_id", "order_status")
    )
    return orders_silver_df


def transform_to_silver_2(sales_bronze_df):
    # Apply standardizations to sales data
    sales_silver_df = (
        sales_bronze_df.withColumn("sale_date", to_date(col("sale_date").cast("Date")))
        .withColumn("sale_amount", round(col("sale_amount").cast("Double") * 0.9, 2))
        .withColumn("currency", lit("USD"))
        .withColumn("product_id", col("product_id").cast("Integer"))
        .select("sale_id", "product_id", "sale_date", "sale_amount", "currency")
    )
    return sales_silver_df


# COMMAND ----------


def standardize_product_data(df):
    # Replace null values in "product_category" column with "Unknown"
    df = (
        df.withColumn("product_id", col("product_id").cast("Integer"))
        .withColumn("product_start_date", col("product_start_date").cast("Timestamp"))
        .withColumn(
            "product_category", coalesce(col("product_category"), lit("Unknown"))
        )
        .select("product_id", "product_category", "product_start_date")
    )

    return df
