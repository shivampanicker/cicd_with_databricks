# Databricks notebook source
# MAGIC %run ../../python/silver/standardise_retail_dataset

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType


def test_transform_to_silver_1():
    # Create input data
    input_data = [("1", "2022-01-01T08:11:14.000Z", "100", "shipped")]
    input_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True)
    ])
    input_df = spark.createDataFrame(input_data, input_schema)
    # Run the transformation function
    actual_df = transform_to_silver_1(input_df)
    return actual_df


# Define expected output data
expected_data = [("1", "2022-01-01T08:11:14.000Z", "100", "completed")]
expected_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True)
])
expected_df = spark.createDataFrame(expected_data, expected_schema)

actual_df = test_transform_to_silver_1()
# Verify the result
assert expected_df.select("order_status").collect(
) == actual_df.select("order_status").collect()

# COMMAND ----------

def test_transform_to_silver_2():
    # Create input data
    input_data = [("1", "100", "2022-12-11T04:03:39.000Z", "50", "US", "AUD")]
    input_schema = StructType([
        StructField("sale_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("sale_date", StringType(), True),
        StructField("sale_amount", StringType(), True),
        StructField("state", StringType(), True),
        StructField("currency", StringType(), True)
    ])
    input_df = spark.createDataFrame(input_data, input_schema)

    # Define expected output data
    expected_data = [("1", "100", "50", "US", "USD")]
    expected_schema = StructType([
        StructField("sale_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("sale_amount", StringType(), True),
        StructField("state", StringType(), True),
        StructField("currency", StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    expected_df = expected_df.withColumn("sale_date", F.to_date(F.lit("2022-12-11")))
    # Run the transformation function
    actual_df = transform_to_silver_2(input_df)
    # Verify the result
    assert expected_df.select("sale_date").collect() == actual_df.select("sale_date").collect()
    assert expected_df.select("currency").collect() == actual_df.select("currency").collect()
    assert expected_df.select("sale_id").collect() == actual_df.select("sale_id").collect()
    assert expected_df.select("sale_amount").collect() == actual_df.select("sale_amount").collect()

# COMMAND ----------

test_transform_to_silver_2()
