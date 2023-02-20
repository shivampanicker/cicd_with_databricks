# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType

def test_standardization_1():
  # Create input data
  input_data = [("John", "Doe", "123 Main St", "New York", "NY", "12345", 50.00, "USD")]
  input_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("currency", StringType(), True)
  ])
  input_df = spark.createDataFrame(input_data, input_schema)
  
  # Define expected output data
  expected_data = [("John", "Doe", "123 Main St", "New York", "NY", "12345", 50.00, "USD")]
  expected_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("amount_usd", DecimalType(10, 2), True)
  ])
  expected_df = spark.createDataFrame(expected_data, expected_schema)
  
  # Run the transformation function
  actual_df = transform_to_silver_1(input_df)
  
  # Verify the result
  assert expected_df.collect() == actual_df.collect()

# COMMAND ----------

def test_standardization_2():
  # Create input data
  input_data = [("John", "Doe", "123 Main St", "New York", "NY", "12345", 50.00, "USD")]
  input_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("currency", StringType(), True)
  ])
  input_df = spark.createDataFrame(input_data, input_schema)
  
  # Define expected output data
  expected_data = [("John", "Doe", "123 Main St", "New York", "NY", "12345", 50.00, "USD")]
  expected_schema = StructType([
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("currency_code", StringType(), True)
  ])
  expected_df = spark.createDataFrame(expected_data, expected_schema)
  
  # Run the transformation function
  actual_df = transform_to_silver_2(input_df)
  
  # Verify the result
  assert expected_df.collect() == actual_df.collect()

# COMMAND ----------

# Run the unit tests
test_standardization_1()
test_standardization_2()
