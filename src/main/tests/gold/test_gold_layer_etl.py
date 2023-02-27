# Databricks notebook source
from pyspark.sql.functions import sum, avg, count
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# Load test data from Gold table
gold_df = spark.read.format("delta").load("/mnt/delta/gold_table")

# Execute the function to create the aggregate DataFrame
agg_df = calculate_aggregations(gold_df)

# Define expected output of the function for the test case
expected_output = spark.createDataFrame(
    [
        (1, 245, 18, 157.65, 202),
        (2, 167, 15, 105.74, 162),
        (3, 100, 10, 63.60, 105),
    ],
    schema=["product_id", "num_sales",
            "num_customers", "revenue", "num_returns"]
)

# Define the unit test


def test_calculate_aggregations():
    assert agg_df.count() == 3
    assert agg_df.select("product_id").distinct().count() == 3
    assert agg_df.select(F.sum("num_sales")).collect()[0][0] == 512
    assert agg_df.select(F.sum("num_customers")).collect()[0][0] == 43
    assert agg_df.select(F.sum("revenue")).collect()[0][0] == 327.99
    assert agg_df.select(F.sum("num_returns")).collect()[0][0] == 469
    assert agg_df.select(
        F.format_number(
            F.avg("revenue_per_sale"), 2
        ).cast(DecimalType(5, 2)).alias("revenue_per_sale")
    ).collect() == expected_output.select("revenue_per_sale").collect()


# Execute the unit test
test_calculate_aggregations()


# COMMAND ----------


# define test function

def test_aggregations():
    # load sample data from a test file
    sample_data = spark.read.format('delta').load('/mnt/bronze/retail_data')

    # generate additional aggregations
    agg1 = sample_data.groupBy('category').agg(
        sum('revenue').alias('total_revenue'))
    agg2 = sample_data.groupBy('category').agg(
        avg('revenue').alias('average_revenue'))
    agg3 = sample_data.groupBy('category').agg(count('*').alias('order_count'))

    # check if aggregations are correct
    assert agg1.count() == 10
    assert agg2.count() == 10
    assert agg3.count() == 10
    assert agg1.select('category').distinct().count() == 10
    assert agg2.select('category').distinct().count() == 10
    assert agg3.select('category').distinct().count() == 10
