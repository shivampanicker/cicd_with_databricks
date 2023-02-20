# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Define the start and end dates for each record in the dimension table
start_date = to_date(lit("2022-01-01"))
end_date = to_date(lit("9999-12-31"))

# Define the columns to include in the dimension table
dim_cols = ['customer_id', 'name', 'address', 'city', 'state', 'zip_code', 'start_date', 'end_date']

def transform_to_scd2(df):
  # Create a window function to rank the customer records by customer_id and start_date
  customer_scd2_df = customer_df.select('customer_id', 'customer_name', 'address', 'city', 'state', 'zip_code') \
    .withColumn('current_flag', lit(1)) \
    .withColumn('effective_start_date', current_date()) \
    .withColumn('effective_end_date', to_date(lit('9999-12-31'))) \
    .withColumn('version_number', lit(1))

  # Get the latest version of each customer record
  latest_customer_df = customer_scd2_df.groupBy('customer_id').agg(max('version_number').alias('latest_version_number'))

  # Join the customer_scd2_df with the latest_customer_df to get the latest version of each customer record
  customer_scd2_df = customer_scd2_df.join(latest_customer_df, ['customer_id', 'version_number'], 'inner') \
    .drop('version_number')

  # Get the previous version of each customer record
  previous_customer_df = customer_scd2_df.alias('df1') \
    .join(customer_scd2_df.alias('df2'), (col('df1.customer_id') == col('df2.customer_id')) \
      & (col('df1.effective_end_date') == col('df2.effective_start_date')) \
      & (col('df2.current_flag') == 1), 'left') \
    .select(
      'df1.customer_id',
      'df1.customer_name',
      'df1.address',
      'df1.city',
      'df1.state',
      'df1.zip_code',
      'df1.current_flag',
      'df1.effective_start_date',
      when(col('df2.current_flag') == 1, col('df2.effective_start_date')).otherwise(to_date(lit('9999-12-31'))).alias('effective_end_date'),
      'df1.latest_version_number'
    )

  # Union the customer_scd2_df and the previous_customer_df to get the full history of each customer record
  customer_scd2_df = customer_scd2_df.unionByName(previous_customer_df).orderBy('customer_id', 'effective_start_date')

  # Write the customer_scd2_df to a Delta table
  customer_scd2_df.write.format('delta').mode('overwrite').saveAsTable('customer_scd2')

