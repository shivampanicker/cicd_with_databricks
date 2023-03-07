# Databricks notebook source
# MAGIC %run ../../python/silver/transform_to_scd2

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")]

input_path = f'/FileStore/{username}_bronze_db/' + env + '/'

# COMMAND ----------

# Read in the source data
customer_df = spark.read.format("delta").option("header", True).load(
    input_path+"bronze_customers")

spark.sql(f"drop table if exists {user}_silver_db_test.silver_customers")
# Call the function to transform the customer data into a dimension table with SCD type 2
transform_to_scd2(customer_df, mode="test")

# Define unit tests for the SCD type 2 transformation


def test_scd2_transform():

    customer_dim_df = spark.sql(
        f"select * from {user}_silver_db_test.silver_customers")
    # Test that the dimension table has the correct columns
    assert set(customer_dim_df.columns) == set(dim_cols)
    # Test that each customer_id has only one record with the most recent end_date
    assert customer_dim_df.groupBy('customer_id').agg(
        max('end_date')).filter(col('max(end_date)') != end_date).count() == 0
    spark.sql(f"drop table if exists {user}_silver_db_test.silver_customers")


# Run the unit tests
test_scd2_transform()
