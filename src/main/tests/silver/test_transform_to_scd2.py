# Databricks notebook source
# MAGIC %run ../../python/silver/transform_to_scd2

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")]

input_path = f'/FileStore/{username}_bronze_db/'

# COMMAND ----------

dataset = dbutils.widgets.text("source_dataset", "")

dataset = dbutils.widgets.get("source_dataset")

# Set the target location for the delta table
target_path = f"/FileStore/{username}_bronze_db_test/"


# Read in the source data
customer_df = spark.read.format("delta").option("header", True).load(
    input_path+"bronze_customers")

spark.sql(f"drop table if exists {user}_silver_db.silver_customers_test")
# Call the function to transform the customer data into a dimension table with SCD type 2
transform_to_scd2(customer_df, mode="test")

# Define unit tests for the SCD type 2 transformation


def test_scd2_transform():

    customer_dim_df = spark.sql(
        f"select * from {user}_silver_db.silver_customers_test")
    # Test that the dimension table has the correct columns
    assert set(customer_dim_df.columns) == set(dim_cols)
    # Test that each customer_id has only one record with the most recent end_date
    assert customer_dim_df.groupBy('customer_id').agg(
        max('end_date')).filter(col('max(end_date)') != end_date).count() == 0
    spark.sql(f"drop table if exists {user}_silver_db.silver_customers_test")


# Run the unit tests
test_scd2_transform()
