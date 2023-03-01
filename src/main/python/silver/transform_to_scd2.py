# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().replace('.','_')
user = username[:username.index("@")]

input_path = f'/FileStore/{username}_bronze_db/'
output_path = f'/FileStore/{username}_silver_db/'

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import *

# Define the start and end dates for each record in the dimension table
start_date = to_date(lit("2022-01-01"))
end_date = to_date(lit("9999-12-31"))

# Define the columns to include in the dimension table
dim_cols = ['customer_id', 'customer_name', 'state', 'company', 'phone_number', 'start_date', 'end_date']

def transform_to_scd2(customer_data, mode: str):
  # Generate SCD Type 2 table
  
  if mode == "test":
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {user}_silver_db_test.silver_customers
      (
      customer_id INT,
      customer_name STRING,
      state STRING,
      company STRING,
      phone_number STRING,
      start_date TIMESTAMP,
      end_date TIMESTAMP
      )
    """)
    
    silver_customers = DeltaTable.forName(spark, user+'_silver_db_test.silver_customers')
    
  else:
    silver_customers = DeltaTable.forName(spark, user+'_silver_db.silver_customers')
  effective_date = lit(current_date())
  scd2_data = customer_data.select(
      "customer_id",
      "customer_name",
      "state",
      "company",
      "phone_number"
  ).distinct().withColumn(
      "start_date",
      effective_date
  ).withColumn(
      "end_date",
      to_date(lit("9999-12-31"))
  )

  # Merge SCD Type 2 table with existing Delta Lake table
  merge_condition = "scd2.customer_id = source.customer_id"
  merge_delta_conf = {
      "mergeSchema": "true",
      "predicate": merge_condition
  }

  customer_dim_df = silver_customers.alias("scd2").merge(scd2_data.alias("source"),"scd2.customer_id = source.customer_id").whenMatchedUpdate(set={"end_date": date_sub(current_date(), 1)}).whenNotMatchedInsert(values={ "customer_id": col("source.customer_id"),"customer_name": col("source.customer_name"),"state": col("source.state"), "company": col("source.company"), "phone_number": col("source.phone_number"),"start_date": col("source.start_date"), "end_date": col("source.end_date")}).execute()
