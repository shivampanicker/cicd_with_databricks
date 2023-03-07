# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    LongType,
)
from faker import Faker
import random
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import *


# COMMAND ----------

username = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
    .replace(".", "_")
)
output_dir = f"/FileStore/{username}/retail_dataset/"

# COMMAND ----------


def generate_orders_data(num_rows: int, env: str) -> None:
    """
    Generate fake orders data using faker library and write to Delta table in DBFS.
    """
    # Create Faker instance
    fake = Faker()

    # Define schema for orders data
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", TimestampType(), True),
            StructField("order_status", StringType(), True),
        ]
    )

    # Generate orders data
    orders_data = []
    for i in range(num_rows):
        order_id = fake.uuid4()
        customer_id = random.randint(1, 10000)
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        order_status = fake.random_element(
            elements=("pending", "processing", "shipped")
        )
        orders_data.append((order_id, customer_id, order_date, order_status))

    # Create DataFrame from orders data and apply schema
    orders_df = spark.createDataFrame(orders_data, schema)

    # Write orders data to Delta table
    orders_df.coalesce(1).write.format("csv").option("header", "true").mode(
        "overwrite"
    ).save(output_dir + env + "/orders")
    print("Orders file generated")


# COMMAND ----------


def generate_sales_data(num_rows, env: str):
    fake = Faker()
    sales_data = []
    schema = StructType(
        [
            StructField("sale_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("sale_date", TimestampType(), True),
            StructField("sale_amount", LongType(), True),
            StructField("currency", StringType(), True),
        ]
    )

    # Generate sales data
    for i in range(num_rows):
        sale_id = random.randint(1, 10000)
        product_id = random.randint(1, 500)
        sale_date = fake.date_time_between(start_date="-1y", end_date="now")
        sale_amount = fake.random_int(min=10, max=500)
        currency = fake.currency_code()

        sales_data.append((sale_id, product_id, sale_date, sale_amount, currency))

    # Create a PySpark DataFrame from the generated data
    sales_df = spark.createDataFrame(sales_data, schema)

    # Add a timestamp column
    sales_df = sales_df.withColumn("ingest_timestamp", current_timestamp())

    sales_df.coalesce(1).write.format("csv").option("header", "true").mode(
        "overwrite"
    ).save(output_dir + env + "/sales")
    print("Sales file generated")


# COMMAND ----------


def generate_product_data(num_rows, env: str):
    """
    Generate product dataset using faker library with specified number of rows

    Args:
    num_rows: int, number of rows to generate

    Returns:
    product_df: DataFrame, product dataset with columns "product_id", "product_description", "product_start_date"
    """

    fake = Faker()
    product_data = []
    schema = StructType(
        [
            StructField("product_id", IntegerType(), True),
            StructField("product_category", StringType(), True),
            StructField("product_start_date", TimestampType(), True),
        ]
    )

    for i in range(num_rows):
        product_id = random.randint(1, 500)
        product_description = fake.random_element(
            elements=(
                "Entertainment",
                "Gambling",
                "Health",
                "Mortgage & Rent Repayments",
                "Sports",
                "Recreation",
                "Miscellaneous",
                "Scientific",
            )
        )
        product_start_date = fake.date_time_between(start_date="-5y", end_date="now")
        product_data.append((product_id, product_description, product_start_date))

    # create spark dataframe
    product_df = spark.createDataFrame(product_data, schema=schema)

    product_df.coalesce(1).write.format("csv").option("header", "true").mode(
        "overwrite"
    ).save(output_dir + env + "/products")
    print("Products file generated")


# COMMAND ----------


username = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
    .replace(".", "_")
)
output_dir = f"/FileStore/{username}/retail_dataset/"


def generate_customer_data_day_0(num_rows: int, env: str):
    fake = Faker()
    customer_data = []
    schema = StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("state", StringType(), True),
            StructField("company", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("start_date", TimestampType(), True),
        ]
    )

    for i in range(num_rows):
        customer_id = random.randint(1, 10001)
        customer_name = fake.name()
        state = fake.state()
        company = fake.company()
        phone_number = fake.phone_number()
        start_date = fake.date_time_between(start_date="-5y", end_date="-1y")

        customer_data.append(
            (customer_id, customer_name, state, company, phone_number, start_date)
        )

    # create spark dataframe
    customer_df = spark.createDataFrame(customer_data, schema=schema)

    # Write to Delta Lake as bronze layer
    customer_df.coalesce(1).write.format("csv").option("header", "true").mode(
        "overwrite"
    ).save(output_dir + env + "/customers/")
    print("Customers day0 file generated")


# COMMAND ----------


def generate_customer_data_day_2(env: str):
    fake = Faker()
    customer_data = []
    schema = StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("customer_name", StringType(), True),
            StructField("state", StringType(), True),
            StructField("company", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("start_date", TimestampType(), True),
        ]
    )

    # Generate customer data with random start_date
    for i in range(3000):
        customer_id = random.randint(7000, 10001)
        customer_name = fake.name()
        state = fake.state()
        company = fake.company()
        phone_number = fake.phone_number()
        start_date = fake.date_time_between(start_date="-1y", end_date="now")
        customer_data.append((customer_id, customer_name, state, company, phone_number, start_date))

    # create spark dataframe
    customer_df = spark.createDataFrame(customer_data, schema=schema)

    # Write to Delta Lake as bronze layer
    customer_df.coalesce(1).write.format("csv").option("header", "true").mode(
        "append"
    ).save(output_dir + env + "/customers")
    print("Customers day1 file generated")
