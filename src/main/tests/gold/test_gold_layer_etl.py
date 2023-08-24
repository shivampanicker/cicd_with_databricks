# Databricks notebook source
pip install pytest

# COMMAND ----------

import pytest
import sys

# COMMAND ----------

sys.path.append("../../python/gold/")
from gold_layer_etl import GoldAggregations

# COMMAND ----------

from pyspark.sql.functions import col
from datetime import datetime

order_data = [
    ('order1', 1, datetime(2022, 1, 1, 0, 0, 0), 'completed'),
    ('order2', 1, datetime(2022, 1, 3, 0, 0, 0), 'completed'),
    ('order3', 2, datetime(2022, 1, 5, 0, 0, 0), 'pending'),
    ('order4', 3, datetime(2022, 1, 6, 0, 0, 0), 'completed'),
    ('order5', 4, datetime(2022, 1, 7, 0, 0, 0), 'completed')
]
orders_df = spark.createDataFrame(order_data, ['order_id', 'customer_id', 'order_date', 'order_status'])

sale_data = [
    (1, 1, datetime(2022, 1, 2, 0, 0, 0), 100, 'USD'),
    (2, 1, datetime(2022, 1, 4, 0, 0, 0), 150, 'USD'),
    (3, 2, datetime(2022, 1, 5, 0, 0, 0), 75, 'EUR'),
    (4, 3, datetime(2022, 1, 7, 0, 0, 0), 200, 'USD'),
    (5, 4, datetime(2022, 1, 8, 0, 0, 0), 120, 'EUR')
]
sales_df = spark.createDataFrame(sale_data, ['sale_id', 'product_id', 'sale_date', 'sale_amount', 'currency'])

product_data = [
    (1, 'electronics', datetime(2022, 1, 1, 0, 0, 0)),
    (2, 'furniture', datetime(2022, 1, 2, 0, 0, 0)),
    (3, 'clothing', datetime(2022, 1, 3, 0, 0, 0)),
    (4, 'electronics', datetime(2022, 1, 4, 0, 0, 0)),
    (5, 'furniture', datetime(2022, 1, 5, 0, 0, 0))
]
products_df = spark.createDataFrame(product_data, ['product_id', 'product_category', 'product_start_date'])

customer_data = [
    (1, 'Alice', 'CA', 'ABC Company', '123-456-7890', datetime(2022, 1, 1, 0, 0, 0)),
    (2, 'Bob', 'NY', 'DEF Company', '234-567-8901', datetime(2022, 1, 3, 0, 0, 0)),
    (3, 'Charlie', 'CA', 'GHI Company', '345-678-9012', datetime(2022, 1, 5, 0, 0,0))] 
customers_df = spark.createDataFrame(customer_data, ['customer_id', 'customer_name', 'state', 'company', 'phone_number', 'start_date'])


# create temporary views for each dataset
orders_df.createOrReplaceTempView("orders")
products_df.createOrReplaceTempView("products")
customers_df.createOrReplaceTempView("customers")
sales_df.createOrReplaceTempView("sales")

def test_total_sales_amount_in_usd(spark, sales):
    result = GoldAggregations.total_sales_amount_in_usd(spark, sales)
    assert result.select("total_sales").collect()[0].total_sales == 450
    
test_total_sales_amount_in_usd(spark, "sales")


def test_top_10_best_selling_products(spark, sales, products):
    result = GoldAggregations.top_10_best_selling_products(spark, sales, products)
    assert result.select("product_id").limit(1).collect()[0].product_id == 1
    
test_top_10_best_selling_products(spark, "sales", "products")


def test_num_customers_by_state(spark, customers):
    result = GoldAggregations.num_customers_by_state(spark, customers)
    assert result.select("total_customers").filter("state = 'CA'").collect()[0].total_customers == 2
    
test_num_customers_by_state(spark, "customers")


def test_avg_sales_by_month(spark, sales):
    result = GoldAggregations.avg_sales_by_month(spark, sales)
    assert result.select("avg_sales").filter((col('year') == '2022') & (col('month') == '1')).collect()[0].avg_sales == 129
    
test_avg_sales_by_month(spark, "sales")


# def test_total_num_orders(spark, orders):

#     # Execute function
#     result = GoldAggregations.total_num_orders(spark, orders)
#     # Check the output
#     assert result.select("total_orders").collect()[0].total_orders == orders_df.count()
                        

# test_total_num_orders(spark, "orders")
