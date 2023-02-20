# Databricks notebook source
import random
import string
import csv
import time
from datetime import datetime, timedelta

# Set the number of rows to generate
num_rows = 10000

# Define the output directory in DBFS
database_name = spark.conf.get("com.databricks.training.spark.dbName")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
output_dir = f"/FileStore/{username}/retail_dataset"

# Create the output directory if it doesn't exist
dbutils.fs.mkdirs(output_dir)

# Define the function to generate random product names
def generate_product_name():
    first_word = random.choice(['Red', 'Blue', 'Green', 'Yellow', 'Purple'])
    second_word = random.choice(['Shirt', 'Pants', 'Socks', 'Hat', 'Jacket'])
    return f"{first_word} {second_word}"

# Define the function to generate random customer names
def generate_customer_name():
    first_name = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
    last_name = ''.join(random.choice(string.ascii_uppercase) for _ in range(7))
    return f"{first_name} {last_name}"

# Define the function to generate a random timestamp
def generate_timestamp():
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()
    time_between_dates = end_date - start_date
    random_number_of_days = random.randrange(time_between_dates.days)
    random_date = start_date + timedelta(days=random_number_of_days)
    return random_date.strftime("%Y-%m-%d %H:%M:%S")

# Define the header for the orders CSV file
orders_header = ["order_id", "customer_id", "order_date"]

# Define the header for the sales CSV file
sales_header = ["order_id", "product_name", "quantity", "price"]

# Define the header for the customers CSV file
customers_header = ["customer_id", "customer_name", "customer_email", "customer_phone"]

# Open the CSV files for writing
with open(f"{output_dir}/orders.csv", "w", newline="") as orders_file, \
     open(f"{output_dir}/sales.csv", "w", newline="") as sales_file, \
     open(f"{output_dir}/customers.csv", "w", newline="") as customers_file:
    
    # Create CSV writers for each file
    orders_writer = csv.writer(orders_file)
    sales_writer = csv.writer(sales_file)
    customers_writer = csv.writer(customers_file)

    # Write the headers to each file
    orders_writer.writerow(orders_header)
    sales_writer.writerow(sales_header)
    customers_writer.writerow(customers_header)
    
    # Generate the data for each row and write it to the corresponding file
    for i in range(num_rows):
        # Generate the order data
        order_id = i + 1
        customer_id = random.randint(1, 1000)
        order_date = generate_timestamp()
        orders_writer.writerow([order_id, customer_id, order_date])
        
        # Generate the sales data for the order
        num_sales = random.randint(1, 5)
        for j in range(num_sales):
            product_name = generate_product_name()
            quantity = random.randint(1, 10)
            price = round(random.uniform(10.0, 100.0), 2)
            sales_writer.writerow([order_id, product_name, quantity, price])
        
        # Generate the customer data
        if i % 100 == 0:
            customer_id = customer_id
            customer_name = generate_customer_name()
            customer_email = f"{customer_name.lower().replace('

