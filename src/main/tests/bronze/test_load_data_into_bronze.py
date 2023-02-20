# Databricks notebook source
# MAGIC %md
# MAGIC This unit test checks that the load_data_to_bronze function loads data from the input path to the output path in Delta format, and that the output files are not empty. You can add additional checks as needed depending on the specifics of your autoloader module.

# COMMAND ----------

import pytest
from databricks_autoloader import load_data_to_bronze

# Define the input and output paths for the test case

database_name = spark.conf.get("com.databricks.training.spark.dbName")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
input_path = f'/FileStore/{username}/retail_data/orders'
output_path = f'/FileStore/{username}/bronze/'

def test_load_data_to_bronze():
    # Call the load_data_to_bronze function to load data into bronze
    load_data_to_bronze(input_path, output_path)
    
    # Check that the output path contains the expected number of files
    expected_num_files = 3
    num_files = len(dbutils.fs.ls(output_path))
    assert num_files == expected_num_files, f"Expected {expected_num_files} files, but found {num_files} files."
    
    # Check that the output files have the expected format
    expected_file_format = 'delta'
    for file_info in dbutils.fs.ls(output_path):
        file_name = file_info.name
        file_format = file_name.split('.')[-1]
        assert file_format == expected_file_format, f"Expected {file_name} to be in {expected_file_format} format, but it is in {file_format} format."
    
    # Check that the output files are not empty
    for file_info in dbutils.fs.ls(output_path):
        file_size = file_info.size
        assert file_size > 0, f"{file_info.name} is empty."

