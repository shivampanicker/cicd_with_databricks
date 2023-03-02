# Databricks notebook source
# MAGIC %md
# MAGIC This unit test checks that the load_data_to_bronze function loads data from the input path to the output path in Delta format, and that the output files are not empty. You can add additional checks as needed depending on the specifics of your autoloader module.

# COMMAND ----------

pip install pytest

# COMMAND ----------

import pytest

# COMMAND ----------

# MAGIC %run /Repos/shivam.panicker@databricks.com/cicd_with_databricks/src/main/python/bronze/load_data_into_bronze

# COMMAND ----------


# Define the input and output paths for the test case

username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')

source_dataset = 'customers'
target_path = f'/FileStore/{username}_bronze_db_test/'


def test_load_data_to_bronze():
    # Call the load_data_to_bronze function to load data into bronze
    # dbutils.fs.rm(target_path, True)
    dbutils.fs.rm(target_path, True)

    load_data_to_bronze(source_dataset, target_path)

    # Check that the output path contains the expected number of files
    expected_num_files = 2
    num_files = len(dbutils.fs.ls(target_path+"bronze_"+source_dataset))
    assert num_files == expected_num_files, f"Expected {expected_num_files} files, but found {num_files} files."

    # Check that the output files are not empty
    for file_info in dbutils.fs.ls(target_path+"bronze_"+source_dataset):
        if ".parquet" in file_info:
            file_size = file_info.size
            assert file_size > 0, f"{file_info.name} is empty."

    # Check that the output files has expected count
    expected_count = 10000
    assert spark.read.format("delta").load(
        target_path+"bronze_"+source_dataset).count() == expected_count


# COMMAND ----------

test_load_data_to_bronze()
