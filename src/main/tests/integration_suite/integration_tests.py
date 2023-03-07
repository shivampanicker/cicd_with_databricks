# Databricks notebook source
pip install pytest

# COMMAND ----------

import pytest

# COMMAND ----------

username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get()
user = username[:username.index("@")].replace('.', '_')

# COMMAND ----------

source_dataset = 'customers'
target_path = f'/FileStore/{username}_bronze_db_test/'
abs_path = f'/Repos/{username}/cicd_with_databricks/src/main/tests/'

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

dbutils.notebook.run(abs_path + "integration_suite/cleanup_integration_suite", 300, {"env":env})

# COMMAND ----------

dbutils.notebook.run(abs_path + "/../python/setup/initiate_setup", 300, {"num_rows":1000, "env":env})

# COMMAND ----------

dbutils.notebook.run(abs_path + "integration_suite/test_integration_bronze_layer_etl", 300, {"env":env})

# COMMAND ----------

dbutils.notebook.run(abs_path + "integration_suite/test_integration_silver_layer_etl", 300, {"env":env})

# COMMAND ----------

dbutils.notebook.run(abs_path + "integration_suite/test_integration_gold_layer_etl", 300, {})

# COMMAND ----------

print("All integration tests passed!")

# COMMAND ----------

dbutils.notebook.run(abs_path + "integration_suite/cleanup_integration_suite", 300, {"env":env})
