# Databricks notebook source
abs_path = "/Repos/shivam.panicker@databricks.com/cicd_with_databricks/src/main/tests"

# COMMAND ----------

dbutils.notebook.run(abs_path+"/cleanup_tests", 300, {})

# COMMAND ----------

dbutils.notebook.run(abs_path+"/../python/setup/initiate_setup", 300, {})

# COMMAND ----------

dbutils.notebook.run(abs_path+"/bronze/test_load_data_into_bronze", 300, {})

# COMMAND ----------

dbutils.notebook.run(abs_path+"/silver/test_transform_to_scd2", 300, {})

# COMMAND ----------

dbutils.notebook.run(abs_path+"/silver/test_standardise_retail_dataset", 300, {})

# COMMAND ----------

dbutils.notebook.run(abs_path+"/gold/test_gold_layer_etl", 300, {})

# COMMAND ----------

dbutils.notebook.run(abs_path+"/cleanup_tests", 300, {})
