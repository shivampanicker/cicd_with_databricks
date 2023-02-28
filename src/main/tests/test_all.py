# Databricks notebook source
# MAGIC %run ./bronze/test_load_data_into_bronze

# COMMAND ----------

# MAGIC %run ./silver/test_transform_to_scd2

# COMMAND ----------

# MAGIC %run ./silver/standardise_retail_dataset
