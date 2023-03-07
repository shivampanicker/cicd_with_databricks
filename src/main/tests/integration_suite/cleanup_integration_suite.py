# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")]

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

bronze_path = f'/FileStore/{username}_bronze_db' + env

# COMMAND ----------

dbutils.fs.rm(bronze_path, True)

# COMMAND ----------

spark.sql(f"drop database if exists {user}_silver_db_test cascade")

# COMMAND ----------

spark.sql(f"drop database if exists {user}_gold_db_test cascade")
