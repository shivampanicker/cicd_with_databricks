# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")]

# COMMAND ----------

bronze_path = f'/FileStore/{username}_bronze_db_test/'
silver_path = f'/FileStore/{username}_silver_db_test/'
gold_path = f'/FileStore/{username}_gold_db_test/'

# COMMAND ----------

dbutils.fs.rm(bronze_path, True)
dbutils.fs.rm(silver_path, True)
dbutils.fs.rm(gold_path, True)

# COMMAND ----------

spark.sql(f"drop database if exists {user}_bronze_db_test cascade")

# COMMAND ----------

spark.sql(f"drop database if exists {user}_silver_db_test cascade")

# COMMAND ----------

spark.sql(f"drop database if exists {user}_gold_db_test cascade")
