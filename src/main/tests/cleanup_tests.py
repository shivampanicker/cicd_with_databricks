# Databricks notebook source
username = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get().replace('.', '_')
user = username[:username.index("@")]

# COMMAND ----------

env = dbutils.widgets.get("env")

# COMMAND ----------

bronze_path = f'/FileStore/{username}_bronze_db/' + env
silver_path = f'/FileStore/{username}_silver_db/' + env
gold_path = f'/FileStore/{username}_gold_db/' + env

# COMMAND ----------

dbutils.fs.rm(bronze_path, True)
dbutils.fs.rm(silver_path, True)
dbutils.fs.rm(gold_path, True)

# COMMAND ----------

spark.sql(f"drop database if exists {user}_bronze_db_{env} cascade")

# COMMAND ----------

spark.sql(f"drop database if exists {user}_silver_db_{env} cascade")

# COMMAND ----------

spark.sql(f"drop database if exists {user}_gold_db_{env} cascade")

# COMMAND ----------

dbutils.fs.rm(f"/FileStore/{username}/retail_dataset/" + env, True)
