# Databricks notebook source
username = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .userName()
    .get()
    .replace(".", "_")
)
user = username[: username.index("@")]

# COMMAND ----------

spark.sql(f"drop database if exists {user}_bronze_db cascade")

# COMMAND ----------

spark.sql(f"drop database if exists {user}_silver_db cascade")

# COMMAND ----------

spark.sql(f"drop database if exists {user}_gold_db cascade")

# COMMAND ----------

dbutils.fs.rm(f"/FileStore/{username}_bronze_db/", True)

# COMMAND ----------

dbutils.fs.rm(f"/FileStore/{username}_silver_db/", True)

# COMMAND ----------

dbutils.fs.rm(f"/FileStore/{username}_gold_db/", True)

# COMMAND ----------

dbutils.fs.rm(f"/FileStore/{username}/retail_dataset/", True)
