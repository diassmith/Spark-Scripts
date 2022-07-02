# Databricks notebook source
# DBTITLE 1,Reading JSON FILE
df_json = spark.read.format("json")\
.option("mode", "FAILFAST")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_summary.json")

# COMMAND ----------

# DBTITLE 1,Print Schema
df_json.printSchema()

# COMMAND ----------

# DBTITLE 1,Print 10 rows
display(df_json.head(10))
