# Databricks notebook source
# DBTITLE 1,Using SQL
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM 2015_summary_csv
GROUP BY DEST_COUNTRY_NAME
""")

# COMMAND ----------

# DBTITLE 1,Using Python
dataFrameWay = df.groupBy("DEST_COUNTRY_NAME").count()

# COMMAND ----------

# DBTITLE 1,Print the execute plan 
sqlWay.explain()

# COMMAND ----------

# DBTITLE 1,Print the execute plan 
dataFrameWay.explain()
