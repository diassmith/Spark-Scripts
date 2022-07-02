# Databricks notebook source
# DBTITLE 1,Bring the first row value max from count column
#Out[3]: [Row(max(count)=370002)]

from pyspark.sql.functions import max
df.select(max("count")).take(1)

# COMMAND ----------

# DBTITLE 1,Filtering with filter method
#+-----------------+-------------------+-----+
#|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
#+-----------------+-------------------+-----+
#|    United States|            Romania|    1|
#|Equatorial Guinea|      United States|    1|
#+-----------------+-------------------+-----+

df.filter("count < 2").show(2)

# COMMAND ----------

# DBTITLE 1,Filtering with where (alias)
df.where("count < 2").show(2)

# COMMAND ----------

# DBTITLE 1,Filtering using SQL
# MAGIC %sql
# MAGIC -- filtrando linhas com sql
# MAGIC SELECT * 
# MAGIC FROM 2015_summary_csv
# MAGIC WHERE count < 2
# MAGIC LIMIT 2

# COMMAND ----------

# DBTITLE 1,Getting the unic row
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
