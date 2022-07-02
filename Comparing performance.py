# Databricks notebook source
# utilizando SQL
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM 2015_summary_csv
GROUP BY DEST_COUNTRY_NAME
""")

# COMMAND ----------

# Utilizando Python
dataFrameWay = df.groupBy("DEST_COUNTRY_NAME").count()

# COMMAND ----------

# imprime o plano de execução do código
sqlWay.explain()

# COMMAND ----------

# imprime o plano de execução do código
dataFrameWay.explain()
