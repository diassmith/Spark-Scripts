# Databricks notebook source
# MAGIC %md
# MAGIC ### Modos de leitura
# MAGIC - **permissive**: *Sets all fields to NULL when it finds corrupted records and puts all corrupted records in a column called _corrupt_record.* (default)
# MAGIC 
# MAGIC - **dropMalformed**: *Deletes a corrupted line or that it cannot read.*
# MAGIC 
# MAGIC - **failFast**: *Falha imediatamente quando encontra uma linha que não consiga ler.Fails immediately when it encounters a line it cannot read*

# COMMAND ----------

# DBTITLE 1,Reading CSV file
# leia o arquivo alterando os modos de leitura (failfast, permissive, dropmalformed)
df = spark.read.format("csv")\
.option("mode", "failfast")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_12_01.csv")

# COMMAND ----------

# DBTITLE 1,Print 10 frist rows
display(df.head(10))
