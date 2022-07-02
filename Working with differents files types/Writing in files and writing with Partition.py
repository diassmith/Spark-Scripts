# Databricks notebook source
# MAGIC %md
# MAGIC ### Escrevendo arquivos
# MAGIC - **append** : Adds output files to the list of files that already exist in the location.
# MAGIC - **overwrite** : Overwrite files on destination.
# MAGIC - **erroIfExists** : Throws an error and stops if files already exist on the destination.
# MAGIC - **ignore** : If the data exists in the destination, it does nothing.

# COMMAND ----------

# DBTITLE 1,Write csv file
df.write.format("csv")\
.mode("overwrite") \
.option("sep", ",") \
.save("/FileStore/tables/bronze/OUT_2010_12_01.csv")

# COMMAND ----------

# DBTITLE 1,Getting my file
file = "/FileStore/tables/bronze/OUT_2010_12_01.csv/part-00000-tid-7923131282069400718-03d45ef4-9010-4be7-bbe2-ed94928bb60f-2-1-c000.csv"
df = spark.read.format("csv")\
.option("header", "True")\
.option("inferSchema", "True")\
.option("timestampFormat",'yyyy-/MM/DD hh:mm:ss')\
.load(file)

# COMMAND ----------

# DBTITLE 1,Print file
df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing data in parallel

# COMMAND ----------

# DBTITLE 1,Repartitioning the data csv files
df.repartition(5).write.format("csv")\
.mode("overwrite") \
.option("sep", ",") \
.save("/FileStore/tables/bronze/OUT_2010_12_01.csv")
