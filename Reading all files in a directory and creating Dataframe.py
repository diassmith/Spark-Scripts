# Databricks notebook source
# DBTITLE 1,Reading all files in a directory and creating a Dataframe
# CAN USE BELOW
#df = spark.read.option("inferSchema", "True").option("header", "True").csv("/FileStore/tables/bronze/*.csv")

# OR CAN USE THE BELOW, THIS IS THE RECOMMENDED

df = spark\
.read\
.option("inferSchema", "True")\
.option("header", "True")\
.csv("/FileStore/tables/bronze/*.csv")

# COMMAND ----------

# DBTITLE 1,Show me the 5 first rows
################# OUT ###########################
#+-----------------+-------------------+-----+
#|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
#+-----------------+-------------------+-----+
#|    United States|            Romania|    1|
#|    United States|            Ireland|  264|
#|    United States|              India|   69|
#|            Egypt|      United States|   24|
#|Equatorial Guinea|      United States|    1|
#+-----------------+-------------------+-----+



df.show(5)

# COMMAND ----------

# DBTITLE 1,Print the amount lines in dataframe
# imprime a quantidade de linhas do datafrme
df.count()

# COMMAND ----------

# DBTITLE 1,Plots options with diplay command
display(df.head(10))
