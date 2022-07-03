# Databricks notebook source
# DBTITLE 1,Coverting .csv to .parquet
# MAGIC %md
# MAGIC - Dataset .csv used  https://www.kaggle.com/nhs/general-practice-prescribing-data

# COMMAND ----------

# DBTITLE 1,Reading the all files
# Lendo todos os arquivos .csv do diretório bigdata (>4GB)
df = spark.read.format("csv")\
.option("header", "True")\
.option("inferSchema","True")\
.load("/FileStore/tables/bigdata/*.csv")

# COMMAND ----------

# DBTITLE 1,Print 10 rows from the data frame
display(df.head(10))

# COMMAND ----------

# DBTITLE 1,Print inferSchema 
############ OUT ###########
 #|-- practice: integer (nullable = true)
 #|-- bnf_code: integer (nullable = true)
 #|-- bnf_name: integer (nullable = true)
 #|-- items: integer (nullable = true)
 #|-- nic: double (nullable = true)
 #|-- act_cost: double (nullable = true)
 #|-- quantity: integer (nullable = true)

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Count rows from dataframe
###### OUT ####
# 131004583
df.count()

# COMMAND ----------

# DBTITLE 1,Writing parquet file
# escrevendo em formato parquet
df.write.format("parquet")\
.mode("overwrite")\
.save("/FileStore/tables/bigdata/bronze/df-parquet-file.parquet")

# COMMAND ----------

# DBTITLE 1,Consulting parquet file
# MAGIC %fs
# MAGIC ls /FileStore/tables/bigdata/bronze/df-parquet-file.parquet
