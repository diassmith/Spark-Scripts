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
df.write.format("parquet")\
.mode("overwrite")\
.save("/FileStore/tables/bigdata/bronze/df-parquet-file.parquet")

# COMMAND ----------

# DBTITLE 1,Consulting parquet file
# MAGIC %fs
# MAGIC ls /FileStore/tables/bigdata/bronze/df-parquet-file.parquet

# COMMAND ----------

# DBTITLE 1,Reading .parquet file
df_parquet = spark.read.format("parquet")\
.load("/FileStore/tables/bigdata/bronze/df-parquet-file.parquet")

# COMMAND ----------

df_parquet.count()

# COMMAND ----------

display(df_parquet.head(10))

# COMMAND ----------

# DBTITLE 1,Showing the size files
display(dbutils.fs.ls("/FileStore/tables/bigdata/bronze/df-parquet-file.parquet"))

# COMMAND ----------

# DBTITLE 1,Getting size in Gigabytes
# MAGIC %scala
# MAGIC val path="/FileStore/tables/bigdata/bronze/df-parquet-file.parquet"
# MAGIC val filelist=dbutils.fs.ls(path)
# MAGIC val df_temp = filelist.toDF()
# MAGIC df_temp.createOrReplaceTempView("adlsSize")

# COMMAND ----------

# DBTITLE 1,Consulting the view created to get the size in GB
# MAGIC %sql
# MAGIC select round(sum(size)/(1024*1024*1024),3) as sizeInGB from adlsSize
