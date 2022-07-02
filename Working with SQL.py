# Databricks notebook source
# MAGIC %md
# MAGIC #Working with SQL

# COMMAND ----------

# DBTITLE 1,Reading all files in a directory and creating a Dataframe
#df = spark.read.option("inferSchema", "True").option("header", "True").csv("/FileStore/tables/stack/BigData/bronze/*.csv")

df = spark\
.read\
.option("inferSchema", "True")\
.option("header", "True")\
.csv("/FileStore/tables/bronze/*.csv")

# COMMAND ----------

# DBTITLE 1,Checking if the global table 'all_files' then delete
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS all_files;

# COMMAND ----------

# DBTITLE 1,Creating a global table in Spark data base
# MAGIC %sql
# MAGIC CREATE TABLE all_files
# MAGIC USING csv
# MAGIC OPTIONS (path "/FileStore/tables/bronze/*.csv", header "true")

# COMMAND ----------

# DBTITLE 1,Consulting the global table using SQL
# MAGIC %sql
# MAGIC SELECT * FROM all_files;

# COMMAND ----------

# DBTITLE 1,Counting the rows on global table
# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM all_files;

# COMMAND ----------

# DBTITLE 1,Counting the countries
# MAGIC %sql
# MAGIC -- Consutando dados usando a linguagem SQL
# MAGIC SELECT DEST_COUNTRY_NAME
# MAGIC        ,avg(count) AS Avg_Amount_Country
# MAGIC FROM all_files
# MAGIC GROUP BY DEST_COUNTRY_NAME
# MAGIC ORDER BY DEST_COUNTRY_NAME;

# COMMAND ----------

# DBTITLE 1,Create a view or temp table 
df.createOrReplaceTempView("2015_summary_csv")

# COMMAND ----------

# DBTITLE 1,Consulting the temp table
# MAGIC %sql
# MAGIC select * from 2015_summary_csv

# COMMAND ----------

# DBTITLE 1,Consult doing measures
# MAGIC %sql
# MAGIC -- Query na view 2015_summary_csv com multiplicação.
# MAGIC SELECT DEST_COUNTRY_NAME 
# MAGIC       ,ORIGIN_COUNTRY_NAME
# MAGIC       ,count * 10 as count_mult_ten
# MAGIC FROM 2015_summary_csv
