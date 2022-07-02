# Databricks notebook source
# DBTITLE 1,Reading the dataset in DBFH
df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_12_01.csv")


# COMMAND ----------

# DBTITLE 1,Print the 10 first rows
display(df.head(10))

# COMMAND ----------

# DBTITLE 1,Filter where InvoiceNo <> 536365 

############ OUUUUUTTT #######
# +---------+--------------------+
#|InvoiceNo|         Description|
#+---------+--------------------+
#|   536366|HAND WARMER UNION...|
#|   536366|HAND WARMER RED P...|
#|   536367|ASSORTED COLOUR B...|
#   536367|POPPY'S PLAYHOUSE...|
#|   536367|POPPY'S PLAYHOUSE...|
#+---------+--------------------+
from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
.select("InvoiceNo", "Description")\
.show(5)

# COMMAND ----------

# DBTITLE 1,Creating the temp table
# cria a tabela temporária dftrable
df.createOrReplaceTempView("dfTable")

# COMMAND ----------

# DBTITLE 1,Show me the Temp table
# MAGIC %sql 
# MAGIC select * from dfTable

# COMMAND ----------

# DBTITLE 1,Using the Boolean operator
df.where("InvoiceNo <> 536365").show(5)

# COMMAND ----------

df.where("InvoiceNo = 536365").show(5)

# COMMAND ----------

# DBTITLE 1,Understand the Booleans order
from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1

# COMMAND ----------

# DBTITLE 1,Apply the filters
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

# COMMAND ----------

# DBTITLE 1,Apply the filters in SQL
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM dfTable 
# MAGIC WHERE StockCode in ("DOT")
# MAGIC AND(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)

# COMMAND ----------

# DBTITLE 1,Combine the filter and booleans
####### OUTTT ######
#+---------+-----------+
#|unitPrice|isExpensive|
#+---------+-----------+
#|   569.77|       true|
#|   607.49|       true|
#+---------+-----------+


from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1


# COMMAND ----------

# DBTITLE 1,Apply the filters
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive").show(5)

# COMMAND ----------

# DBTITLE 1,Apply the filters in SQL
# MAGIC %sql
# MAGIC -- Aplicando as mesmas ideias usando SQL
# MAGIC SELECT UnitPrice, (StockCode = 'DOT' AND
# MAGIC (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
# MAGIC FROM dfTable
# MAGIC WHERE (StockCode = 'DOT' AND
# MAGIC (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))
