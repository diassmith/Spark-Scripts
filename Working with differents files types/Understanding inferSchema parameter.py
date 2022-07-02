# Databricks notebook source
# MAGIC %md
# MAGIC #### Creating a schema
# MAGIC - The option **infer_schema** will not always set the best datatype 
# MAGIC - Improves performance when reading large databases.
# MAGIC - Allows customization of column types.
# MAGIC - It is important to know for rewriting applications. (pandas codes)

# COMMAND ----------

# DBTITLE 1,Print the dataframe Schema
 
    
 ########## OUT ###########   
 #|-- InvoiceNo: string (nullable = true)
 #|-- StockCode: string (nullable = true)
 #|-- Description: string (nullable = true)
 #|-- Quantity: integer (nullable = true)
 #|-- InvoiceDate: timestamp (nullable = true)
 #|-- UnitPrice: double (nullable = true)
 #|-- CustomerID: double (nullable = true)
 #|-- Country: string (nullable = true)

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Setting your InfraSchema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, TimestampType
schema_df = StructType([
    StructField("InvoiceNo", IntegerType()),
    StructField("StockCode", IntegerType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("InvoiceDate", TimestampType()),
    StructField("UnitPrice", DoubleType()),
    StructField("CustomerID", DoubleType()),
    StructField("Country", StringType())
])

# COMMAND ----------

# DBTITLE 1,Checking the datatype
type(schema_df)

# COMMAND ----------

# DBTITLE 1,Using the new structType
df = spark.read.format("csv")\
.option("header", "True")\
.schema(schema_df)\
.option("timestampFormat",'yyyy-/MM/DD hh:mm:ss')\
.load("/FileStore/tables/bronze/2010_12_01.csv")

# COMMAND ----------

########## OUT #################
#|-- InvoiceNo: integer (nullable = true)
#|-- StockCode: integer (nullable = true)
#|-- Description: string (nullable = true)
#|-- Quantity: integer (nullable = true)
#|-- InvoiceDate: timestamp (nullable = true)
#|-- UnitPrice: double (nullable = true)
#|-- CustomerID: double (nullable = true)
#|-- Country: string (nullable = true)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Print the Dataframe 
# print 10 rows
display(df.collect())
