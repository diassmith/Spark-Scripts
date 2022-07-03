# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC * current_day();
# MAGIC * date_format(dataExpr, format);
# MAGIC * to_date();
# MAGIC * to_date(column, format);
# MAGIC * add_months(column, numMonths);
# MAGIC * data_add(column, days);
# MAGIC * date_sub(column, days);
# MAGIC * datediff(end, start);
# MAGIC * current_timestamp();
# MAGIC * hour(column);

# COMMAND ----------

# DBTITLE 1,Reading .csv file
df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_12_01.csv")

# COMMAND ----------

# DBTITLE 1,Print dataframe
########################### OUT ###############
#+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
#|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
#+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
#|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
#|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
#|   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|
#|   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
#|   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
#+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+

display(df.head(10))

# COMMAND ----------

# DBTITLE 1,Print the Schema
########## OUT ############
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

# DBTITLE 1,Import library
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Current date
##### OUT #####
#+------------+
#|current date|
#+------------+
#|  2022-07-03|
#+------------+

df.select(current_date().alias("current date")).show(1)

# COMMAND ----------

# DBTITLE 1,Formating date
############ OUT ##################
#+-------------------+-------------------+
#|        InvoiceDate|        date_format|
#+-------------------+-------------------+
#|2010-12-01 08:26:00|01-12-2010 08:26:00|
#+-------------------+-------------------+
df.select(col("InvoiceDate"), date_format(col("InvoiceDate"), "dd-MM-yyyy hh:mm:ss").alias("date_format")).show(1)

# COMMAND ----------

# DBTITLE 1,Count days between 2 dates
##################### OUT ###############
#+-------------------+-------------------------------------+
#|        InvoiceDate|datediff(current_date(), InvoiceDate)|
#+-------------------+-------------------------------------+
#|2010-12-01 08:26:00|                                 4232|
#|2010-12-01 08:26:00|                                 4232|
#|2010-12-01 08:26:00|                                 4232|
#|2010-12-01 08:26:00|                                 4232|
#|2010-12-01 08:26:00|                                 4232|
#+-------------------+-------------------------------------+
df.select(col("InvoiceDate"), datediff(current_date(), col("InvoiceDate")  )).show(5)

# COMMAND ----------

# DBTITLE 1,Count months between dates
#+-------------------+-------------------------------------------------+
#|        InvoiceDate|months_between(current_date(), InvoiceDate, true)|
#+-------------------+-------------------------------------------------+
#|2010-12-01 08:26:00|                                       139.053181|
#|2010-12-01 08:26:00|                                       139.053181|
#|2010-12-01 08:26:00|                                       139.053181|
#|2010-12-01 08:26:00|                                       139.053181|
#|2010-12-01 08:26:00|                                       139.053181|
#+-------------------+-------------------------------------------------+
df.select(col("InvoiceDate"), months_between(current_date(), col("InvoiceDate")  )).show(5)

# COMMAND ----------

# DBTITLE 1,Adding and Sub Month and days
#+-------------------+----------+----------+----------+----------+
#|        InvoiceDate| add_month| sub_month|  date_add|  date_sub|
#+-------------------+----------+----------+----------+----------+
#|2010-12-01 08:26:00|2011-03-01|2010-09-01|2010-12-05|2010-11-27|
#|2010-12-01 08:26:00|2011-03-01|2010-09-01|2010-12-05|2010-11-27|
#|2010-12-01 08:26:00|2011-03-01|2010-09-01|2010-12-05|2010-11-27|
#|2010-12-01 08:26:00|2011-03-01|2010-09-01|2010-12-05|2010-11-27|
#|2010-12-01 08:26:00|2011-03-01|2010-09-01|2010-12-05|2010-11-27|
#+-------------------+----------+----------+----------+----------+

df.select(col("InvoiceDate"),
          add_months(col("InvoiceDate"),3).alias("add_month"),
          add_months(col("InvoiceDate"),-3).alias("sub_month"),
          date_add(col("InvoiceDate"),4).alias("date_add"),
          date_sub(col("InvoiceDate"),4).alias("date_sub")
          ).show(5)

# COMMAND ----------

# DBTITLE 1,Getting part from date
########### OUT ############
#+-------------------+----+-----+----------+------------+
# |        InvoiceDate|year|month|  next day|week of year|
# +-------------------+----+-----+----------+------------+
# |2010-12-01 08:26:00|2010|   12|2010-12-05|          48|
# |2010-12-01 08:26:00|2010|   12|2010-12-05|          48|
# |2010-12-01 08:26:00|2010|   12|2010-12-05|          48|
# |2010-12-01 08:26:00|2010|   12|2010-12-05|          48|
# |2010-12-01 08:26:00|2010|   12|2010-12-05|          48|
# +-------------------+----+-----+----------+------------+

df.select(col("InvoiceDate"),
          year(col("InvoiceDate")).alias("year"),
          month(col("InvoiceDate")).alias("month"),
          next_day(col("InvoiceDate"),"Sunday").alias("next day"),
          weekofyear(col("InvoiceDate")).alias("week of year")
          ).show(5)

# COMMAND ----------

# DBTITLE 1,Getting days of WeeK, Month and Year

########### OUT ############
#+-------------------+-----------+------------+-----------+
#|        InvoiceDate|Day of week|day of month|day of year|
#+-------------------+-----------+------------+-----------+
#|2010-12-01 08:26:00|          4|           1|        335|
#|2010-12-01 08:26:00|          4|           1|        335|
#|2010-12-01 08:26:00|          4|           1|        335|
#|2010-12-01 08:26:00|          4|           1|        335|
#|2010-12-01 08:26:00|          4|           1|        335|
#+-------------------+-----------+------------+-----------+

df.select(col("InvoiceDate"),
          dayofweek(col("InvoiceDate")).alias("Day of week"),
          dayofmonth(col("InvoiceDate")).alias("day of month"),
          dayofyear(col("InvoiceDate")).alias("day of year")
          ).show(5)

# COMMAND ----------

# DBTITLE 1,Current Timestamp
########### OUT ##########
#+-----------------------+
#|current_timestamp      |
#+-----------------------+
#|2022-07-03 18:21:42.318|
#+-----------------------+

# COMMAND ----------

# DBTITLE 1,Getting time properties (hour, minute and seconds)
########### OUT ############
#+-------------------+----+------+------+
#|        InvoiceDate|hour|minute|second|
#+-------------------+----+------+------+
#|2010-12-01 08:26:00|   8|    26|     0|
#|2010-12-01 08:26:00|   8|    26|     0|
#|2010-12-01 08:26:00|   8|    26|     0|
#|2010-12-01 08:26:00|   8|    26|     0|
#|2010-12-01 08:26:00|   8|    26|     0|
#+-------------------+----+------+------+

df.select(col("InvoiceDate"),
          hour(col("InvoiceDate")).alias("hour"),
          minute(col("InvoiceDate")).alias("minute"),
          second(col("InvoiceDate")).alias("second")
          ).show(5)
