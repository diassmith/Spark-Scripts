# Databricks notebook source
# DBTITLE 1,Reading .csv file
df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/FileStore/tables/bronze/2010_12_01.csv")

# COMMAND ----------


######################## OUT ######################
#+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
#|InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
#+---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
#|   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
#|   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
#   536365|   84406B|CREAM CUPID HEART...|       8|2010-12-01 08:26:00|     2.75|   17850.0|United Kingdom|
#   536365|   84029G|KNITTED UNION FLA...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
#   536365|   84029E|RED WOOLLY HOTTIE...|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
#   536365|    22752|SET 7 BABUSHKA NE...|       2|2010-12-01 08:26:00|     7.65|   17850.0|United Kingdom|
#   536365|    21730|GLASS STAR FROSTE...|       6|2010-12-01 08:26:00|     4.25|   17850.0|United Kingdom|
#   536366|    22633|HAND WARMER UNION...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
#|   536366|    22632|HAND WARMER RED P...|       6|2010-12-01 08:28:00|     1.85|   17850.0|United Kingdom|
#|   536367|    84879|ASSORTED COLOUR B...|      32|2010-12-01 08:34:00|     1.69|   13047.0|United Kingdom|
#---------+---------+--------------------+--------+-------------------+---------+----------+--------------+

df.show(10)
display(df.head(10))


# COMMAND ----------

# DBTITLE 1,SUM
############ OUT ##########
#+--------------+------------------+
#|       Country|    sum(UnitPrice)|
#+--------------+------------------+
#|       Germany| 93.82000000000002|
#|        France|             55.29|
#|          EIRE|133.64000000000001|
#|        Norway|            102.67|
#|     Australia|              73.9|
#|United Kingdom|12428.080000000024|
#|   Netherlands|             16.85|
#+--------------+------------------+

df.groupBy("Country").sum("UnitPrice").show()

# COMMAND ----------

# DBTITLE 1,COUNT
########### OUT ######
#+--------------+-----+
#|       Country|count|
#+--------------+-----+
#|       Germany|   29|
#|        France|   20|
#|          EIRE|   21|
#|        Norway|   73|
#|     Australia|   14|
#|United Kingdom| 2949|
#|   Netherlands|    2|
#+--------------+-----+

df.groupBy("Country").count().show()

# COMMAND ----------

# DBTITLE 1,MIN
######### OUT ###########
#+--------------+--------------+
#|       Country|min(UnitPrice)|
#+--------------+--------------+
#|       Germany|          0.42|
#|        France|          0.42|
#|          EIRE|          0.65|
#|        Norway|          0.29|
#|     Australia|          0.85|
#|United Kingdom|           0.0|
#|   Netherlands|          1.85|
#+--------------+--------------+

df.groupBy("Country").min("UnitPrice").show()

# COMMAND ----------

# DBTITLE 1,MAX
########### OUT ###########
#+--------------+--------------+
#|       Country|max(UnitPrice)|
#+--------------+--------------+
#|       Germany|          18.0|
#|        France|          18.0|
#|          EIRE|          50.0|
#|        Norway|          7.95|
#|     Australia|           8.5|
#|United Kingdom|        607.49|
#|   Netherlands|          15.0|
#+--------------+--------------+

df.groupBy("Country").max("UnitPrice").show()

# COMMAND ----------

# DBTITLE 1,AVERAGE
########### OUT ############
#+--------------+------------------+
#|       Country|    avg(UnitPrice)|
#+--------------+------------------+
#|       Germany| 3.235172413793104|
#|        France|            2.7645|
#|          EIRE|6.3638095238095245|
#|        Norway|1.4064383561643836|
#|     Australia| 5.278571428571429|
#|United Kingdom|4.2143370634113335|
#|   Netherlands|             8.425|
#+--------------+------------------+

df.groupBy("Country").avg("UnitPrice").show()

# COMMAND ----------

# DBTITLE 1,AVERAGE BUT USING "MEAN"

############## OUT ################
#+--------------+------------------+
#|       Country|    avg(UnitPrice)|
#+--------------+------------------+
#|       Germany| 3.235172413793104|
#|        France|            2.7645|
#|          EIRE|6.3638095238095245|
#|        Norway|1.4064383561643836|
#|     Australia| 5.278571428571429|
#|United Kingdom|4.2143370634113335|
#|   Netherlands|             8.425|
#+--------------+------------------+

df.groupBy("Country").mean("UnitPrice").show()

# COMMAND ----------

# DBTITLE 1,GROUP BY 2 FACTORS 
########### OUT #################
#+--------------+----------+------------------+
#|       Country|CustomerID|    sum(UnitPrice)|
#+--------------+----------+------------------+
#|United Kingdom|   17420.0| 38.99999999999999|
#|United Kingdom|   15922.0|              48.5|
#|United Kingdom|   16250.0|             47.27|
#|United Kingdom|   13065.0| 73.11000000000001|
#|United Kingdom|   18074.0|62.150000000000006|
#|United Kingdom|   16048.0|12.969999999999999|
#|       Germany|   12472.0|             49.45|
#|United Kingdom|   18085.0|              34.6|
#|United Kingdom|   17905.0|109.90000000000003|
#|United Kingdom|   17841.0|254.63999999999982|
#|United Kingdom|   15291.0|               6.0|
#|United Kingdom|   17951.0|22.000000000000004|
#|United Kingdom|   13255.0|27.299999999999997|
#|United Kingdom|   17690.0|              34.8|
#|United Kingdom|   18229.0|             48.65|
#|United Kingdom|   15605.0| 58.20000000000002|
#|United Kingdom|   18011.0| 66.10999999999999|
#|United Kingdom|   17809.0|              1.45|
#|United Kingdom|   14307.0|115.35000000000004|
#|United Kingdom|   13705.0|183.98999999999998|
#+--------------+----------+------------------+

df.groupBy("Country","CustomerID").max("UnitPrice").show()
