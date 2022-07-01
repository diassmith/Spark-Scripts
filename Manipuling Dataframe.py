# Databricks notebook source
# DBTITLE 1,order 5 lines from data frame 
#+-----------------+-------------------+-----+
#|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
#+-----------------+-------------------+-----+
#|       Kazakhstan|      United States|    1|
#|    United States|         Azerbaijan|    1|
#|    United States|         The Gambia|    1|
#|    United States|    Solomon Islands|    1|
#|    United States|               Togo|    1|
#+-----------------+-------------------+-----+

df.sort("count").show(5)

# COMMAND ----------

# DBTITLE 1,Ascending order / descending order
#+--------------------+-------------------+-----+
#|   DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
#+--------------------+-------------------+-----+
#|       United States|            Algeria|    1|
#|            Malaysia|      United States|    1|
#|          Azerbaijan|      United States|    1|
#|       United States|             Cyprus|    1|
#|             Liberia|      United States|    1|
#|Saint Vincent and...|      United States|    1|
#|       United States|             Serbia|    1|
#|       United States|            Vietnam|    1|
#|   Equatorial Guinea|      United States|    1|
#|       United States|            Estonia|    1|
#+--------------------+-------------------+-----+

from pyspark.sql.functions import desc, asc, expr
# ascending order 
df.orderBy(expr("count desc")).show(10)
df.orderBy(expr("count asc")).show(10)



# COMMAND ----------

# DBTITLE 1,Describe the column
# literally describes the column 
df.describe().show()

# COMMAND ----------

# DBTITLE 1,Scrolling a list from dataframe
# it's like display(df)

for i in df.collect():
  #print (i)
  print(i[0], i[1], i[2])
