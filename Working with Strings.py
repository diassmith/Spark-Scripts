# Databricks notebook source
# DBTITLE 1,Transforming a column in UPPER CASE OR LOWER CASE using Spark


################ OUT ##############################
#+-----------------+------------------------+-------------------------------+
#|DEST_COUNTRY_NAME|lower(DEST_COUNTRY_NAME)|upper(lower(DEST_COUNTRY_NAME))|
#+-----------------+------------------------+-------------------------------+
#|    United States|           united states|                  UNITED STATES|
#|    United States|           united states|                  UNITED STATES|
#|    United States|           united states|                  UNITED STATES|
#|            Egypt|                   egypt|                          EGYPT|
#|Equatorial Guinea|       equatorial guinea|              EQUATORIAL GUINEA|
#+-----------------+------------------------+-------------------------------+


#col to know and to inform that it's a column
# import the functions LOWER AND UPPER
from pyspark.sql.functions import lower, upper, col
df.select(col("DEST_COUNTRY_NAME"),lower(col("DEST_COUNTRY_NAME")),upper(lower(col("DEST_COUNTRY_NAME")))).show(5)

# COMMAND ----------

# DBTITLE 1,Transforming a column in UPPER CASE OR LOWER CASE using SQL
# MAGIC %sql
# MAGIC 
# MAGIC -- to execute this command, you need execute a tem view or table
# MAGIC --df.createOrReplaceTempView("2015_summary_csv")
# MAGIC -- Using SQL..
# MAGIC SELECT DEST_COUNTRY_NAME
# MAGIC       ,lower(DEST_COUNTRY_NAME)
# MAGIC       ,Upper(DEST_COUNTRY_NAME)
# MAGIC FROM 2015_summary_csv

# COMMAND ----------

# DBTITLE 1,Removing the whitespace on the left side
# importing the ltrim function
from pyspark.sql.functions import ltrim
df.select(ltrim(col("DEST_COUNTRY_NAME"))).show(2)

# COMMAND ----------

# DBTITLE 1,Removing the whitespace on the right side
# remove espaços a direita
from pyspark.sql.functions import rtrim
df.select(rtrim(col("DEST_COUNTRY_NAME"))).show(2)

# COMMAND ----------

# DBTITLE 1,RTemovinf whitesapace
################## OUTTT #############
#+------+------+-----+---+----------+
#| ltrim| rtrim| trim| lp|        rp|
#+------+------+-----+---+----------+
#|HELLO | HELLO|HELLO|HEL|HELLO     |
#|HELLO | HELLO|HELLO|HEL|HELLO     |
#+------+------+-----+---+----------+

#The 'lit' function creates a column in the dataframe copy
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

# COMMAND ----------

# DBTITLE 1,Order by the Cout after another column
#+-----------------+-------------------+-----+
#|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
#+-----------------+-------------------+-----+
#|       Azerbaijan|      United States|    1|
#|          Belarus|      United States|    1|
#|          Belarus|      United States|    1|
#|           Brunei|      United States|    1|
#|         Bulgaria|      United States|    1|
#+-----------------+-------------------+-----+

df.orderBy("count", "DEST_COUNTRY_NAME").show(5)

# COMMAND ----------

# DBTITLE 1,Order by two columns a column des and other asc
#+-----------------+-------------------+-----+
#|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
#+-----------------+-------------------+-----+
#|            Malta|      United States|    1|
#|            Yemen|      United States|    1|
#+-----------------+-------------------+-----+

#+-----------------+-------------------+------+
#|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME| count|
#+-----------------+-------------------+------+
#|    United States|      United States|370002|
#|    United States|      United States|358354|
#+-----------------+-------------------+------+

from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)
