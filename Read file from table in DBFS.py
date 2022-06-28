# Databricks notebook source
# DBTITLE 1,STORING THE FILE PATH IN VARIABLE "file"

#Reading a file from a table in DBFS and storing the variable file path
file = "/FileStore/tables/bronze/2015_summary.csv"

# COMMAND ----------

# DBTITLE 1,Reading a File
#reading the file
# inferSchema = True 
# header = True

#.read.format("csv")\ # type file  (THAT'S NOT MANDATORY)
#.option("inferSchema", "True")\ #getting the data type of the columns ex: string, integer, float etc
#.option("header", "True")\ # getting the columns header 
#.csv(file) #setting the variable that storing the file path

#CAN USE BELLOW
#flightData2015 = spark.read.format("csv").option("inferSchema", "True").option("header", "True").csv(file) 

#OR CAN USE BELLOW BUT IT'S THE RECOMMENDED
flightData2015 = spark\
.read.format("csv")\
.option("inferSchema", "True")\
.option("header", "True")\
.csv(file)

# COMMAND ----------

# DBTITLE 1,Print datatype columns from file
# print the datatypes from the columns from dataframe(file)
flightData2015.printSchema()

# COMMAND ----------

# DBTITLE 1,Print datatype variable 
# print the variable datatype
##### out #####
#pyspark.sql.dataframe.DataFrame

#So, my variables is a Dataframe
type(flightData2015)

# COMMAND ----------

# DBTITLE 1,Print the 3 lines from file
#print the first 3 lines from dataframe as array format

###########OUT##################################
#Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Romania', count=15),
#Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Croatia', count=1),
#Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Ireland', count=344)]

flightData2015.take(3)

# COMMAND ----------

# DBTITLE 1,Print Using display command
#Using the display command I'm going to get the result below

############## OUTTT ###########################
#+-----------------+-------------------+-----+
#|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
#+-----------------+-------------------+-----+
#|    United States|            Romania|   15|
#|    United States|            Croatia|    1|
#|    United States|            Ireland|  344|
#+-----------------+-------------------+-----+


display(flightData2015.show(3))

# COMMAND ----------

# DBTITLE 1,Print the amount lines from file 
#print the amount lines in the dataframe
flightData2015.count()

# COMMAND ----------

# DBTITLE 1,Reading the file inferSchema = False
# read the file but with dataframe set as False

############## OUT ##############
#DEST_COUNTRY_NAME:string
#ORIGIN_COUNTRY_NAME:string
#count:string

flightData2015 = spark\
.read\
.option("inferSchema", "False")\
.option("header", "True")\
.csv(arquivo)
