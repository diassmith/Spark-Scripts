# Databricks notebook source
# DBTITLE 1,Defining the function
from pyspark.sql.types import LongType
def squared(x):
    return x * x

# COMMAND ----------

# DBTITLE 1,Register the function
from pyspark.sql.types import LongType
spark.udf.register("Func_Squared", squared, LongType())

# COMMAND ----------

# DBTITLE 1,Create range
#### OUT ####
# +---+
# | id|
# +---+
# |  1|
# |  2|
# |  3|
# |  4|
# |  5|
# |  6|
# |  7|
# |  8|
# |  9|
# +---+
spark.range(1,10).show()

# COMMAND ----------

# DBTITLE 1,Create a temp view from range
spark.range(1,10).createOrReplaceTempView("temp_view")

# COMMAND ----------

# DBTITLE 1,Using the function on SQL
# MAGIC %sql
# MAGIC select id, Func_Squared(id) as id_squared from temp_view

# COMMAND ----------

# DBTITLE 1,Usin the function on Spark SQL
####### OUT ######
# +---+----------+
# | id|id_squared|
# +---+----------+
# |  1|         1|
# |  2|         4|
# |  3|         9|
# |  4|        16|
# |  5|        25|
# |  6|        36|
# |  7|        49|
# |  8|        64|
# |  9|        81|
# +---+----------+
df = spark.sql("""select id, Func_Squared(id) as id_squared from temp_view""").show()
