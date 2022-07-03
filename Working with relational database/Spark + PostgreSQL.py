# Databricks notebook source
# DBTITLE 1,Connecting and reading  PostgreSQL
# IT'S LIKE TO EXECUTE A QUERY:  select * from pg_catalog.pg_tables

pgDF = spark.read.format("jdbc")\
.option("driver", "org.postgresql.Driver")\
.option("url", "jdbc:postgresql://svr-pg01.postgres.database.azure.com:5432/postgres?user=[USER]&password=[PASSWORD]@&sslmode=require")\
.option("dbtable", "pg_catalog.pg_tables")\
.option("user", "stack").option("password", "[PASSWORD]").load()

# COMMAND ----------

# DBTITLE 1,Print all rows from Dataframe(pgDF) bringing the all tables from schema(pg_catalog)
display(pgDF.collect())

# COMMAND ----------

# DBTITLE 1,Print schename column
##### OUT #########
#+------------------+
#|        schemaname|
#+------------------+
#|information_schema|
#|            public|
#|        pg_catalog|
#+------------------+

pgDF.select("schemaname").distinct().show()

# COMMAND ----------

# DBTITLE 1,Doing a specific consulting
pgDF = spark.read.format("jdbc")\
.option("driver", "org.postgresql.Driver")\
.option("url", "jdbc:postgresql://svr-pg01.postgres.database.azure.com:5432/postgres?user=[USER]&password=[PASSWORD]@&sslmode=require")\
.option("query", "select schemaname,tablename from pg_catalog.pg_tables")\
.option("user", "stack").option("password", "bigdata123").load()

# COMMAND ----------

# DBTITLE 1,Print all table from schema(pg_catalog)
display(pgDF.collect())

# COMMAND ----------

# DBTITLE 1,Print the Dataframe that storing .parquet file data
#+--------+--------+--------+-----+----+--------+--------+
#|practice|bnf_code|bnf_name|items| nic|act_cost|quantity|
#+--------+--------+--------+-----+----+--------+--------+
#|    5668|    8092|     592|    2|44.1|   40.84|     189|
#|    1596|   17512|   16983|    2|1.64|    1.64|      35|
#|    1596|   25587|   16124|    1|1.26|    1.28|      42|
#|    1596|   12551|    1282|    2|0.86|    1.02|      42|
#|    1596|   18938|   10575|    1|1.85|    1.82|      56|
#+--------+--------+--------+-----+----+--------+--------+
df_parquet.show(5)

# COMMAND ----------

# DBTITLE 1,DF to receve the .parquet file data
#As the files has 4GB more and less to store in the postgreSQL this would take time then, I decide just write 10 rows
#Here I'm getting 10 first rows
df_tenrows = df_parquet.limit(10)

# COMMAND ----------

# DBTITLE 1,Creates the "products" table from the "df_tenrows" dataframe data
df_tenrows.write.mode("overwrite")\
.format("jdbc")\
.option("url", "jdbc:postgresql://svr-pg01.postgres.database.azure.com:5432/postgres?user=[USER]&password=[PASSWORD]@&sslmode=require")\
.option("dbtable", "products")\
.option("user", "stack")\
.option("password", "[PASSWORD]")\
.save()

# COMMAND ----------

# DBTITLE 1,Reading the and storing in DF(df_products)
# cria o dataframe df_produtos a partir da tabela criada.
df_products = spark.read.format("jdbc")\
.option("driver", "org.postgresql.Driver")\
.option("url", "jdbc:postgresql://svr-pg01.postgres.database.azure.com:5432/postgres?user=[USER]&password=[PASSWORD]@&sslmode=require")\
.option("dbtable", "produtos")\
.option("user", "stack").option("password", "[PASSWORD]").load()

# COMMAND ----------

# DBTITLE 1,Print df
############# OUT ######################
#+--------+--------+--------+-----+------+--------+--------+
#|practice|bnf_code|bnf_name|items|   nic|act_cost|quantity|
#+--------+--------+--------+-----+------+--------+--------+
#|    3626|   12090|   20521|    3|   8.4|    7.82|     168|
#|    3626|   23511|   11576|    1| 32.18|   29.81|      28|
#|    3626|   14802|   14672|  162|141.13|  133.93|    4760|
#|    3626|   14590|   10011|   17| 15.01|   14.12|     532|
#|    3626|   24483|   13726|   69| 57.57|   54.67|    2121|
#|    3626|    7768|   22070|  155|113.03|  109.41|    4144|
#|    3626|    1877|   13598|  102|  68.5|    67.4|    2370|
#|    3626|   18110|    3990|  189|156.66|  150.44|    5222|
#|    3626|   14058|    2144|   23| 23.52|   22.48|     588|
#|    3626|    4558|    5695|   32|116.64|  109.21|     756|
#+--------+--------+--------+-----+------+--------+--------+

df_products.show()

# COMMAND ----------

display(df_produtos.collect())
