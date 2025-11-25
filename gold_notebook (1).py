# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

# MAGIC %md
# MAGIC # Database Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE gold1

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading and Writing and CRAETING delta tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Storage Variables**

# COMMAND ----------

silver = '/mnt/silver'
gold = '/mnt/gold'

# COMMAND ----------

# MAGIC %md
# MAGIC **DATA ZONE**

# COMMAND ----------

df_zone = spark.read.format('parquet')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load(f'{silver}/trip_zone')

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format('delta')\
        .mode('append')\
        .option('path',f'{gold}/trip_zone')\
        .saveAsTable('gold1.trip_zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold1.trip_zone
# MAGIC where Borough = 'EWR'

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

df_type = spark.read.format('parquet')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load(f'{silver}/trip_type')

# COMMAND ----------

df_type.write.format('delta')\
        .mode('append')\
        .option('path',f'{gold}/trip_type')\
        .saveAsTable('gold1.trip_type')

# COMMAND ----------

# MAGIC %md
# MAGIC **Trips Data**

# COMMAND ----------

df_trip = spark.read.format('parquet')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load(f'{silver}/trips2023data')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('delta')\
        .mode('append')\
        .option('path',f'{gold}/tripsdata')\
        .saveAsTable('gold1.trip_trip')

# COMMAND ----------

# MAGIC %md
# MAGIC # Learning Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone 
# MAGIC where LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold.trip_zone 
# MAGIC SET Borough = 'EMR' where LocationID = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM gold.trip_zone 
# MAGIC WHERE LocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone
# MAGIC where LocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Time Travel**

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE gold.trip_zone TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_type

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data 2023**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_trip

# COMMAND ----------


