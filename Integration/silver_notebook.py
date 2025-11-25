# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing Libraries**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading CSV DATA**

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type Data**

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                    .option('inferSchema',True)\
                    .option('header',True)\
                    .load('/mnt/bronze/trip-type/trip_type.csv')

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
                    .option('inferSchema',True)\
                    .option('header',True)\
                    .load('/mnt/bronze/trip-zone/taxi_zone_lookup.csv')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

myschema = '''
                VendorID BIGINT,
                lpep_pickup_datetime TIMESTAMP,
                lpep_dropoff_datetime TIMESTAMP,
                store_and_fwd_flag STRING,
                RatecodeID BIGINT,
                PULocationID BIGINT,
                DOLocationID BIGINT,
                passenger_count BIGINT,
                trip_distance DOUBLE,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                ehail_fee DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                payment_type BIGINT,
                trip_type BIGINT,
                congestion_surcharge DOUBLE

      '''

# COMMAND ----------

df_trip = spark.read.format('parquet')\
              .schema(myschema)\
              .option('header',True)\
              .option('recursiveFileLookup',True)\
              .load('/mnt/bronze/trip-data/')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC **Taxi Trip Type**

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed('description','trip_description')
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format('parquet')\
            .mode('append')\
            .option("path", "/mnt/silver/trip_type")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('zone1',split(col('Zone'),'/')[0])\
                            .withColumn('zone2',split(col('Zone'),'/')[1])

df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
          .mode('append')\
          .option('path','/mnt/silver/trip_zone')\
          .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date',to_date('lpep_pickup_datetime'))\
                  .withColumn('trip_year',year('lpep_pickup_datetime'))\
                  .withColumn('trip_month',month('lpep_pickup_datetime'))
                  

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.select('VendorID','PULocationID','DOLocationID','fare_amount','total_amount','trip_date','trip_year','trip_month')
df_trip.display()

# COMMAND ----------

df_trip.write.format('parquet')\
            .mode('append')\
            .option('path','/mnt/silver/trips2023data')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis

# COMMAND ----------

display(df_trip)

# COMMAND ----------


