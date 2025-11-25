# Databricks notebook source
#Dataframe---> A dataframe in pyspark is distributed collection of data organized in columns, similar to table in RDBMS.
#Lazy evolution means, dataframe is not evaluated until an action is performed.
#step1-->creation of dF, step2-->transformations, step3-->action display, count, show, take,collect, write
#spark will store execution history-->lineage information#

# COMMAND ----------

# DBTITLE 1,create DF
#syntax--> spark.createDataFrame(data, schema)
# help(spark.createDataFrame)
data1=[(1,"sravan"),(2,"deepika"),(3,"naveen"),(4,"deepika"),(5,"sravan"),(6,"deepika"),(7,"sravan"),(8,"deepika"),(9,"sravan"),(10,"deepika")]
schema=["id","name"]
df=spark.createDataFrame(data,schema)
df.display()
df.printSchema()


# COMMAND ----------

help(df.printSchema)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# if you want to define the data types explicitly --use StructType and StructField
help(StructType)

# COMMAND ----------

from pyspark.sql.types import StructField
from pyspark.sql.types import *

# COMMAND ----------

schema1=StructType([StructField("id",IntegerType(),False),StructField("name",StringType(),False)])

# COMMAND ----------

df1=spark.createDataFrame(data1,schema1)
df1.printSchema()
df1.display()

# COMMAND ----------

d=[{"id":1,"name":"sravan"},{"id1":2,"name":"deepika"},{"id":3,"name":"naveen"},{"id":4,"name":"deepika"},{"id":5,"name":"sravan"},{"id":6,"name":"deepika"}]
df1=spark.createDataFrame(d,schema1)
df1.show()
df1.printSchema()

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------



# COMMAND ----------

df=spark.read.csv("/mnt/input/dataSRH.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------


