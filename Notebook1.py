# Databricks notebook source
from pyspark.sql.functions import col, round
from pyspark.sql import Row

data = [
    Row(name="Alice", dept="HR", salary=70000),
    Row(name="Bob", dept="Engineering", salary=95000),
    Row(name="Charlie", dept="Marketing", salary=65000),
    Row(name="Diana", dept="Finance", salary=80000),
    Row(name="Eve", dept="Engineering", salary=99000)
]

df = spark.createDataFrame(data)
display(df)

# COMMAND ----------

df.createOrReplaceGlobalTempView("emp20")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.emp20

# COMMAND ----------


