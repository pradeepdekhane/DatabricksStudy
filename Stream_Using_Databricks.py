# Databricks notebook source
# MAGIC %md
# MAGIC ####Spark Structured Steraming

# COMMAND ----------

# MAGIC %md
# MAGIC 1] Lets import stream data file available in github url:
# MAGIC   https://github.com/pradeepdekhane/DatabricksStudy/tree/main/order_stream
# MAGIC
# MAGIC   Lets import first file orderdetail.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/order_stream/
# MAGIC mkdir /dbfs/order_stream/
# MAGIC wget -O /dbfs/order_stream/orderdetail.csv https://raw.githubusercontent.com/pradeepdekhane/DatabricksStudy/main/order_stream/orderdetail.csv

# COMMAND ----------

#move to container /importGithub/order_stream folder
#this command need not be needed in latest Databricks Edition as mount dbfs is created in local %sh for databricks file system
dbutils.fs.mv(f"file://///dbfs/order_stream/", f"/importGithub/order_stream", recurse=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/importGithub/order_stream/

# COMMAND ----------

# MAGIC %md
# MAGIC 2] Create a stream that reads data from the folder, using a csv schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create a stream that reads data from the folder, using a JSON schema
inputPath = '/importGithub/order_stream/'
orderSchema = StructType([
StructField("orderNumber", StringType(), False),
StructField("productCode", StringType(), False),
StructField("quantityOrdered", StringType(), False),
StructField("priceEach", StringType(), False),
StructField("orderLineNumber", StringType(), False)
])

iotstream = spark.readStream.schema(orderSchema).option("maxFilesPerTrigger", 1).csv(inputPath)
print("Source stream created...")

# COMMAND ----------

# MAGIC %md
# MAGIC 3] write stream of data to a delta table folder

# COMMAND ----------

# Write the stream to a delta table
delta_stream_table_path = '/stg/order_stream'
checkpointpath = '/stg/order_stream/checkpoint'
deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
print("... Streaming to the delta sink ...")

# COMMAND ----------

# MAGIC %md
# MAGIC 4] create a delta table based on the streaming delta table folder:

# COMMAND ----------

# create a catalog table based on the streaming sink
spark.sql("CREATE DATABASE IF NOT EXISTS STG")
spark.sql("CREATE TABLE STG.ORDER_STREAM USING DELTA LOCATION '{0}'".format(delta_stream_table_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM STG.ORDER_STREAM

# COMMAND ----------

# MAGIC %md
# MAGIC 5] Let's add some fresh order data to the stream to know if it load in table

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -r /dbfs/order_stream/
# MAGIC mkdir /dbfs/order_stream/
# MAGIC wget -O /dbfs/order_stream/orderdetail1.csv https://raw.githubusercontent.com/pradeepdekhane/DatabricksStudy/main/order_stream/orderdetail1.csv
# MAGIC wget -O /dbfs/order_stream/orderdetail2.csv https://raw.githubusercontent.com/pradeepdekhane/DatabricksStudy/main/order_stream/orderdetail2.csv

# COMMAND ----------

#move to container /importGithub/order_stream folder
#this command need not be needed in latest Databricks Edition as mount dbfs is created in local %sh for databricks file system
dbutils.fs.mv(f"file://///dbfs/order_stream/", f"/importGithub/order_stream", recurse=True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/importGithub/order_stream/

# COMMAND ----------

# MAGIC %md
# MAGIC 6] Check count of table to see if it is updated

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS CNT FROM STG.ORDER_STREAM

# COMMAND ----------

# MAGIC %md
# MAGIC 7] stop the stream job
# MAGIC earlier count was 2997 new is 8991, so streaming is working properly

# COMMAND ----------

deltastream.stop()
