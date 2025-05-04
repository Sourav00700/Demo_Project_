# Databricks notebook source
# MAGIC %md
# MAGIC #Source :Databricks notebook 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Ingestion and Tracking CDC(Change Data Capture) in Databricks

# COMMAND ----------

# Ingesting mock JSON data into Delta Lake, simulating a Change Data Capture (CDC) process with inserts/updates, and creating a separate change log.

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable

# Initialize Spark
spark = SparkSession.builder.appName("CDC Pipeline Demo").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Loading Mock JSON Data

# COMMAND ----------

# Mock data
mock_data = [
    {"id": 1, "product": "Sugar", "mrp": 70},
    {"id": 2, "product": "Oil", "mrp": 110},
    {"id": 3, "product": "Salt", "mrp": 50}
]

df = spark.createDataFrame(mock_data)
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Writing to Delta Lake (Initial load)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/tmp/delta/user")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Simulating CDC Updates / Inserts

# COMMAND ----------

cdc_data = [
    {"id": 2, "product": "Chocolate", "mrp": 85},   # update
    {"id": 4, "product": "Perfume", "mrp": 250} # insert
]

cdc_df = spark.createDataFrame(cdc_data)
cdc_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Merging Changes (Delta Lake merge)

# COMMAND ----------

delta_table = DeltaTable.forPath(spark, "/tmp/delta/user")

(delta_table.alias("target")
 .merge(
     cdc_df.alias("source"),
     "target.id = source.id"
 )
 .whenMatchedUpdate(set={
     "product": "source.product",
     "mrp": "source.mrp"
 })
 .whenNotMatchedInsert(values={
     "id": "source.id",
     "product": "source.product",
     "mrp": "source.mrp"
 })
 .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Creating a Change Log Table

# COMMAND ----------

change_log = (cdc_df.withColumn("change_type", lit("upsert"))
                       .withColumn("change_time", current_timestamp()))

change_log.write.format("delta").mode("append").save("/tmp/delta/change_lg")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Step 6: Validating Change Log

# COMMAND ----------

change_log_df = spark.read.format("delta").load("/tmp/delta/change_lg")
change_log_df.display()
