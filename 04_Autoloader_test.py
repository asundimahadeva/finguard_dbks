# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
source_path='/Volumes/finguard/source/fraud_watchlist/source_data/'

# COMMAND ----------

dbutils.fs.ls(source_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog finguard

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create volume if not exists finguard.source.schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- list volumes in the finguard catalog
# MAGIC
# MAGIC show volumes in finguard.source

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete the schema volume and then create directory within the /fraud_watchlist/schema/
# MAGIC
# MAGIC drop volume if exists finguard.source.schema;
# MAGIC     
# MAGIC -- create directory within the /fraud_watchlist/schema/
# MAGIC
# MAGIC -- SQL has no CREATE DIRECTORY command; the directory is created automatically
# MAGIC -- when Auto Loader writes to cloudFiles.schemaLocation, or explicitly via
# MAGIC -- dbutils.fs.mkdirs('/Volumes/finguard/source/fraud_watchlist/schema') in a Python cell
# MAGIC     
# MAGIC -- list the directory within the /fraud_watch

# COMMAND ----------

input_stream=(spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format", "json")
              .option("cloudFiles.schemaLocation", "/Volumes/finguard/source/fraud_watchlist/schema/")
              .option("cloudFiles.inferColumnTypes","true")
              .load(source_path)
)


# COMMAND ----------

from pyspark.sql import functions as F
tranformed_df=input_stream.select(
"*",
F.col("_metadata.file_path").alias("file_path"),
F.current_timestamp().alias("ingestion_timestamp")
)

# COMMAND ----------

streaming_query=(tranformed_df.writeStream.format("delta")
.outputMode("Append")
.option("checkpointLocation", "/Volumes/finguard/source/fraud_watchlist/checkpoint/")
.trigger(availableNow=True)
.toTable("finguard.bronze.fraud_watchlist_batch_test")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.fraud_watchlist_batch_test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.silver.fraud_watchlist