# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC Ingests raw transaction CSV files from the landing zone into the bronze Delta Lake table using Databricks Auto Loader.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# MAGIC %run ../configs/pipeline_config

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, regexp_extract, when, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Auto Loader Stream

# COMMAND ----------

schema_location = f"{VOLUME_ROOT}/schemas/bronze"

bronze_stream = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("cloudFiles.inferColumnTypes", "true")
         .option("cloudFiles.schemaLocation", schema_location)
         .option("header", "true")
         .load(LANDING_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append Ingestion Metadata

# COMMAND ----------

# Defaulting to batch 999 for test files so Spark doesn't crash trying to cast empty strings.
bronze_enriched_stream = (
    bronze_stream
        .withColumn("ingest_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("batch_id_str", regexp_extract(col("_metadata.file_name"), r"batch_(\d+)\.csv", 1))
        .withColumn("batch_id", when(col("batch_id_str") == "", lit(999)).otherwise(col("batch_id_str").cast("int")))
        .drop("batch_id_str")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Stream to Bronze Delta Table

# COMMAND ----------

query = (
    bronze_enriched_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
)

query.awaitTermination()
print("Ingestion stream completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Ingestion Results

# COMMAND ----------

bronze_df = spark.read.table(BRONZE_TABLE)
row_count = bronze_df.count()
print(f"Total rows in Bronze: {row_count}")

display(bronze_df.limit(5))

# COMMAND ----------

bronze_df.printSchema()