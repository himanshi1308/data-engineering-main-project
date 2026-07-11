# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Processing
# MAGIC Consumes raw data from Bronze, cleans schemas, validates fields, enriches transactions with customer profile reference data, routes late-arriving records, and merges valid entries into the Silver Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configurations

# COMMAND ----------

# MAGIC %run ../configs/pipeline_config

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, current_timestamp, max, broadcast, expr
from pyspark.sql.types import DoubleType, LongType
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Target Tables

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE {SILVER_TABLE}")
spark.sql(f"DESCRIBE TABLE {SILVER_LATE_TABLE}")
print("Silver tables validated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Customer Profiles

# COMMAND ----------

customer_df = (
    spark.read
         .option("header", "true")
         .schema(CUSTOMER_SCHEMA)
         .csv(f"{CUSTOMER_PROFILE_PATH}/customer_profile.csv")
)
print("Loaded customer profiles:")
customer_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Ingestion and Cleaning Stream

# COMMAND ----------

bronze_stream = spark.readStream.table(BRONZE_TABLE)

cleaned_stream = (
    bronze_stream
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("transaction_time", to_timestamp(col("transaction_time"), TIMESTAMP_FORMAT))
        .withColumn("batch_id", col("batch_id").cast(LongType()))
        .withWatermark("transaction_time", WATERMARK_DURATION)
        .filter(
            col("transaction_id").isNotNull() &
            col("customer_id").isNotNull() &
            col("amount").isNotNull() &
            (col("amount") > 0.0) &
            col("location").isNotNull() &
            (col("transaction_time") <= current_timestamp())
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Micro-Batches

# COMMAND ----------

def process_silver_batch(batch_df, batch_id):
    batch_df = safe_cache(batch_df)
    
    deduped_df = batch_df.dropDuplicates(["transaction_id"])
    
    enriched_df = (
        deduped_df
            .drop("_rescued_data")
            .join(broadcast(customer_df), on="customer_id", how="inner")
            .select(
                "transaction_id", "customer_id", "card_id", "transaction_time",
                "amount", "merchant", "merchant_category", "location",
                "ingest_timestamp", "source_file", "batch_id",
                "home_location", "avg_spend_per_day", "preferred_category"
            )
    )
    enriched_df = safe_cache(enriched_df)
    
    # Avoiding .collect() so we don't choke the driver. Calculating max time directly on the executors instead.
    max_time_df = enriched_df.select(max("transaction_time").alias("max_time"))
    enriched_with_max = enriched_df.crossJoin(broadcast(max_time_df))
    
    # Filter valid vs late records using the watermark cutoff
    valid_df = enriched_with_max.filter(
        (col("max_time").isNull()) | 
        (col("transaction_time") >= col("max_time") - expr(f"INTERVAL {WATERMARK_DURATION}"))
    ).drop("max_time")
    
    late_df = enriched_with_max.filter(
        (col("max_time").isNotNull()) & 
        (col("transaction_time") < col("max_time") - expr(f"INTERVAL {WATERMARK_DURATION}"))
    ).drop("max_time")
        
    (late_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(SILVER_LATE_TABLE))

    silver_table = DeltaTable.forName(spark, SILVER_TABLE)

    (silver_table.alias("target")
         .merge(
             valid_df.alias("source"),
             "target.transaction_id = source.transaction_id"
          )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

    safe_unpersist(enriched_df)
    safe_unpersist(batch_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Streaming Job

# COMMAND ----------

silver_query = (
    cleaned_stream.writeStream
        .format("delta")
        .foreachBatch(process_silver_batch)
        .option("checkpointLocation", SILVER_CHECKPOINT)
        .trigger(availableNow=True)
        .start()
)

silver_query.awaitTermination()
print("Silver processing stream complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Results

# COMMAND ----------

silver_df = spark.read.table(SILVER_TABLE)
late_df = spark.read.table(SILVER_LATE_TABLE)

print(f"Transactions in Silver: {silver_df.count()}")
print(f"Late arrivals: {late_df.count()}")

silver_df.printSchema()