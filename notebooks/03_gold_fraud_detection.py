# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Fraud Detection
# MAGIC Consumes Silver table changes via Change Data Feed, calculates risk scores across 5 metrics, flags alerts, and aggregates customer risk profiles.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configurations

# COMMAND ----------

# MAGIC %run ../configs/pipeline_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lag, count, sum, avg, max as max_func, min as min_func, 
    hour, array, array_contains, array_distinct, collect_list, 
    array_compact, when, lit, least, current_timestamp, broadcast, expr
)
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-Create Target Gold Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_ALERTS_TABLE} (
    transaction_id STRING,
    customer_id STRING,
    transaction_time TIMESTAMP,
    amount DOUBLE,
    location STRING,
    fraud_risk_score INT,
    fraud_rules_triggered ARRAY<STRING>,
    risk_level STRING,
    alert_generated_at TIMESTAMP
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_RISK_TABLE} (
    customer_id STRING,
    total_transactions LONG,
    total_spend DOUBLE,
    avg_risk_score DOUBLE,
    max_risk_score INT,
    fraud_alert_count LONG,
    top_locations ARRAY<STRING>,
    risk_category STRING,
    last_updated TIMESTAMP
) USING DELTA
""")

print("Gold tables initialized.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Change Data Feed Stream Reader

# COMMAND ----------

silver_stream = (
    spark.readStream
         .format("delta")
         .option("readChangeFeed", "true")
         .option("startingVersion", 0)
         .table(SILVER_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Micro-Batches

# COMMAND ----------

def process_gold_batch(batch_df, batch_id):
    # Process only post-images and inserts from Change Data Feed
    change_events = batch_df.filter(
        col("_change_type").isin(["insert", "update_postimage"])
    )

    if change_events.isEmpty():
        return

    # Extract distinct customer IDs in this batch to scope history lookup
    customer_ids = [row.customer_id for row in change_events.select("customer_id").distinct().collect()]

    # Query historical customer transactions to evaluate sliding rule windows correctly across batches.
    silver_history = (
        spark.read.table(SILVER_TABLE)
             .filter(col("customer_id").isin(customer_ids))
    )

    combined_df = silver_history.union(change_events.drop("_change_type", "_commit_version", "_commit_timestamp")).dropDuplicates(["transaction_id"])
    combined_df = safe_cache(combined_df)

    # Rule 1: Location Hop (+35 points)
    hop_window = Window.partitionBy("customer_id").orderBy("transaction_time")
    combined_df = combined_df.withColumn("prev_location", lag("location", 1).over(hop_window))
    combined_df = combined_df.withColumn("prev_time", lag("transaction_time", 1).over(hop_window))
    
    time_diff_sec = (col("transaction_time").cast("long") - col("prev_time").cast("long"))
    location_hop_cond = (
        col("prev_location").isNotNull() & 
        (col("location") != col("prev_location")) & 
        (time_diff_sec <= LOCATION_HOP_WINDOW_MINUTES * 60)
    )

    # Rule 2: High Amount Anomaly (+30 points)
    high_amount_cond = col("amount") > (col("avg_spend_per_day") * 5)

    # Rule 3: Unusual Merchant Category (+20 points)
    unusual_category_cond = (
        (col("merchant_category") != col("preferred_category")) & 
        (col("amount") > (col("avg_spend_per_day") * 2))
    )

    # Rule 4: Late Night Transaction (+10 points)
    late_night_cond = hour(col("transaction_time")).between(LATE_NIGHT_START_HOUR, LATE_NIGHT_END_HOUR)

    # Rule 5: Multiple Transactions (+15 points)
    multi_window = (
        Window.partitionBy("customer_id")
              .orderBy(col("transaction_time").cast("long"))
              .rangeBetween(-MULTI_TXN_WINDOW_MINUTES * 60, 0)
    )
    combined_df = combined_df.withColumn("txn_count_15m", count("transaction_id").over(multi_window))
    multi_txn_cond = col("txn_count_15m") > MULTI_TXN_COUNT_THRESHOLD

    scored_df = combined_df.withColumn(
        "rules_list",
        array(
            when(location_hop_cond, lit("Location Hop")).otherwise(lit(None)),
            when(high_amount_cond, lit("High Amount Anomaly")).otherwise(lit(None)),
            when(unusual_category_cond, lit("Unusual Merchant Category")).otherwise(lit(None)),
            when(late_night_cond, lit("Late Night Transaction")).otherwise(lit(None)),
            when(multi_txn_cond, lit("Multiple Transactions")).otherwise(lit(None))
        )
    )
    scored_df = scored_df.withColumn("fraud_rules_triggered", array_compact(col("rules_list"))).drop("rules_list")

    scored_df = scored_df.withColumn(
        "fraud_risk_score",
        least(
            lit(MAX_FRAUD_SCORE),
            (
                when(array_contains(col("fraud_rules_triggered"), "Location Hop"), lit(WEIGHT_LOCATION_HOP)).otherwise(lit(0)) +
                when(array_contains(col("fraud_rules_triggered"), "High Amount Anomaly"), lit(WEIGHT_HIGH_AMOUNT)).otherwise(lit(0)) +
                when(array_contains(col("fraud_rules_triggered"), "Unusual Merchant Category"), lit(WEIGHT_UNUSUAL_MERCH)).otherwise(lit(0)) +
                when(array_contains(col("fraud_rules_triggered"), "Late Night Transaction"), lit(WEIGHT_LATE_NIGHT)).otherwise(lit(0)) +
                when(array_contains(col("fraud_rules_triggered"), "Multiple Transactions"), lit(WEIGHT_MULTI_TXN)).otherwise(lit(0))
            ).cast("int")
        )
    )

    scored_df = scored_df.withColumn(
        "risk_level",
        when(col("fraud_risk_score") < RISK_LOW_MAX, lit("LOW"))
        .when(col("fraud_risk_score") < RISK_MEDIUM_MAX, lit("MEDIUM"))
        .when(col("fraud_risk_score") < RISK_HIGH_MAX, lit("HIGH"))
        .otherwise(lit("CRITICAL"))
    )
    scored_df = safe_cache(scored_df)

    # 1. Merge alerts (score >= 70)
    alerts_df = (
        scored_df.join(change_events.select("transaction_id").distinct(), on="transaction_id", how="inner")
                 .filter(col("fraud_risk_score") >= FRAUD_ALERT_THRESHOLD)
                 .withColumn("alert_generated_at", current_timestamp())
                 .select(
                     "transaction_id", "customer_id", "transaction_time", "amount", "location",
                     "fraud_risk_score", "fraud_rules_triggered", "risk_level", "alert_generated_at"
                 )
    )

    if not alerts_df.isEmpty():
        alerts_table = DeltaTable.forName(spark, GOLD_ALERTS_TABLE)
        (alerts_table.alias("target")
             .merge(
                 alerts_df.alias("source"),
                 "target.transaction_id = source.transaction_id"
             )
             .whenMatchedUpdateAll()
             .whenNotMatchedInsertAll()
             .execute())

    # 2. Merge rolled-up aggregations to gold_customer_risk_profile
    # Limit customer history to 24 hours to bound the aggregation state size and prevent unbounded growth.
    max_history_time = scored_df.select(max_func("transaction_time").alias("global_max_time"))
    windowed_scored_df = (
        scored_df
            .crossJoin(broadcast(max_history_time))
            .filter(
                (col("global_max_time").isNull()) |
                (col("transaction_time") >= col("global_max_time") - expr(f"INTERVAL {CUSTOMER_RISK_WINDOW_HOURS} HOURS"))
            )
            .drop("global_max_time")
    )
    windowed_scored_df = safe_cache(windowed_scored_df)

    profile_df = (
        windowed_scored_df
            .groupBy("customer_id")
            .agg(
                count("transaction_id").alias("total_transactions"),
                sum("amount").alias("total_spend"),
                avg("fraud_risk_score").alias("avg_risk_score"),
                max_func("fraud_risk_score").alias("max_risk_score"),
                sum(when(col("fraud_risk_score") >= FRAUD_ALERT_THRESHOLD, 1).otherwise(0)).alias("fraud_alert_count"),
                array_distinct(collect_list("location")).alias("top_locations"),
                current_timestamp().alias("last_updated")
            )
            .withColumn(
                "risk_category",
                when(col("avg_risk_score") < RISK_LOW_MAX, lit("LOW"))
                .when(col("avg_risk_score") < RISK_MEDIUM_MAX, lit("MEDIUM"))
                .otherwise(lit("HIGH"))
            )
    )
 
    profile_table = DeltaTable.forName(spark, GOLD_RISK_TABLE)
    (profile_table.alias("target")
         .merge(
             profile_df.alias("source"),
              "target.customer_id = source.customer_id"
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
 
    safe_unpersist(alerts_df)
    safe_unpersist(windowed_scored_df)
    safe_unpersist(scored_df)
    safe_unpersist(combined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start Gold Processing Stream

# COMMAND ----------

gold_query = (
    silver_stream.writeStream
         .format("delta")
         .foreachBatch(process_gold_batch)
         .option("checkpointLocation", GOLD_ALERTS_CHECKPOINT)
         .trigger(availableNow=True)
         .start()
)

gold_query.awaitTermination()
print("Gold stream processing complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold Tables

# COMMAND ----------

RUN_MAINTENANCE_OPTIMIZATION = True
 
if RUN_MAINTENANCE_OPTIMIZATION:
    print("Optimizing Gold tables...")
    spark.sql(f"OPTIMIZE {GOLD_ALERTS_TABLE} ZORDER BY (customer_id, transaction_time)")
    spark.sql(f"OPTIMIZE {GOLD_RISK_TABLE} ZORDER BY (customer_id)")
    print("Z-ordering complete.")

print("Refreshing dashboard views...")
spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_ALERTS_VIEW} AS
SELECT 
    transaction_id,
    customer_id,
    transaction_time,
    amount,
    location,
    fraud_risk_score,
    array_join(fraud_rules_triggered, ', ') AS rules_triggered,
    risk_level,
    alert_generated_at
FROM {GOLD_ALERTS_TABLE}
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_RISK_VIEW} AS
SELECT 
    customer_id,
    total_transactions,
    total_spend,
    round(avg_risk_score, 2) AS avg_risk_score,
    max_risk_score,
    fraud_alert_count,
    array_join(top_locations, ', ') AS top_locations_list,
    risk_category,
    last_updated
FROM {GOLD_RISK_TABLE}
""")
print("Dashboard views refreshed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Results

# COMMAND ----------

alerts_df = spark.read.table(GOLD_ALERTS_TABLE)
profiles_df = spark.read.table(GOLD_RISK_TABLE)

print(f"Total fraud alerts: {alerts_df.count()}")
print(f"Total customer risk profiles: {profiles_df.count()}")

# Display sample alerts
display(alerts_df.orderBy(col("fraud_risk_score").desc()).limit(5))