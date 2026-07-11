# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Database and Prepare Data
# MAGIC This notebook initializes the database, validates source data, splits the transaction file into landing batches, and pre-creates Delta tables and views.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# MAGIC %run ../configs/pipeline_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")
print(f"Database {DATABASE_NAME} ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Source Files in Volume

# COMMAND ----------

def verify_file(path, label):
    try:
        dbutils.fs.ls(path)
    except Exception:
        raise FileNotFoundError(f"Source file {label} not found at {path}. Please upload it first.")

verify_file(f"{RAW_DATA_PATH}/transaction.csv",      "transaction.csv")
verify_file(f"{RAW_DATA_PATH}/customer_profile.csv", "customer_profile.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Landing Files
# MAGIC Splits the main transactions file into batches to simulate a stream.

# COMMAND ----------

import math
from pyspark.sql.functions import monotonically_increasing_id

transactions_df = (
    spark.read
         .option("header", "true")
         .csv(f"{RAW_DATA_PATH}/transaction.csv")
)
total_rows = transactions_df.count()

dbutils.fs.rm(LANDING_PATH, recurse=True)
dbutils.fs.mkdirs(LANDING_PATH)

benchmark_file_count = NUM_BATCHES

transactions_df = transactions_df.withColumn("_row_id", monotonically_increasing_id())
batch_size = math.ceil(total_rows / benchmark_file_count)

for i in range(benchmark_file_count):
    batch_df = transactions_df.filter(
        (transactions_df._row_id >= i * batch_size) & 
        (transactions_df._row_id < (i + 1) * batch_size)
    ).drop("_row_id")
    
    batch_count = batch_df.count()
    if batch_count == 0:
        continue
    
    # Write to a temp directory, coalesce to 1 to ensure a single output part file
    temp_dir = f"{VOLUME_ROOT}/_temp/batch_{i:02d}"
    batch_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)
    
    temp_files = [f for f in dbutils.fs.ls(temp_dir) if f.name.startswith("part-") and f.name.endswith(".csv")]
    if temp_files:
        part_file = temp_files[0].path
        target_path = f"{LANDING_PATH}/batch_{i:02d}.csv"
        dbutils.fs.cp(part_file, target_path)
    
    dbutils.fs.rm(temp_dir, recurse=True)

print(f"Staged {benchmark_file_count} batch files in the landing zone.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy Customer Profile Reference

# COMMAND ----------

customer_df = (
    spark.read
         .option("header", "true")
         .csv(f"{RAW_DATA_PATH}/customer_profile.csv")
)

dbutils.fs.mkdirs(CUSTOMER_PROFILE_PATH)
dbutils.fs.cp(f"{RAW_DATA_PATH}/customer_profile.csv", f"{CUSTOMER_PROFILE_PATH}/customer_profile.csv")

print("Customer profile staged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Checkpoints and Target Tables
# MAGIC Pre-create checkpoint directories and Delta tables so downstream streaming jobs do not perform DDL operations in their execution path.

# COMMAND ----------

dbutils.fs.rm(CHECKPOINT_BASE, recurse=True)
dbutils.fs.mkdirs(CHECKPOINT_BASE)

checkpoint_dirs = {
    "bronze": BRONZE_CHECKPOINT,
    "silver": SILVER_CHECKPOINT,
    "gold_alerts": GOLD_ALERTS_CHECKPOINT,
    "gold_risk_profile": GOLD_RISK_CHECKPOINT,
}

for path in checkpoint_dirs.values():
    dbutils.fs.mkdirs(path)

# Pre-creating schema structures to simplify streaming queries
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    transaction_id STRING,
    customer_id STRING,
    card_id STRING,
    transaction_time TIMESTAMP,
    amount DOUBLE,
    merchant STRING,
    merchant_category STRING,
    location STRING,
    _rescued_data STRING,
    ingest_timestamp TIMESTAMP,
    source_file STRING,
    batch_id INT
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    transaction_id STRING,
    customer_id STRING,
    card_id STRING,
    transaction_time TIMESTAMP,
    amount DOUBLE,
    merchant STRING,
    merchant_category STRING,
    location STRING,
    ingest_timestamp TIMESTAMP,
    source_file STRING,
    batch_id BIGINT,
    home_location STRING,
    avg_spend_per_day DOUBLE,
    preferred_category STRING
) USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

spark.sql(f"ALTER TABLE {SILVER_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_LATE_TABLE} (
    transaction_id STRING,
    customer_id STRING,
    card_id STRING,
    transaction_time TIMESTAMP,
    amount DOUBLE,
    merchant STRING,
    merchant_category STRING,
    location STRING,
    ingest_timestamp TIMESTAMP,
    source_file STRING,
    batch_id BIGINT,
    home_location STRING,
    avg_spend_per_day DOUBLE,
    preferred_category STRING
) USING DELTA
""")

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

# Create helper dashboard views to flatten array properties for visualization tools
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

print("Delta tables and database views initialized.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Landing Zone

# COMMAND ----------

landing_files = dbutils.fs.ls(LANDING_PATH)
batch_csvs = [f for f in landing_files if f.name.endswith(".csv") and not f.isDir()]

print(f"Staged {len(batch_csvs)} files in the landing path.")

# COMMAND ----------

print("Setup completed successfully.")