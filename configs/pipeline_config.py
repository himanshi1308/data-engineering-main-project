# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Configuration
# MAGIC Real-Time Credit Card Fraud Detection Pipeline

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Unity Catalog Volume Base Paths
# Using Unity Catalog Volumes instead of DBFS because DBFS root is disabled in this workspace
VOLUME_ROOT           = "/Volumes/himanshi/raw/data_volume"
RAW_DATA_PATH         = f"{VOLUME_ROOT}/raw"
LANDING_PATH          = f"{VOLUME_ROOT}/data/landing"
CUSTOMER_PROFILE_PATH = f"{VOLUME_ROOT}/data/customer_profile"

# Shared Input Schemas
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("home_location", StringType(), True),
    StructField("avg_spend_per_day", DoubleType(), True),
    StructField("preferred_category", StringType(), True),
])

# Checkpoint Paths
CHECKPOINT_BASE            = f"{VOLUME_ROOT}/checkpoints"
BRONZE_CHECKPOINT          = f"{CHECKPOINT_BASE}/bronze"
SILVER_CHECKPOINT          = f"{CHECKPOINT_BASE}/silver"
GOLD_ALERTS_CHECKPOINT     = f"{CHECKPOINT_BASE}/gold_alerts"
GOLD_RISK_CHECKPOINT       = f"{CHECKPOINT_BASE}/gold_risk_profile"

# Database & Delta Table Names
DATABASE_NAME    = "fraud_db"
BRONZE_TABLE     = f"{DATABASE_NAME}.bronze_transactions"
SILVER_TABLE     = f"{DATABASE_NAME}.silver_transactions"
SILVER_LATE_TABLE = f"{DATABASE_NAME}.silver_late_arrivals"
GOLD_ALERTS_TABLE = f"{DATABASE_NAME}.gold_fraud_alerts"
GOLD_RISK_TABLE  = f"{DATABASE_NAME}.gold_customer_risk_profile"
GOLD_ALERTS_VIEW = f"{DATABASE_NAME}.view_dashboard_fraud_alerts"
GOLD_RISK_VIEW  = f"{DATABASE_NAME}.view_dashboard_customer_risk_profile"

# Streaming Configuration
WATERMARK_DURATION = "2 hours"

# Fraud Detection Rule Weights
WEIGHT_LOCATION_HOP  = 35
WEIGHT_HIGH_AMOUNT   = 30
WEIGHT_UNUSUAL_MERCH = 20
WEIGHT_LATE_NIGHT    = 10
WEIGHT_MULTI_TXN     = 15
MAX_FRAUD_SCORE      = 100

# Risk Band Thresholds
RISK_LOW_MAX      = 30
RISK_MEDIUM_MAX   = 60
RISK_HIGH_MAX     = 80

# Fraud Alert Threshold
FRAUD_ALERT_THRESHOLD = 70

# Rule Detection Window Durations
LOCATION_HOP_WINDOW_MINUTES = 30
MULTI_TXN_WINDOW_MINUTES    = 15
MULTI_TXN_COUNT_THRESHOLD   = 5

# Late Night Detection Window (24-hour clock, IST)
LATE_NIGHT_START_HOUR = 1
LATE_NIGHT_END_HOUR   = 4

# Data Preparation
NUM_BATCHES = 10

# Timestamp Format
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
TIMEZONE         = "Asia/Kolkata"

# Set Spark session timezone to match transaction timezone
spark.conf.set("spark.sql.session.timeZone", TIMEZONE)

# Optimize shuffle partitions for 4-core Single Node Cluster (Standard_D4ds_v4)
spark.conf.set("spark.sql.shuffle.partitions", "4")

# Customer Risk Profile Sliding Window (Hours)
CUSTOMER_RISK_WINDOW_HOURS = 24

# COMMAND ----------

# Conditional Caching Helpers
def safe_cache(df):
    try:
        return df.cache()
    except Exception:
        return df

def safe_unpersist(df):
    try:
        df.unpersist()
    except Exception:
        pass

# COMMAND ----------

print("Pipeline config loaded.")