# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Orchestration
# MAGIC Orchestrates the execution of the Bronze, Silver, and Gold notebooks in sequence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configurations

# COMMAND ----------

# MAGIC %run ../configs/pipeline_config

# COMMAND ----------

import time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline Layers

# COMMAND ----------

execution_stats = {}

def run_pipeline_step(notebook_path, step_name):
    print(f"Starting step: {step_name} ({notebook_path})...")
    start_time = time.time()
    
    try:
        result = dbutils.notebook.run(notebook_path, 600)
        end_time = time.time()
        elapsed = end_time - start_time
        
        execution_stats[step_name] = {
            "status": "Success",
            "duration_sec": round(elapsed, 2),
            "result": result
        }
        print(f"Finished step: {step_name} in {elapsed:.2f} seconds.\n")
        
    except Exception as e:
        end_time = time.time()
        elapsed = end_time - start_time
        execution_stats[step_name] = {
            "status": "Failed",
            "duration_sec": round(elapsed, 2),
            "error": str(e)
        }
        print(f"Failed step: {step_name} in {elapsed:.2f} seconds.\nError: {e}\n")
        raise e

# COMMAND ----------

run_pipeline_step("01_bronze_ingestion", "Bronze Ingestion")
run_pipeline_step("02_silver_processing", "Silver Processing")
run_pipeline_step("03_gold_fraud_detection", "Gold Fraud Detection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Summary

# COMMAND ----------

total_duration = 0

for step, stats in execution_stats.items():
    status = stats["status"]
    duration = stats["duration_sec"]
    total_duration += duration
    print(f"{step}: {status} ({duration} sec)")

print(f"Total Pipeline Duration: {total_duration:.2f} seconds")