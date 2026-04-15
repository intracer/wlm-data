# Databricks notebook source
# This notebook runs the images pipeline (bronze → silver → gold).
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - Image CSV files in INPUT_DIR below

import sys
sys.path.insert(0, "src")

# COMMAND ----------

from wlm.pipeline import ImagePaths, run_images_pipeline

# COMMAND ----------
# Config — edit DBFS paths as needed

BASE = "dbfs:/Volumes/workspace/default/wlm_data"

paths = ImagePaths(
    images_dir=f"{BASE}/images",
    humdata_csv=f"{BASE}/raw/humdata.csv",
    bronze_dir=f"{BASE}/bronze/images",
    silver_dir=f"{BASE}/silver/images",
    gold_cumulative_dir=f"{BASE}/gold/images_cumulative",
    gold_windowed_dir=f"{BASE}/gold/images_windowed",
)

# COMMAND ----------
# spark is pre-created by Databricks

run_images_pipeline(
    spark,
    paths,
    fmt="delta",
    window_duration="365 days",
    watermark_duration="30 days",
)
print(f"Pipeline complete. Gold layer written to {BASE}/gold/")
