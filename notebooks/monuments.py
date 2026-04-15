# Databricks notebook source
# This notebook runs the monuments batch pipeline (bronze → silver → gold).
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - Raw CSV files in the DBFS paths below (run fetch/convert scripts first)

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), "..", "src"))

# COMMAND ----------

from wlm.pipeline import MonumentPaths, run_monuments_pipeline

# COMMAND ----------
# Config — edit DBFS paths as needed

BASE = "dbfs:/Volumes/workspace/default/wlm_data"

paths = MonumentPaths(
    monuments_csv=f"{BASE}/raw/monuments.csv",
    humdata_csv=f"{BASE}/raw/humdata.csv",
    katotth_csv=f"{BASE}/katotth/katotth_koatuu.csv",
    bronze_dir=f"{BASE}/bronze/monuments",
    silver_cleaned_dir=f"{BASE}/silver/monuments_cleaned",
    silver_with_cities_dir=f"{BASE}/silver/monuments_with_cities",
    gold_full_dir=f"{BASE}/gold/monuments_full",
    gold_by_adm_dir=f"{BASE}/gold/monuments_by_adm",
    gold_pictured_by_adm_dir=f"{BASE}/gold/pictured_by_adm",
)

# COMMAND ----------
# spark is pre-created by Databricks — no SparkSession.builder needed

run_monuments_pipeline(spark, paths, fmt="delta")
print(f"Pipeline complete. Gold layer written to {BASE}/gold/")
