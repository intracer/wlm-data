# Databricks notebook source
# This notebook runs the recent changes pipeline.
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - CSV files uploaded to DBFS at the paths below
#   - Run each time you want to ingest new recent changes
#     (each run continues from where the last one left off via checkpoints/)

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), "..", "src"))

# COMMAND ----------

from wlm.recent_changes import LookupSet, RecentChangesClient, RecentChangesWriter

# COMMAND ----------
# Config — edit paths as needed.
# MONUMENTS_CSV and IMAGES_GLOB use /dbfs/ FUSE mount paths (required for Python open()).
# OUTPUT_PATH uses dbfs:/ URI (required for Spark Delta write).

MONUMENTS_CSV   = "/dbfs/Volumes/workspace/default/wlm_data/monuments/wlm-ua-monuments.csv"
IMAGES_GLOB     = "/dbfs/Volumes/workspace/default/wlm_data/images"
CHECKPOINT_PATH = "checkpoints/recent_changes.json"
OUTPUT_PATH     = "dbfs:/tables/recent_changes"

# Optional: override resume timestamp (ISO 8601). Leave as None to use checkpoint.
SINCE = None  # e.g. "2024-09-01T00:00:00Z"

# COMMAND ----------
# Build the images file list from DBFS

import glob as _glob
images_paths = _glob.glob(f"{IMAGES_GLOB}/wlm-UA-*-images.csv")

# COMMAND ----------
# Build lookup set from monuments and images CSVs

lookup = LookupSet(MONUMENTS_CSV, images_paths)
print(f"Lookup set: {len(lookup._map)} entries")

# COMMAND ----------
# Fetch recent changes from MediaWiki API

client = RecentChangesClient(CHECKPOINT_PATH, since=SINCE)
records, last_token = client.fetch_with_token()
print(f"Fetched {len(records)} raw records")

# COMMAND ----------
# Filter, enrich, and append matched records to Delta table

writer = RecentChangesWriter(spark, OUTPUT_PATH, CHECKPOINT_PATH)
writer.write(records, lookup, last_token)
print(f"Done. Checkpoint updated.")

# COMMAND ----------
# Preview the Delta table

spark.read.format("delta").load(OUTPUT_PATH).orderBy("timestamp", ascending=False).show(20, truncate=False)
