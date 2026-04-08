# Databricks notebook source
# This notebook runs the recent changes pipeline.
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - CSV files uploaded to DBFS at the paths below
#   - Run each time you want to ingest one day of recent changes
#     (each run advances the window by one day via checkpoints/)

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

RANGE_START = "2025-10-01T00:00:00Z"  # first run starts here
RANGE_END   = "2025-11-01T00:00:00Z"  # exclusive — stop after Oct 31

# COMMAND ----------
# Build the images file list

import glob as _glob
images_paths = _glob.glob(f"{IMAGES_GLOB}/wlm-UA-*-images.csv")

# COMMAND ----------
# Build lookup set from monuments and images CSVs

lookup = LookupSet(MONUMENTS_CSV, images_paths)
print(f"Lookup set: {len(lookup._map)} entries")

# COMMAND ----------
# Check if all days have been processed

client = RecentChangesClient(CHECKPOINT_PATH, since=RANGE_START)
effective_since = client._effective_since()

if effective_since and effective_since >= RANGE_END:
    print(f"All days processed (checkpoint={effective_since!r}). Nothing to do.")
else:
    # COMMAND ----------
    # Fetch one day of recent changes from MediaWiki API

    records, last_token, rcend = client.fetch_with_token()

    # Cap rcend at RANGE_END so we never overshoot
    if rcend and rcend > RANGE_END:
        rcend = RANGE_END

    print(f"Fetched {len(records)} raw records for window ending {rcend!r}")

    # COMMAND ----------
    # Filter, enrich, and append matched records to Delta table

    writer = RecentChangesWriter(spark, OUTPUT_PATH, CHECKPOINT_PATH)
    writer.write(records, lookup, last_token, next_start=rcend)
    print(f"Done. Checkpoint advanced to {rcend!r}.")

    # COMMAND ----------
    # Preview the Delta table

    spark.read.format("delta").load(OUTPUT_PATH).orderBy("timestamp", ascending=False).show(20, truncate=False)
