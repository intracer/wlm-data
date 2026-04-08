# Databricks notebook source
# This notebook runs the monuments batch pipeline.
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - CSV files uploaded to DBFS at the paths below

import sys, os
sys.path.insert(0, os.path.join(os.getcwd(), "..", "src"))

# COMMAND ----------

from wlm.common import AdmLevel, Lang
from wlm.monuments import MonumentRepo

# COMMAND ----------
# Config — edit DBFS paths as needed

MONUMENTS_CSV  = "dbfs:/Volumes/workspace/default/wlm_data/monuments/wlm-ua-monuments.csv"
HUMDATA_CSV    = "dbfs:/Volumes/workspace/default/wlm_data/humdata/ukraine-populated-places.csv"
KATOTTH_CSV    = "dbfs:/Volumes/workspace/default/wlm_data/katotth/katotth_koatuu.csv"
OUTPUT_DIR     = "dbfs:/Volumes/workspace/default/wlm_data/output/monuments-with-cities"

# COMMAND ----------
# spark is pre-created by Databricks — no SparkSession.builder needed

repo = MonumentRepo(
    spark,
    Lang.EN,
    path=MONUMENTS_CSV,
    humdata_path=HUMDATA_CSV,
    katotth_path=KATOTTH_CSV,
)

# COMMAND ----------
# Write monuments joined with geographic data to parquet

joined = repo.joined_with_katotth()
joined.write.mode("overwrite").parquet(OUTPUT_DIR)
print(f"Written to {OUTPUT_DIR}")

# COMMAND ----------
# Show pictured monument percentage by region (ADM1)

display(repo.percentage_of_pictured_monuments_by_adm(AdmLevel.ADM1, df=spark.read.parquet(OUTPUT_DIR)))
