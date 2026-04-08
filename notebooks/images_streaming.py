# Databricks notebook source
# This notebook runs the images structured streaming pipeline.
# Prerequisites:
#   - Repo cloned via Databricks Repos (Git integration)
#   - Image CSV files dropped into dbfs:/FileStore/wlm-data/images/
#   - Run ONCE per session; re-run to pick up newly added CSV files
#     (trigger=availableNow processes all available files then terminates)

import sys
sys.path.insert(0, "src")

# COMMAND ----------

from wlm.common import AdmLevel, Lang, PopulatedPlaceRepo
from wlm.images import WlmSchema, transform, windowed_agg, cumulative_agg

# COMMAND ----------
# Config — edit as needed

INPUT_DIR      = "dbfs:/FileStore/wlm-data/images"
HUMDATA_CSV    = "dbfs:/FileStore/wlm-data/humdata/ukraine-populated-places.csv"
OUTPUT_DIR     = "dbfs:/FileStore/wlm-data/output"
CHECKPOINT_DIR = "dbfs:/FileStore/wlm-data/checkpoints"
WINDOW_DUR     = "1 hour"
WATERMARK_DUR  = "10 minutes"

# Toggle: set to True to run the cumulative query instead of windowed
RUN_CUMULATIVE = False

# COMMAND ----------

adm_names_df = PopulatedPlaceRepo(spark, Lang.EN, path=HUMDATA_CSV).adm_names(AdmLevel.ADM1)

raw_stream = (spark.readStream
              .schema(WlmSchema.csv_schema)
              .option("header", "true")
              .csv(INPUT_DIR))

transformed = transform(raw_stream)

# COMMAND ----------

if RUN_CUMULATIVE:
    query = (cumulative_agg(transformed, adm_names_df)
             .writeStream
             .outputMode("complete")
             .option("checkpointLocation", f"{CHECKPOINT_DIR}/cumulative")
             .trigger(availableNow=True)
             .foreachBatch(lambda df, _: (
                 df.show(truncate=False),
                 df.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/cumulative")
             ))
             .start())
else:
    query = (windowed_agg(transformed, adm_names_df, WINDOW_DUR, WATERMARK_DUR)
             .writeStream
             .outputMode("append")
             .option("checkpointLocation", f"{CHECKPOINT_DIR}/windowed")
             .trigger(availableNow=True)
             .foreachBatch(lambda df, _: (
                 df.sort("monuments_pictured", ascending=False).show(truncate=False),
                 df.write.mode("append").parquet(f"{OUTPUT_DIR}/windowed")
             ))
             .start())

# COMMAND ----------
# Wait for the triggered run to finish (availableNow=True terminates after processing all files)

query.awaitTermination()
print("Streaming query complete.")
