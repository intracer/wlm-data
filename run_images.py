# run_images.py  (create this file at the repo root, or paste into a REPL)
import sys
sys.path.insert(0, "src")

from pyspark.sql import SparkSession
from wlm.common import AdmLevel, Lang, PopulatedPlaceRepo
from wlm.images import WlmSchema, transform, windowed_agg, cumulative_agg

INPUT_DIR      = "data/images"
OUTPUT_DIR     = "output"
CHECKPOINT_DIR = "checkpoints"
WINDOW_DUR     = "1 hour"
WATERMARK_DUR  = "10 minutes"
RUN_CUMULATIVE = False   # set to True for the cumulative query

spark = (SparkSession.builder
         .master("local[*]")
         .appName("wlm-images")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

adm_names_df = PopulatedPlaceRepo(spark, Lang.EN).adm_names(AdmLevel.ADM1)

raw_stream = (spark.readStream
              .schema(WlmSchema.csv_schema)
              .option("header", "true")
              .csv(INPUT_DIR))

transformed = transform(raw_stream)

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

query.awaitTermination()
print("Done.")
spark.stop()