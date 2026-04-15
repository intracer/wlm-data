# run_monuments.py
import sys
sys.path.insert(0, "src")

from pyspark.sql import SparkSession

from wlm.pipeline import MonumentPaths, run_monuments_pipeline

spark = (SparkSession.builder
         .master("local[*]")
         .appName("wlm-monuments")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

paths = MonumentPaths(
    monuments_csv="data/raw/monuments.csv",
    humdata_csv="data/raw/humdata.csv",
    katotth_csv="data/katotth/katotth_koatuu.csv",
)

run_monuments_pipeline(spark, paths, fmt="parquet", fetch=True)
print("Written to data/processed/spark/monuments/")

spark.stop()
