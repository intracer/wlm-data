# run_monuments.py
import sys
sys.path.insert(0, "src")

from pyspark.sql import SparkSession
from wlm.common import AdmLevel, Lang
from wlm.monuments import MonumentRepo

spark = (SparkSession.builder
         .master("local[*]")
         .appName("wlm-monuments")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

repo = MonumentRepo(spark, Lang.EN)

# Write joined dataset to parquet
repo.joined_with_katotth().write.mode("overwrite").parquet("output/monuments-with-cities")
print("Written to output/monuments-with-cities/")

# Print statistics
print("\n=== Monuments with pictures by region (ADM1) ===")
repo.number_of_pictured_monuments_by_adm(AdmLevel.ADM1).show(30, truncate=False)

print("\n=== Percentage of pictured monuments by region (ADM1) ===")
repo.percentage_of_pictured_monuments_by_adm(AdmLevel.ADM1).show(30, truncate=False)

spark.stop()