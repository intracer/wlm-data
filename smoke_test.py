import sys
import os
import glob

sys.path.insert(0, "src")

_IVY_CACHE = os.path.expanduser("~/.ivy2/cache")
_DELTA_JAR = os.path.join(_IVY_CACHE, "io.delta", "delta-spark_2.12", "jars", "delta-spark_2.12-3.2.0.jar")
_STORAGE_JAR = os.path.join(_IVY_CACHE, "io.delta", "delta-storage", "jars", "delta-storage-3.2.0.jar")

from pyspark.sql import SparkSession
from wlm.recent_changes import LookupSet, RecentChangesClient, RecentChangesWriter

RANGE_START = "2025-10-01T00:00:00Z"
RANGE_END   = "2025-11-01T00:00:00Z"  # exclusive — stop processing after Oct 31
CHECKPOINT  = "checkpoints/recent_changes.json"
OUTPUT      = "output/recent_changes"

spark = (SparkSession.builder
         .master("local")
         .appName("rc-smoke")
         .config("spark.ui.enabled", "false")
         .config("spark.jars", f"{_DELTA_JAR},{_STORAGE_JAR}")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

images_paths = glob.glob("data/images/wlm-UA-*-images.csv")
lookup = LookupSet("data/wiki/monuments/wlm-ua-monuments.csv", images_paths)
print(f"Lookup entries: {len(lookup._map)}")

# Determine the start of this run's window.
# On first run, use RANGE_START. On subsequent runs, the checkpoint holds rcend
# from the previous run, which is the start of this run's window.
client = RecentChangesClient(CHECKPOINT, since=RANGE_START)
effective_since = client._effective_since()

if effective_since and effective_since >= RANGE_END:
    print(f"All days processed (checkpoint={effective_since!r} >= range_end={RANGE_END!r}). Nothing to do.")
    spark.stop()
    sys.exit(0)

print(f"Fetching window: rcstart={effective_since!r}, rcend=auto (+1 day, capped at {RANGE_END})")

# Auto-compute rcend = min(since + 1 day, RANGE_END)
records, token, rcend = client.fetch_with_token()

# Cap rcend at RANGE_END so we never overshoot
if rcend and rcend > RANGE_END:
    rcend = RANGE_END

print(f"Raw records: {len(records)}, rcend={rcend!r}, last_token={token!r}")

writer = RecentChangesWriter(spark, OUTPUT, CHECKPOINT)
writer.write(records, lookup, token, next_start=rcend)
print("Write complete. Checkpoint updated.")

result = spark.read.format("delta").load(OUTPUT)
print(f"Delta table total rows: {result.count()}")
result.orderBy("timestamp", ascending=False).show(10, truncate=False)

spark.stop()
