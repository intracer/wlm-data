import sys
import os
import glob
from datetime import datetime, timezone

sys.path.insert(0, "src")

_IVY_CACHE = os.path.expanduser("~/.ivy2/cache")
_DELTA_JAR = os.path.join(_IVY_CACHE, "io.delta", "delta-spark_2.12", "jars", "delta-spark_2.12-3.2.0.jar")
_STORAGE_JAR = os.path.join(_IVY_CACHE, "io.delta", "delta-storage", "jars", "delta-storage-3.2.0.jar")

from pyspark.sql import SparkSession
from wlm.recent_changes import LookupSet, RecentChangesClient, RecentChangesWriter, _WIKI_NS_COMBOS

RANGE_START = "2026-03-10T00:00:00Z"
RANGE_END   = "2026-04-10T00:00:00Z"  # exclusive — 31 days from RANGE_START
CHECKPOINT  = "checkpoints/recent_changes.json"
OUTPUT      = "output/recent_changes"

def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

_range_start_dt = _parse(RANGE_START)
_range_end_dt   = _parse(RANGE_END)
_total_seconds  = (_range_end_dt - _range_start_dt).total_seconds()

def _progress(current_ts: str) -> str:
    elapsed = (_parse(current_ts) - _range_start_dt).total_seconds()
    pct = elapsed / _total_seconds * 100
    days_done = elapsed / 86400
    days_total = _total_seconds / 86400
    bar_len = 30
    filled = int(bar_len * elapsed / _total_seconds)
    bar = "█" * filled + "░" * (bar_len - filled)
    return f"[{bar}] {pct:.1f}%  ({days_done:.0f}/{days_total:.0f} days)"

def _intra_day_bar(window_start: str, window_end: str, current_ts: str) -> str:
    start = _parse(window_start)
    end   = _parse(window_end)
    cur   = _parse(current_ts)
    total = (end - start).total_seconds()
    elapsed = max(0.0, min((cur - start).total_seconds(), total))
    pct = elapsed / total * 100 if total else 0
    bar_len = 20
    filled = int(bar_len * elapsed / total) if total else 0
    bar = "█" * filled + "░" * (bar_len - filled)
    return f"[{bar}] {pct:.0f}%"

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
wiki_idx = client._effective_wiki_idx()

if effective_since and effective_since >= RANGE_END:
    print(f"All days processed. {_progress(RANGE_END)}")
    spark.stop()
    sys.exit(0)

# Show progress before fetch
since_for_progress = effective_since or RANGE_START
wiki, ns = _WIKI_NS_COMBOS[wiki_idx]
print(f"Progress before this run: {_progress(since_for_progress)}")
print(f"Window: {since_for_progress}  →  +1 hour (capped at {RANGE_END})")
print(f"Wiki [{wiki_idx+1}/{len(_WIKI_NS_COMBOS)}]: {wiki}  ns={ns}")

_window_start = since_for_progress

def _on_page(wiki: str, latest_ts, total: int) -> None:
    if latest_ts:
        bar = _intra_day_bar(_window_start, RANGE_END, latest_ts)
        print(f"\r  {wiki:30s}  {bar}  {total:>5} records", end="", flush=True)

records, token, rcend, wiki_idx = client.fetch_with_token(on_page=_on_page)
print()  # end the \r line

# Cap rcend at RANGE_END so we never overshoot
if rcend and rcend > RANGE_END:
    rcend = RANGE_END

matched_count = sum(
    1 for r in records
    if lookup.source_type(r.get("wiki", ""), r.get("title", "")) is not None
)
# print(f"Raw records: {records} ")
print(f"Raw records size: {len(records)}  matched size: {matched_count}")

# Advance to next wiki combo. If this was the last combo (wraps to 0),
# advance the timestamp to rcend so the next run fetches the next hour.
next_wiki_idx = (wiki_idx + 1) % len(_WIKI_NS_COMBOS)
next_start = rcend if next_wiki_idx == 0 else since_for_progress

writer = RecentChangesWriter(spark, OUTPUT, CHECKPOINT)
writer.write(records, lookup, token, next_start=next_start, wiki_idx=next_wiki_idx)
print(f"Progress after  this run: {_progress(next_start or RANGE_END)}")

if os.path.exists(OUTPUT):
    result = spark.read.format("delta").load(OUTPUT)
    print(f"Delta table total rows: {result.count()}")
    result.orderBy("timestamp", ascending=False).show(10, truncate=False)
else:
    print("No matched records yet — Delta table not created.")

spark.stop()
