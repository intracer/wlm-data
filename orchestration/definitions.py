import subprocess
from pathlib import Path

import duckdb
from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    define_asset_job,
    AssetSelection,
)
from pyspark.sql import SparkSession

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wlm.pipeline import ImagePaths, MonumentPaths, run_images_pipeline, run_monuments_pipeline

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
SPARK_OUTPUT_DIR = REPO_ROOT / "data" / "processed" / "spark"
DBT_PROJECT_DIR = REPO_ROOT / "dbt"
DBT_OUTPUT_DIR = REPO_ROOT / "data" / "processed" / "dbt"
COMPARISON_REPORT = REPO_ROOT / "data" / "processed" / "comparison_report.md"


@asset
def monuments_raw(context: AssetExecutionContext):
    """Fetch WLM Ukraine monuments from Wikipedia Heritage API."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output = RAW_DIR / "monuments.csv"
    result = subprocess.run(
        ["python", "scripts/fetch_monuments.py", "--output", str(output)],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    context.log.info(result.stderr)
    with open(output, encoding="utf-8") as f:
        row_count = sum(1 for _ in f) - 1
    context.add_output_metadata({"row_count": row_count, "path": str(output)})


@asset
def humdata_raw(context: AssetExecutionContext):
    """Download and convert humdata populated places to CSV."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output = RAW_DIR / "humdata.csv"
    result = subprocess.run(
        ["python", "scripts/convert_humdata.py", "--output", str(output)],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    context.log.info(result.stderr)
    with open(output, encoding="utf-8") as f:
        row_count = sum(1 for _ in f) - 1
    context.add_output_metadata({"row_count": row_count, "path": str(output)})


@asset(deps=[monuments_raw, humdata_raw])
def spark_output(context: AssetExecutionContext):
    """Run PySpark pipeline — writes bronze/silver/gold parquet layers."""
    SPARK_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    spark = (SparkSession.builder
             .master("local[*]")
             .appName("wlm-dagster")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    try:
        monument_paths = MonumentPaths(
            monuments_csv=str(RAW_DIR / "monuments.csv"),
            humdata_csv=str(RAW_DIR / "humdata.csv"),
            katotth_csv=str(REPO_ROOT / "data" / "katotth" / "katotth_koatuu.csv"),
            bronze_dir=str(SPARK_OUTPUT_DIR / "monuments" / "bronze"),
            silver_cleaned_dir=str(SPARK_OUTPUT_DIR / "monuments" / "silver" / "cleaned"),
            silver_with_cities_dir=str(SPARK_OUTPUT_DIR / "monuments" / "silver" / "with_cities"),
            gold_full_dir=str(SPARK_OUTPUT_DIR / "monuments" / "gold" / "full"),
            gold_by_adm_dir=str(SPARK_OUTPUT_DIR / "monuments" / "gold" / "by_adm"),
            gold_pictured_by_adm_dir=str(SPARK_OUTPUT_DIR / "monuments" / "gold" / "pictured_by_adm"),
        )
        run_monuments_pipeline(spark, monument_paths, fmt="parquet")
        context.log.info("Monuments pipeline complete")

        image_paths = ImagePaths(
            images_dir=str(REPO_ROOT / "data" / "images"),
            humdata_csv=str(RAW_DIR / "humdata.csv"),
            bronze_dir=str(SPARK_OUTPUT_DIR / "images" / "bronze"),
            silver_dir=str(SPARK_OUTPUT_DIR / "images" / "silver"),
            gold_cumulative_dir=str(SPARK_OUTPUT_DIR / "images" / "gold" / "cumulative"),
            gold_windowed_dir=str(SPARK_OUTPUT_DIR / "images" / "gold" / "windowed"),
        )
        run_images_pipeline(spark, image_paths, fmt="parquet")
        context.log.info("Images pipeline complete")
    finally:
        spark.stop()

    gold_dir = SPARK_OUTPUT_DIR / "monuments" / "gold" / "full"
    parquet_files = list(gold_dir.glob("*.parquet"))
    context.add_output_metadata({"gold_parquet_files": len(parquet_files), "gold_path": str(gold_dir)})


@asset(deps=[monuments_raw, humdata_raw])
def dbt_pipeline(context: AssetExecutionContext):
    """Run dbt models (bronze → silver → gold) against DuckDB."""
    DBT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for cmd in [["dbt", "run"], ["dbt", "test"]]:
        result = subprocess.run(
            cmd + ["--project-dir", str(DBT_PROJECT_DIR), "--profiles-dir", str(DBT_PROJECT_DIR)],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        context.log.info(result.stdout[-3000:])
    duckdb_path = DBT_OUTPUT_DIR / "wlm.duckdb"
    context.add_output_metadata({"duckdb_path": str(duckdb_path), "exists": duckdb_path.exists()})


@asset(deps=[spark_output, dbt_pipeline])
def compare_outputs(context: AssetExecutionContext):
    """Diff Spark Parquet output against dbt DuckDB gold layer."""
    duckdb_path = DBT_OUTPUT_DIR / "wlm.duckdb"

    gold_spark_dir = SPARK_OUTPUT_DIR / "monuments" / "gold" / "full"
    conn = duckdb.connect()
    spark_df = conn.execute(
        f"SELECT id, adm1, adm2, adm3, adm4, municipality "
        f"FROM read_parquet('{gold_spark_dir}/*.parquet') ORDER BY id"
    ).df()
    conn.close()

    dbt_conn = duckdb.connect(str(duckdb_path), read_only=True)
    dbt_df = dbt_conn.execute(
        "SELECT id, adm1, adm2, adm3, adm4, municipality "
        "FROM gold.gold_monuments_full ORDER BY id"
    ).df()
    dbt_conn.close()

    spark_count = len(spark_df)
    dbt_count = len(dbt_df)
    count_diff = abs(spark_count - dbt_count)
    count_diff_pct = count_diff / spark_count * 100 if spark_count > 0 else 0.0

    lines = [
        "# Pipeline Comparison Report",
        "",
        f"| | Spark | dbt/DuckDB |",
        f"|---|---|---|",
        f"| Row count | {spark_count} | {dbt_count} |",
        f"| Difference | {count_diff} ({count_diff_pct:.2f}%) | |",
        "",
        "## Column-level mismatches (matched on id)",
        "",
    ]

    merged = spark_df.merge(dbt_df, on="id", suffixes=("_spark", "_dbt"))
    matched = len(merged)
    lines.append(f"Matched rows (by id): {matched}")
    lines.append("")

    if matched == 0:
        raise ValueError("No rows matched between Spark and dbt outputs on 'id' — check schema alignment")

    threshold_ok = True
    for col in ["adm1", "adm2", "adm3", "adm4", "municipality"]:
        col_s = merged[f"{col}_spark"]
        col_d = merged[f"{col}_dbt"]
        mismatches = int(((col_s != col_d) & ~(col_s.isna() & col_d.isna())).sum())
        pct = mismatches / matched * 100 if matched > 0 else 0.0
        lines.append(f"- `{col}`: {mismatches} mismatches ({pct:.2f}%)")
        if pct > 0.1:
            threshold_ok = False

    COMPARISON_REPORT.parent.mkdir(parents=True, exist_ok=True)
    COMPARISON_REPORT.write_text("\n".join(lines), encoding="utf-8")
    context.log.info("\n".join(lines))
    context.add_output_metadata({"report_path": str(COMPARISON_REPORT)})

    if count_diff_pct > 1.0:
        raise ValueError(f"Row count divergence {count_diff_pct:.2f}% exceeds 1% threshold")
    if not threshold_ok:
        raise ValueError("Column mismatch rate exceeds 0.1% threshold — see comparison_report.md")


spark_job = define_asset_job(
    "spark_job",
    selection=AssetSelection.assets(monuments_raw, humdata_raw, spark_output),
    description="Primary production pipeline: fetch raw data, run Spark/Scala.",
)

dbt_job = define_asset_job(
    "dbt_job",
    selection=AssetSelection.assets(monuments_raw, humdata_raw, dbt_pipeline),
    description="Alternative DuckDB pipeline: fetch raw data, run dbt medallion models.",
)

compare_job = define_asset_job(
    "compare_job",
    selection=AssetSelection.assets(
        monuments_raw, humdata_raw, spark_output, dbt_pipeline, compare_outputs
    ),
    description="Migration validation: runs both pipelines and diffs gold layer outputs.",
)

defs = Definitions(
    assets=[monuments_raw, humdata_raw, spark_output, dbt_pipeline, compare_outputs],
    jobs=[spark_job, dbt_job, compare_job],
)
