import subprocess
from pathlib import Path

import duckdb
import pandas as pd
from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    define_asset_job,
    AssetSelection,
)

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
    row_count = sum(1 for _ in open(output)) - 1
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
    row_count = sum(1 for _ in open(output)) - 1
    context.add_output_metadata({"row_count": row_count, "path": str(output)})


@asset(deps=[monuments_raw, humdata_raw])
def spark_output(context: AssetExecutionContext):
    """Run existing Spark/Scala pipeline via sbt."""
    SPARK_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["sbt", f"run {SPARK_OUTPUT_DIR}"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
        timeout=600,
    )
    context.log.info(result.stdout[-3000:])
    parquet_files = list(SPARK_OUTPUT_DIR.glob("*.parquet"))
    context.add_output_metadata({"parquet_files": len(parquet_files), "path": str(SPARK_OUTPUT_DIR)})


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

    conn = duckdb.connect()
    spark_df = conn.execute(
        f"SELECT id, adm1, adm2, adm3, adm4, municipality "
        f"FROM read_parquet('{SPARK_OUTPUT_DIR}/*.parquet') ORDER BY id"
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

    threshold_ok = True
    for col in ["adm1", "adm2", "adm3", "adm4", "municipality"]:
        mismatches = int((merged[f"{col}_spark"] != merged[f"{col}_dbt"]).sum())
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
