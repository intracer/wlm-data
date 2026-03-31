# Design: Embed Spark Pipeline in dbt + Dagster with Medallion Architecture

**Date:** 2026-03-31
**Status:** Approved

## Overview

Introduce a dbt+DuckDB pipeline alongside the existing Spark/Scala pipeline, both orchestrated by Dagster. The dbt pipeline mirrors the Spark logic using medallion architecture (bronze/silver/gold). Spark remains primary; DuckDB becomes primary after validation via a comparison job.

## Repository Structure

```
wlm-data/
├── src/                        # existing Spark/Scala (unchanged)
├── dbt/                        # new dbt project (dbt-duckdb adapter)
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── profiles.yml            # DuckDB target
│   └── dbt_project.yml
├── scripts/
│   ├── fetch_monuments.py      # Wikipedia fetch → data/raw/monuments.csv
│   └── convert_humdata.py      # Excel download → data/raw/humdata.csv
├── orchestration/
│   └── definitions.py          # Dagster assets, jobs, asset checks
├── data/
│   ├── raw/                    # fetched + static inputs (monuments, humdata, katotth)
│   └── processed/
│       ├── spark/              # Spark pipeline outputs
│       ├── dbt/                # dbt pipeline outputs
│       └── comparison_report.md
└── Makefile
```

## Data Sources

| Source | Format | How obtained |
|--------|--------|--------------|
| Monuments | CSV | `fetch_monuments.py` fetches from Wikipedia |
| HumData populated places | CSV | `convert_humdata.py` converts Excel from humdata.org |
| KATOTTH/KOATUU | CSV | Static file in repo |

All sources land in `data/raw/` before any pipeline runs.

## Medallion Architecture (dbt models)

### Bronze — raw ingestion, no transformation

| Model | Source file |
|-------|-------------|
| `bronze_monuments_raw` | `data/raw/monuments.csv` |
| `bronze_humdata_raw` | `data/raw/humdata_populated_places.csv` |
| `bronze_katotth_raw` | `data/katotth/katotth_koatuu.csv` |

DuckDB reads files directly via `read_csv`. No copies, just schema declarations.

### Silver — cleaned and joined

| Model | Logic |
|-------|-------|
| `silver_monuments_cleaned` | Derives `adm1`/`adm2` KOATUU codes from monument ID; cleans municipality names (replicates `Monument.cleanMunicipality` in SQL) |
| `silver_katotth_unique` | Deduplicates KATOTTH by adm2 prefix + name (mirrors `uniqueNameByAdm2`) |
| `silver_monuments_with_cities` | Joins monuments → KATOTTH → HumData admin names; resolves `adm2`/`adm3`/`adm4` codes |

### Gold — analytical outputs

| Model | Logic |
|-------|-------|
| `gold_monuments_full` | Fully denormalized monument table with all admin names |
| `gold_monuments_by_adm` | Count of monuments per admin level (1/2/3/4) |
| `gold_pictured_by_adm` | Photo coverage percentage per admin level |

## Dagster Orchestration

### Assets

| Asset | Type | Depends on | Output |
|-------|------|------------|--------|
| `monuments_raw` | Python | — | `data/raw/monuments.csv` |
| `humdata_raw` | Python | — | `data/raw/humdata.csv` |
| `spark_output` | Python | `monuments_raw`, `humdata_raw` | `data/processed/spark/` |
| `dbt_pipeline` | dbt asset | `monuments_raw`, `humdata_raw` | `data/processed/dbt/` (DuckDB) |
| `compare_outputs` | Python | `spark_output`, `dbt_pipeline` | `data/processed/comparison_report.md` |

### Jobs

| Job | Assets | Purpose |
|-----|--------|---------|
| `spark_job` | `monuments_raw → humdata_raw → spark_output` | Primary production pipeline |
| `dbt_job` | `monuments_raw → humdata_raw → dbt_pipeline` | Alternative DuckDB pipeline |
| `compare_job` | all assets including `compare_outputs` | Runs both pipelines to separate dirs and diffs results |

`spark_job` is the default scheduled job. `dbt_job` is run manually during validation. `compare_job` is used during the migration phase.

### `compare_outputs` asset

- Spark outputs Parquet to `data/processed/spark/`; dbt outputs are stored in a DuckDB file at `data/processed/dbt/wlm.duckdb`
- `compare_outputs` uses the DuckDB Python API to read both: Parquet via `read_parquet()` and dbt results via attaching the DuckDB file
- Compares: row counts, key columns (`id`, `adm1`, `adm2`, `adm3`, `adm4`, `municipality`), aggregation totals
- `gold_pictured_by_adm` totals must match within 0.1%
- Writes diff report to `data/processed/comparison_report.md`
- Fails (raises exception) if discrepancies exceed threshold

## Testing & Validation

### dbt tests

- `not_null` + `unique` on monument IDs across silver and gold models
- Row count assertions: gold models must have > minimum expected row count
- Custom test: `silver_monuments_with_cities` has no unresolved admin codes (no null `adm2`/`adm3`)

### Dagster asset checks

- `monuments_raw`: file exists and row count > 0
- `spark_output`: output Parquet is readable and has expected schema
- `dbt_pipeline`: all dbt tests pass

## Migration Path

| Phase | State |
|-------|-------|
| **Now** | `spark_job` is scheduled default. `dbt_job` available but run manually. |
| **Validation** | Run `compare_job` periodically. Fix dbt models until diffs are zero or within threshold. |
| **Cutover** | Schedule `dbt_job` instead of `spark_job`. Spark/Scala stays in repo but is inactive. |
| **Cleanup (optional)** | Retire Spark/Scala after dbt pipeline is stable for a defined period. |

Cutover requires no code change — only changing which Dagster job is scheduled.

## Out of Scope

- Cloud deployment / remote Dagster instance
- Automatic scheduling configuration (schedule cadence TBD by operator)
- Real-time or streaming ingestion
