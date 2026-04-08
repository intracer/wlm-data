# Recent Changes Pipeline — Design Spec

**Date:** 2026-04-08
**Branch:** eventstreams

## Goal

Add a resumable batch pipeline that fetches Wikipedia/Commons recent changes for articles related to Ukrainian WLM monuments and publishes results to a Delta table on Databricks Free Edition (and locally for testing).

---

## Background

The existing project is a PySpark pipeline for Wiki Loves Monuments Ukraine. It has two pipelines:
- **Monuments batch** — joins monuments CSV with geographic data
- **Images streaming** — processes WLM image upload CSVs

This spec adds a third pipeline: **Recent Changes** — a polling-based pipeline that fetches incremental changes from the MediaWiki API and appends them to a Delta table.

---

## Sources

Four MediaWiki API endpoints, one per wiki/namespace combination:

| Wiki | Namespace | Matches |
|---|---|---|
| `uk.wikipedia.org` | NS 0 (articles) | Monument article titles, place article titles |
| `en.wikipedia.org` | NS 0 (articles) | Monument article titles (English), place names |
| `commons.wikimedia.org` | NS 14 (categories) | `gallery` and `commonscat` values from monuments CSV |
| `commons.wikimedia.org` | NS 6 (files) | File titles from the images dataset |

---

## Architecture

```
src/wlm/
  recent_changes.py         # LookupSet, RecentChangesClient, RecentChangesWriter
notebooks/
  recent_changes.py         # Databricks entry point
checkpoints/
  recent_changes.json       # {"rccontinue": "...", "timestamp": "..."}
tests/
  test_recent_changes.py
```

### Components

**`LookupSet`**
- Loads `wlm-ua-monuments.csv` and the images CSVs
- Extracts all watchable `(wiki, title)` pairs and assigns each a `source_type`:
  - `monument_article` — monument article names on uk/en wikipedia
  - `place_article` — place names associated with monuments on uk/en wikipedia
  - `commons_category` — `gallery` and `commonscat` column values from monuments CSV
  - `commons_file` — file titles from the images dataset
- Returns a `dict[(wiki, title), source_type]`
- Missing columns in source CSVs are tolerated with a warning (does not crash)

**`RecentChangesClient`**
- Calls `https://{wiki}/w/api.php?action=query&list=recentchanges&...` for each of the 4 wiki/namespace combinations
- Reads resume position from `checkpoints/recent_changes.json` (or accepts a `since` timestamp override via parameter)
- Paginates via `rccontinue` until exhausted
- Returns a flat list of raw API result dicts with an added `wiki` field
- Does NOT update the checkpoint — that is the writer's responsibility after successful write

**`RecentChangesWriter`**
- Accepts raw API records + `LookupSet`
- Filters to matched `(wiki, title)` pairs
- Enriches each record with `source_type` and `wiki` columns
- Creates a PySpark DataFrame with the full API schema + `wiki` + `source_type`
- Appends to Delta table:
  - Local: `./output/recent_changes/`
  - Databricks: `dbfs:/tables/recent_changes/`
- Updates checkpoint only after successful write

---

## Data Flow

```
checkpoints/recent_changes.json
        │ rccontinue / since timestamp
        ▼
RecentChangesClient
  ├─ GET uk.wikipedia.org  (NS 0)
  ├─ GET en.wikipedia.org  (NS 0)
  ├─ GET commons.wikimedia.org (NS 14)
  └─ GET commons.wikimedia.org (NS 6)
        │ raw rc records
        ▼
LookupSet.match(wiki, title)
  → filters to matched titles
  → adds source_type
        │ enriched records
        ▼
RecentChangesWriter
  → spark.createDataFrame(records, schema)
  → df.write.format("delta").mode("append")
        ├─ local:  ./output/recent_changes/
        └─ DBFS:   dbfs:/tables/recent_changes/
        ▼
checkpoints/recent_changes.json  ← updated rccontinue token
```

---

## Output Schema

All fields from the MediaWiki `recentchanges` API response, plus two added columns:

| Column | Type | Source |
|---|---|---|
| `rcid` | long | API |
| `type` | string | API — `edit`, `new`, `log` |
| `ns` | integer | API — namespace number |
| `title` | string | API |
| `timestamp` | string | API — ISO 8601 |
| `user` | string | API |
| `userid` | long | API |
| `comment` | string | API |
| `parsedcomment` | string | API |
| `old_len` | integer | API (`sizes.old`) |
| `new_len` | integer | API (`sizes.new`) |
| `revid` | long | API |
| `old_revid` | long | API |
| `pageid` | long | API |
| `redirect` | boolean | API |
| `tags` | array\<string\> | API |
| `sha1` | string | API |
| `logtype` | string | API |
| `logaction` | string | API |
| `logparams` | string | API (JSON-encoded) |
| `wiki` | string | Added — e.g. `uk.wikipedia.org` |
| `source_type` | string | Added — `monument_article`, `place_article`, `commons_category`, `commons_file` |

---

## Checkpoint & Resume

- Checkpoint file: `checkpoints/recent_changes.json`
- Format: `{"rccontinue": "<token>", "timestamp": "<ISO8601>"}`
- On first run (no checkpoint): fetches from the last 24 hours (API default)
- `--since` parameter: overrides checkpoint read, uses value as `rcstart`; checkpoint is still updated after the run
- Checkpoint is written only after `df.write` completes — no partial-run corruption

---

## Error Handling

- HTTP errors raise immediately; checkpoint is not updated; next run retries from the same position
- API `error` field in JSON response raises `RuntimeError` with the API message
- Empty result set (no matches) is valid — nothing is written, checkpoint is still updated
- Missing columns in source CSVs are tolerated with a warning

---

## Testing

`tests/test_recent_changes.py` covers three units:

- **`LookupSet`** — small in-memory CSV fixture; verifies correct `source_type` assignment and deduplication of `(wiki, title)` pairs
- **`RecentChangesClient`** — mocked `requests.get`; verifies pagination via `rccontinue`, `since` override, checkpoint read/write
- **`RecentChangesWriter`** — small PySpark session; verifies schema correctness, `mode("append")` accumulates rows across two runs, unmatched titles are excluded

---

## Files Changed

**Create:**
- `src/wlm/recent_changes.py`
- `notebooks/recent_changes.py`
- `tests/test_recent_changes.py`

**Modify:**
- `requirements.txt` — add `requests` and `delta-spark`
- `checkpoints/recent_changes.json` — created on first run
