# Wiki Loves Monuments — Ukraine Data Pipeline

PySpark pipeline for analysing [Wiki Loves Monuments](https://www.wikilovesmonuments.org/) Ukraine data.
Runs on a local machine (Python + PySpark) and on [Databricks Community Edition](https://community.cloud.databricks.com/).

Two pipelines:
- **Monuments batch** — joins the WLM Ukraine monuments list with Ukrainian geographic data (KATOTTH/KOATUU mapping) and computes pictured-monument statistics by region.
- **Images streaming** — processes WLM image upload CSVs using structured streaming and aggregates distinct monuments photographed per author and region, either cumulatively or in time windows.

---

## Contents

- [0. Local development environment](#0-local-development-environment)
- [1. Run pipelines locally](#1-run-pipelines-locally)
  - [1a. Monuments batch pipeline](#1a-monuments-batch-pipeline)
  - [1b. Images streaming pipeline](#1b-images-streaming-pipeline)
- [2. Run unit tests](#2-run-unit-tests)
- [3. Databricks setup and run](#3-databricks-setup-and-run)
  - [3a. Prerequisites](#3a-prerequisites)
  - [3b. Upload data to DBFS](#3b-upload-data-to-dbfs)
  - [3c. Import the repo into Databricks Repos](#3c-import-the-repo-into-databricks-repos)
  - [3d. Run the monuments batch notebook](#3d-run-the-monuments-batch-notebook)
  - [3e. Run the images streaming notebook](#3e-run-the-images-streaming-notebook)
  - [3f. Run the test suite on Databricks](#3f-run-the-test-suite-on-databricks)

---

## 0. Local development environment

**Requirements:** Python 3.9+, Java 11 or 17 (required by PySpark).

### Check Java

PySpark needs a JVM on `PATH`. Verify with:

```bash
java -version
```

Java 11 and 17 both work. If Java is missing, install it via your package manager
(e.g. `brew install openjdk@17` on macOS, `sudo apt install openjdk-17-jdk` on Debian/Ubuntu).

### Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

`requirements.txt` pins `pyspark==3.5.6` and `pytest==8.3.5`.

### Data files

The pipelines read CSV files from the `data/` directory (already present in the repo):

| Path | Contents |
|---|---|
| `data/wiki/monuments/wlm-ua-monuments.csv` | WLM Ukraine monuments list |
| `data/humdata/ukraine-populated-places.csv` | Ukraine populated places with PCODE hierarchy (ADM0–ADM4) |
| `data/katotth/katotth_koatuu.csv` | KATOTTH ↔ KOATUU administrative code mapping |
| `data/images/wlm-UA-YYYY-images.csv` | WLM image upload records per year (2012–2025) |

---

## 1. Run pipelines locally

The business logic lives in `src/wlm/`. Run the pipelines from the repo root with the
virtual environment activated.

### 1a. Monuments batch pipeline

The pipeline reads the monuments CSV, joins it with geographic data, and prints
pictured-monument statistics by region.

Run it:

```bash
python run_monuments.py
```

Other available methods on `MonumentRepo`:

| Method | Description |
|---|---|
| `dataframe()` | Raw monuments CSV as a DataFrame |
| `with_koatuu_from_id()` | Monuments with KOATUU codes derived from the monument `id` |
| `cleaned_municipality_dataset()` | Cleaned municipality names (removes abbreviations, wiki markup) |
| `monuments_with_unmapped_koatuu()` | Monuments whose KOATUU code could not be matched to the KATOTTH table |
| `joined_with_katotth()` | Monuments matched to their KATOTTH entry and ADM4 place name |
| `number_of_monuments_by_adm(adm_level)` | Count of all monuments per administrative level |
| `number_of_pictured_monuments_by_adm(adm_level)` | Count of pictured monuments per administrative level |
| `percentage_of_pictured_monuments_by_adm(adm_level)` | Pictured-monument percentage per administrative level |

`adm_level` is an `AdmLevel` enum value: `AdmLevel.ADM1` (region), `AdmLevel.ADM2` (district),
`AdmLevel.ADM3` (community), `AdmLevel.ADM4` (settlement).

### 1b. Images streaming pipeline

The streaming pipeline reads image CSVs from a watched directory, transforms them into
one row per monument-image pair, and aggregates distinct monuments photographed per
author and region.

Two aggregation modes:

- **Windowed** (default) — counts per time window (e.g. 1 hour). Output mode: `append`.
- **Cumulative** — running total of distinct monuments per author/region. Output mode: `complete`.

`trigger(availableNow=True)` is used in both modes: the pipeline processes all files
currently in the input directory and then terminates. Re-run to pick up new files.

Run it:

```bash
python run_images.py
```

Output parquet files are written to `output/windowed/` or `output/cumulative/`.
Streaming checkpoints are stored in `checkpoints/` — delete this directory if you
want to reprocess all files from scratch.

**Output schema (windowed):**

| Column | Type | Description |
|---|---|---|
| `window` | struct{start, end} | Time window |
| `author` | string | Wikimedia Commons username |
| `region_name` | string | ADM1 region name (e.g. "Kyiv Oblast") |
| `monuments_pictured` | long | Approx. distinct monuments photographed |

**Output schema (cumulative):** same but without `window`.

---

## 2. Run unit tests

The test suite uses `pytest` and a local `SparkSession`. All tests run entirely in-process
— no external services required.

```bash
source .venv/bin/activate
pytest tests/ -v
```

Expected output: **31 tests passed**.

Run a single test file:

```bash
pytest tests/test_monuments.py -v
pytest tests/test_images.py -v
pytest tests/test_common.py -v
```

Run a single test by name:

```bash
pytest tests/test_common.py::test_clean_municipality_removes_sel_abbreviation -v
```

**Test files:**

| File | What it covers |
|---|---|
| `tests/test_common.py` | `AdmLevel`/`Lang` enums, `clean_municipality_col`, `PopulatedPlaceRepo`, `KatotthKoatuuRepo` |
| `tests/test_monuments.py` | `MonumentRepo` — KOATUU derivation, municipality cleaning, statistics methods |
| `tests/test_images.py` | `WlmSchema`, `transform`, `cumulative_agg`, `windowed_agg` |

`tests/test_common.py` and `tests/test_monuments.py` include integration tests that read
the real CSV files from `data/`. `tests/test_images.py` uses only in-memory DataFrames
(or a temporary directory for the streaming smoke test).

---

## 3. Databricks setup and run

These steps target **Databricks Community Edition** (free tier).
Community Edition does not support Scala — all notebooks are Python.

### 3a. Prerequisites

- A Databricks Community Edition account at [community.cloud.databricks.com](https://community.cloud.databricks.com/).
- A running cluster. Community Edition provides a single-node cluster; create one via
  **Compute → Create cluster** (any runtime ≥ 13.x / Spark 3.5 works).

### 3b. Upload data to a Unity Catalog volume

The notebooks read CSV files from a Unity Catalog volume. Create a volume and upload
the four source files before running any notebook.

> DBFS (`dbfs:/FileStore/…`) is deprecated and unavailable in new Databricks accounts.
> Use Unity Catalog volumes instead.

**Create a volume (once)**  

1. In the Databricks sidebar, click **Catalog**.
2. Navigate to your catalog and schema (e.g. **main** → **default**).
3. Click **Create** → **Create volume**, name it `wlm_data`, and click **Create**.

This gives you the volume path `/Volumes/main/default/wlm_data/`.

> Replace `main` and `default` with your own catalog and schema names if they differ.

**Upload files via the UI**

1. Open the volume in Catalog Explorer.
2. Click **Upload to this volume**.
3. Upload each file, creating subdirectories (`monuments/`, `humdata/`, `katotth/`, `images/`) as needed.

| Local path | Volume destination |
|---|---|
| `data/wiki/monuments/wlm-ua-monuments.csv` | `/Volumes/main/default/wlm_data/monuments/wlm-ua-monuments.csv` |
| `data/humdata/ukraine-populated-places.csv` | `/Volumes/main/default/wlm_data/humdata/ukraine-populated-places.csv` |
| `data/katotth/katotth_koatuu.csv` | `/Volumes/main/default/wlm_data/katotth/katotth_koatuu.csv` |
| `data/images/wlm-UA-YYYY-images.csv` (one or more) | `/Volumes/main/default/wlm_data/images/` |

**Upload via the Databricks CLI v2 (optional)**

Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html)
(v0.200+ — not the legacy `databricks-cli` pip package):

```bash
# macOS
brew install databricks/tap/databricks
# Other platforms: see the install guide linked above
```

Configure authentication:

```bash
databricks configure        # enter your workspace URL and a personal access token
```

Copy files to the volume:

```bash
databricks fs cp data/wiki/monuments/wlm-ua-monuments.csv \
    dbfs:/Volumes/main/default/wlm_data/monuments/wlm-ua-monuments.csv --overwrite
databricks fs cp data/humdata/ukraine-populated-places.csv \
    dbfs:/Volumes/main/default/wlm_data/humdata/ukraine-populated-places.csv --overwrite
databricks fs cp data/katotth/katotth_koatuu.csv \
    dbfs:/Volumes/main/default/wlm_data/katotth/katotth_koatuu.csv --overwrite
# Copy one year of images as a starting point
databricks fs cp data/images/wlm-UA-2024-images.csv \
    dbfs:/Volumes/main/default/wlm_data/images/wlm-UA-2024-images.csv --overwrite
```

### 3c. Import the repo into Databricks Repos

Databricks Repos syncs this Git repository into the workspace so notebooks can import
the `src/wlm/` modules.

1. In the Databricks sidebar, click **Repos → Add Repo**.
2. Enter the Git URL of this repository.
3. Click **Create Repo**. Databricks clones the repo.
4. To pull the latest changes later: open the Repo, click the branch dropdown, and
   select **Pull**.

After cloning, the workspace will contain:

```
/Repos/<your-username>/wlm-data/
├── src/wlm/
├── notebooks/
│   ├── monuments.py
│   ├── images_streaming.py
│   └── run_tests.py
├── tests/
└── ...
```

### 3d. Run the monuments batch notebook

1. Open `notebooks/monuments.py` in the Databricks workspace.
2. Attach it to your cluster (**Connect** button in the top bar).
3. Optionally edit the config cell at the top to change volume paths:

   ```python
   MONUMENTS_CSV  = "/Volumes/main/default/wlm_data/monuments/wlm-ua-monuments.csv"
   HUMDATA_CSV    = "/Volumes/main/default/wlm_data/humdata/ukraine-populated-places.csv"
   KATOTTH_CSV    = "/Volumes/main/default/wlm_data/katotth/katotth_koatuu.csv"
   OUTPUT_DIR     = "/Volumes/main/default/wlm_data/output/monuments-with-cities"
   ```

4. Click **Run All**.

The notebook writes the joined dataset as parquet to `OUTPUT_DIR` and prints the
pictured-monument percentage table for all 27 Ukrainian regions.

Read the output back later:

```python
df = spark.read.parquet("/Volumes/main/default/wlm_data/output/monuments-with-cities")
df.show(10, truncate=False)
```

### 3e. Run the images streaming notebook

1. Open `notebooks/images_streaming.py`.
2. Attach to your cluster.
3. Edit the config cell as needed:

   ```python
   INPUT_DIR      = "/Volumes/main/default/wlm_data/images"
   HUMDATA_CSV    = "/Volumes/main/default/wlm_data/humdata/ukraine-populated-places.csv"
   OUTPUT_DIR     = "/Volumes/main/default/wlm_data/output"
   CHECKPOINT_DIR = "/Volumes/main/default/wlm_data/checkpoints"
   WINDOW_DUR     = "1 hour"      # time window size for windowed aggregation
   WATERMARK_DUR  = "10 minutes"  # late-data tolerance
   RUN_CUMULATIVE = False         # set True to run the cumulative query instead
   ```

4. Click **Run All**.

The notebook processes all CSV files currently in `INPUT_DIR` and terminates.
To process newly added files, upload them to `/Volumes/main/default/wlm_data/images/` and
re-run the notebook. Delete the checkpoint directory first if you want a clean rerun:

```python
dbutils.fs.rm("/Volumes/main/default/wlm_data/checkpoints/windowed", recurse=True)
```

**Output locations:**

| Mode | Parquet output |
|---|---|
| Windowed (default) | `/Volumes/main/default/wlm_data/output/windowed/` |
| Cumulative | `/Volumes/main/default/wlm_data/output/cumulative/` |

### 3f. Run the test suite on Databricks

`notebooks/run_tests.py` installs pytest on the cluster and runs all 31 tests.

1. Open `notebooks/run_tests.py`.
2. Attach to your cluster.
3. Click **Run All**.

The notebook installs pytest (`%pip install pytest`), then calls `pytest.main()`
and asserts that all tests pass. Test output appears in the cell results.

> **Note:** The tests in `tests/test_common.py` and `tests/test_monuments.py` read
> CSV files from the local `data/` directory. These files are present in the repo
> and are available automatically when running through Databricks Repos — no extra
> upload step is needed for the test data.
