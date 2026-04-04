# BigDataPipeline — ecommerce batch analytics

Orchestrated **batch pipeline** for event-level ecommerce CSVs: ingest into **DuckDB (bronze)**, model **silver/gold in dbt**, coordinate runs with **Dagster**. Includes a **Polars** batch job (separate from the warehouse) that builds the same marketing mart grain into **Parquet**, satisfying the “second processing engine / single entry point” requirement without mandating a JVM cluster.

**Architecture diagram (Mermaid):** [docs/pipeline_architecture.md](docs/pipeline_architecture.md) — renders on GitHub/GitLab.

---

## Prerequisites

- Python **3.11+** (3.13 supported)
- Windows / macOS / Linux

---

## Quick start

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS / Linux

pip install -r requirements.txt
```

### 1) Optional: smoke test (fixture → dbt)

Uses a tiny CSV under `data/fixtures/` and a fresh local DuckDB file:

```bash
python scripts/verify_pipeline.py
```

### 2) Full local workflow

1. Place CSVs in `data/raw/` (gitignored when large; see `.gitignore`).
2. Generate dbt artifacts (required before Dagster can load `@dbt_assets`):

   ```bash
   cd transform
   dbt compile
   cd ..
   ```

3. Start Dagster UI (from repo root):

   ```bash
   dagster dev -m definitions
   ```

4. Materialize **raw_events_ingestion**, then **ecom_dbt_assets** (or run the full job).

### 3) dbt only (no Dagster)

```bash
cd transform
dbt compile
dbt build
```

Profile and project-local credentials: `transform/profiles.yml` (paths relative to the dbt project directory).

**DuckDB spill:** `transform/duckdb_spill/` is created when you load `assets` (Dagster) or run `scripts/verify_pipeline.py`. For **dbt CLI only**, create it once (`mkdir transform\duckdb_spill` on Windows). The path is set via **`config_options.temp_directory`** in `profiles.yml` (not `settings`), so DuckDB applies it at connect time only—avoiding dbt-duckdb’s per-cursor `SET temp_directory`, which triggers “Cannot switch temporary directory…” on large merges.

### 4) Polars mart (outside DuckDB / dbt)

Single entry point — reads CSV globs, writes Parquet (overwrite, idempotent output path):

```bash
python jobs/polars_marketing_mart.py --input-glob "data/fixtures/*.csv" --output-dir data/marts/polars_marketing_report
```

**Default input** is `data/fixtures/ecommerce_sample.csv` (small, committed). If you only have a huge file like `2019-Oct.csv` in `data/fixtures/`, do **not** use `*.csv` unless you intend to process all of it. Pass e.g. `--input-glob "data/fixtures/ecommerce_sample.csv"` or a path to a subset. Use **forward slashes** in CLI paths on PowerShell when in doubt.

Read the printed **absolute path** after a successful run, e.g. `Wrote N rows to C:\...\marketing_report.parquet`, then:

```powershell
python -c "import polars as pl; print(pl.read_parquet(r'data/marts/polars_marketing_report/marketing_report.parquet'))"
```

Use your own glob for production-scale files (e.g. `data/processed/*.csv`). Output directory is recreated/overwritten per run.

---

## Repository layout

| Path | Purpose |
|------|---------|
| `definitions.py` | Dagster `Definitions` (assets + `DbtCliResource`). |
| `assets.py` | Bronze ingestion asset + `@dbt_assets` wrapper. |
| `transform/` | dbt project (models, `profiles.yml`). |
| `jobs/polars_marketing_mart.py` | Alternative columnar engine: Polars → Parquet mart. |
| `scripts/verify_pipeline.py` | Automated smoke test. |
| `docs/pipeline_architecture.md` | Architecture diagram + layer notes. |
| `data/fixtures/` | Small committed sample for tests and demos. |

---

## Idempotency & data safety

- **Bronze:** does not re-insert rows from a source file whose `_file_name` already exists in `bronze_raw_events`.
- **Silver `fact_events`:** incremental **merge** on `event_id` (dedupe/upsert semantics).
- **Gold `marketing_report`:** full **table** rebuild from silver; stable grain `(report_date, brand, category_l1)` with unknown dimensions bucketed as `__UNKNOWN__`.
- **Ingestion** moves successfully loaded files from `data/raw/` to `data/processed/` (data is not deleted).

---

## Assignment checklist (mapping)

| Requirement | How it is met |
|-------------|----------------|
| Orchestrated pipeline + scalable processing | **Dagster** orchestrates ingestion + dbt; **DuckDB** is the analytical engine (swap profile for a remote warehouse for scale). **Polars** demonstrates out-of-DB scalable batch. |
| Runnable Spark *or other* big-data engine | **Polars** job (`jobs/polars_marketing_mart.py`) — single `main`, CLI args. |
| Architecture diagram in Git | **Mermaid** in `docs/pipeline_architecture.md`. |
| dbt as transformation layer | `transform/models` silver/gold + tests. |

---

## Development notes

- After changing dbt models, run `dbt compile` (or `dbt build`) so `transform/target/manifest.json` exists for Dagster.
- Do not commit `.venv/`, `transform/target/`, or `*.duckdb` — they are ignored by design.
