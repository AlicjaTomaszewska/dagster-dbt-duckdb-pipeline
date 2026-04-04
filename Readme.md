# BigDataPipeline — ecommerce batch analytics

Orchestrated **batch pipeline** for event-level ecommerce CSVs: ingest into **DuckDB (bronze)**, model **silver/gold in dbt**, coordinate runs with **Dagster**. Includes a **Polars** batch job (separate from the warehouse) that builds the same marketing mart grain into **Parquet**, satisfying the "second processing engine / single entry point" requirement without mandating a JVM cluster.

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

### 1) Smoke test (fixture → dbt + Polars)

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
mkdir duckdb_spill      # one-time, needed for DuckDB temp files on Windows
dbt compile
dbt build
```

Profile: `transform/profiles.yml` (paths relative to the dbt project directory).

### 4) Polars mart (outside DuckDB / dbt)

Single entry point — reads CSVs, writes Parquet (overwrite, idempotent output path):

```bash
python jobs/polars_marketing_mart.py
```

Default input is `data/fixtures/ecommerce_sample.csv`. For other files:

```bash
python jobs/polars_marketing_mart.py --input-glob "data/processed/*.csv" --output-dir data/marts/polars_marketing_report
```

Use **forward slashes** in CLI paths on PowerShell. Output directory is recreated per run.

---

## Repository layout

| Path | Purpose |
|------|---------|
| `definitions.py` | Dagster `Definitions` (assets + `DbtCliResource`). |
| `assets.py` | Bronze ingestion asset + `@dbt_assets` wrapper. |
| `transform/` | dbt project (models, profiles, tests). |
| `jobs/polars_marketing_mart.py` | Alternative columnar engine: Polars → Parquet mart. |
| `scripts/verify_pipeline.py` | Automated smoke test (dbt + Polars). |
| `docs/pipeline_architecture.md` | Architecture diagram + layer notes (Mermaid). |
| `data/fixtures/` | Small committed sample for tests and demos. |

---

## Data model

All dbt models use **`incremental` materialization with `merge`** for efficient processing without full table rebuilds.

### Silver layer

| Model | Merge key | Incremental filter | Purpose |
|-------|-----------|---------------------|---------|
| `fact_events` | `event_id` (MD5 hash of event attributes) | `_ingested_at > max(_loaded_at)` — filters by **ingest time**, not event time, so batches can arrive in any order. | Deduplicated event fact table. |
| `dim_products` | `product_id` | `_ingested_at > max(_updated_at)` | Latest brand and category per product. |
| `dim_users` | `user_id` | `_ingested_at > max(_updated_at)` | Latest name per user. |

### Gold layer

| Model | Merge key | Strategy |
|-------|-----------|----------|
| `marketing_report` | `(report_date, brand, category_l1)` | Identifies dates with new events (`_loaded_at > max(_refreshed_at)`), then **recomputes full aggregates for those dates** from all of `fact_events`. Merge overwrites stale partial totals with correct cumulative values. |

This means: if you load October, then November, then more October data — all three batches are processed correctly regardless of order.

---

## Idempotency & data safety

- **Bronze:** skips files whose `_file_name` already exists in `bronze_raw_events` (no duplicate inserts). Successfully loaded files move from `data/raw/` to `data/processed/` (source data is not deleted).
- **Silver `fact_events`:** incremental **merge** on `event_id` — reprocessed events upsert, not duplicate. Filter by `_ingested_at` (not business timestamp) ensures batches loaded out of chronological order are still picked up.
- **Silver dimensions:** merge on business key (`product_id` / `user_id`), latest attributes win.
- **Gold `marketing_report`:** affected dates recomputed fully from silver → same input always produces same output. Unaffected dates are untouched.
- **Polars job:** overwrites output directory each run.

---

## Assignment checklist

| Requirement | How it is met |
|-------------|----------------|
| Orchestrated pipeline + scalable processing | **Dagster** orchestrates ingestion + dbt; **DuckDB** is the analytical engine. **Polars** demonstrates out-of-DB scalable batch. |
| Runnable Spark *or other* big-data engine | **Polars** job (`jobs/polars_marketing_mart.py`) — single `main`, CLI args. |
| Architecture diagram in Git | **Mermaid** in `docs/pipeline_architecture.md`. |
| dbt as transformation layer | `transform/models/` — silver dimensions + fact, gold mart, singular test. |
| Idempotency | Merge-based incremental models, file-level dedup in bronze, full-day recompute in gold. |
| Maintainable structure | English comments, meaningful names, `.gitignore`, this README, verification script. |

---

## Development notes

- After changing dbt models, run `dbt compile` (or `dbt build`) so `transform/target/manifest.json` exists for Dagster.
- Do not commit `.venv/`, `transform/target/`, `transform/duckdb_spill/`, or `*.duckdb` — they are gitignored.
- To rebuild all models from scratch (e.g. after loading data in wrong order): `dbt build --full-refresh`.
