# BigDataPipeline — ecommerce batch + streaming analytics

Orchestrated pipeline for event-level ecommerce data: ingest into **DuckDB (bronze)** via **batch CSV** or **Kafka streaming**, model **silver/gold in dbt**, coordinate runs with **Dagster**. Includes a **Polars** batch job (separate from the warehouse) that builds the same marketing mart grain into **Parquet**.

Two ingestion paths coexist so you can bulk-load historical data with CSV batch and then stream incremental updates via Kafka — or do both at the same time.

**Architecture diagram (Mermaid):** [docs/pipeline_architecture.md](docs/pipeline_architecture.md) — renders on GitHub/GitLab.

---

## Prerequisites

- Python **3.11+** (3.13 supported)
- **Docker** (for Kafka — optional if you only need batch mode)
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

### 2) Start Kafka (Docker)

```bash
docker-compose up -d
```

This starts Zookeeper + Kafka. Topic `ecommerce-events` is auto-created on first produce.

To stop:

```bash
docker-compose down
```

### 3) Full local workflow — batch mode (historical data)

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

4. **Automatic:** the `new_csv_file_sensor` detects CSVs in `data/raw/` every 30 s and triggers the `batch_pipeline_job` (ingestion + dbt build).
5. **Manual:** materialize `raw_events_ingestion`, then `ecom_dbt_assets` from the Dagster UI.

### 4) Full local workflow — Kafka streaming (incremental data)

1. Make sure Kafka is running (`docker-compose up -d`).
2. Push data to Kafka with the producer:

   ```bash
   # Batch mode — send entire CSV at once (historical / backfill)
   python -m kafka_utils.producer --file data/fixtures/ecommerce_sample.csv

   # Stream mode — simulate real-time with delays between rows
   python -m kafka_utils.producer --file data/fixtures/ecommerce_sample.csv --mode stream --delay 0.5
   ```

3. Start Dagster UI:

   ```bash
   dagster dev -m definitions
   ```

4. **Option A — manual:** materialize `kafka_events_ingestion` from the Dagster UI, then `ecom_dbt_assets`.
5. **Option B — scheduled:** enable `kafka_ingestion_schedule` in the Dagster UI (Schedules tab). It polls Kafka every 5 minutes and ingests any available messages automatically.

### 5) Combined workflow (recommended for production)

1. **Historical load:** batch-ingest large CSVs via `data/raw/` → sensor triggers automatically.
2. **Incremental:** start the Kafka producer feeding live events → enable `kafka_ingestion_schedule` for continuous consumption.
3. Both paths write to the same `bronze_raw_events` table. Downstream dbt models (silver/gold) are agnostic to the source.

### 6) dbt only (no Dagster)

```bash
cd transform
mkdir duckdb_spill      # one-time, needed for DuckDB temp files on Windows
dbt compile
dbt build
```

Profile: `transform/profiles.yml` (paths relative to the dbt project directory).

### 7) Polars mart (outside DuckDB / dbt)

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

## Kafka producer reference

```
python -m kafka_utils.producer --help
```

| Flag | Default | Description |
|------|---------|-------------|
| `--file` | *(required)* | Path to source CSV file. |
| `--topic` | `ecommerce-events` | Kafka topic name. |
| `--bootstrap-servers` | `localhost:9092` | Kafka broker address. |
| `--mode` | `batch` | `batch` = all rows at once; `stream` = one-by-one with delay. |
| `--delay` | `0.1` | Seconds between messages in stream mode. |

All defaults can be overridden via environment variables: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`.

---

## Repository layout

| Path | Purpose |
|------|---------|
| `definitions.py` | Dagster `Definitions` (assets, jobs, sensors, schedules). |
| `assets.py` | Bronze ingestion assets (batch + Kafka) + `@dbt_assets` wrapper. |
| `transform/` | dbt project (models, profiles, tests). |
| `kafka_utils/` | Kafka config + producer script. |
| `docker-compose.yml` | Kafka + Zookeeper infrastructure. |
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
| `fact_events` | `event_id` (MD5 hash of event attributes) | `_ingested_at > max(_loaded_at)` — filters by **ingest time**, not event time, so batches can arrive in any order. | Deduplicated event fact table using incremental append + anti-join on `event_id` (memory-friendlier for large loads). |
| `dim_products` | `product_id` | `_ingested_at > max(_updated_at)` | Latest brand and category per product. |
| `dim_users` | `user_id` | `_ingested_at > max(_updated_at)` | Latest name per user. |

### Gold layer

| Model | Merge key | Strategy |
|-------|-----------|----------|
| `marketing_report` | `(report_date, brand, category_l1)` | Identifies dates with new events (`_loaded_at > max(_refreshed_at)`), then **recomputes full aggregates for those dates** from all of `fact_events`. Merge overwrites stale partial totals with correct cumulative values. |

This means: if you load October, then November, then more October data — all three batches are processed correctly regardless of order.

---

## Idempotency & data safety

- **Bronze (batch):** skips files whose `_file_name` already exists in `bronze_raw_events` (no duplicate inserts). Successfully loaded files move from `data/raw/` to `data/processed/` (source data is not deleted).
- **Bronze (Kafka):** Kafka consumer group offsets track which messages have been processed. Offsets are committed only after successful DuckDB insert.
- **Silver `fact_events`:** incremental **append** with `NOT EXISTS` anti-join on `event_id` — prevents duplicates with lower memory pressure than merge on very large batches. Filter by `_ingested_at` (not business timestamp) ensures batches loaded out of chronological order are still picked up.
- **Silver dimensions:** merge on business key (`product_id` / `user_id`), latest attributes win.
- **Gold `marketing_report`:** affected dates recomputed fully from silver → same input always produces same output. Unaffected dates are untouched.
- **Polars job:** overwrites output directory each run.

---

## Dagster automation

| Trigger | Type | Target job | Default status |
|---------|------|-----------|----------------|
| `new_csv_file_sensor` | Sensor (every 30 s) | `batch_pipeline_job` | Active |
| `kafka_ingestion_schedule` | Cron (`*/5 * * * *`) | `kafka_ingestion_job` | **Stopped** (enable in UI when Kafka is running) |

- The **sensor** watches `data/raw/` for new CSV files and automatically triggers batch ingestion + dbt build.
- The **schedule** periodically polls the Kafka topic. Enable it from the Dagster UI Schedules tab when you have Kafka running and want continuous consumption.

---

## Assignment checklist

| Requirement | How it is met |
|-------------|----------------|
| Orchestrated pipeline + scalable processing | **Dagster** orchestrates ingestion + dbt; **DuckDB** is the analytical engine. **Polars** demonstrates out-of-DB scalable batch. |
| Queue system (Kafka) | `docker-compose.yml` runs Kafka; `kafka_utils/producer.py` sends CSV data; `kafka_events_ingestion` asset consumes. Batch path preserved alongside. |
| Automatic data reading/injection | **Sensor** auto-detects new CSV files; **schedule** auto-polls Kafka topic. |
| Runnable Spark *or other* big-data engine | **Polars** job (`jobs/polars_marketing_mart.py`) — single `main`, CLI args. |
| Architecture diagram in Git | **Mermaid** in `docs/pipeline_architecture.md`. |
| dbt as transformation layer | `transform/models/` — silver dimensions + fact, gold mart, singular test. |
| Idempotency | Merge-based incremental models, file-level dedup in bronze, offset tracking in Kafka, full-day recompute in gold. |
| Maintainable structure | English comments, meaningful names, `.gitignore`, this README, verification script. |

---

## Development notes

- After changing dbt models, run `dbt compile` (or `dbt build`) so `transform/target/manifest.json` exists for Dagster.
- Do not commit `.venv/`, `transform/target/`, `transform/duckdb_spill/`, or `*.duckdb` — they are gitignored.
- To rebuild all models from scratch (e.g. after loading data in wrong order): `dbt build --full-refresh`.
- Kafka config (bootstrap servers, topic, consumer group) can be overridden via env vars — see `kafka_utils/__init__.py`.
