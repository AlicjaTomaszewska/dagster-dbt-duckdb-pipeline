# dbt project `transform`

SQL transformations for the ecommerce warehouse (DuckDB). Layers:

- **Sources:** `main.bronze_raw_events` (loaded by Dagster bronze asset).
- **Silver:** `dim_*` dimensions and `fact_events` (incremental merge on `event_id`).
- **Gold:** `marketing_report` mart.

## Commands

```bash
dbt compile   # refresh manifest for Dagster
dbt build
dbt test
```

Profile lives in this directory: `profiles.yml` (paths relative to the dbt project folder).

See the repository root [README.md](../README.md) for orchestration and the Polars batch job.
