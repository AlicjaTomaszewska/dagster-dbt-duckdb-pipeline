"""Dagster entrypoint: assets, jobs, sensors, and schedules.

Two automatic ingestion triggers:
  1. new_csv_file_sensor  — watches data/raw/ for new CSVs → batch pipeline
  2. kafka_ingestion_schedule — periodically polls Kafka topic (disabled by default)
"""

from __future__ import annotations

import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    define_asset_job,
    load_assets_from_modules,
    sensor,
)

import assets

all_assets = load_assets_from_modules([assets])

# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

batch_pipeline_job = define_asset_job(
    name="batch_pipeline_job",
    description="Batch CSV ingestion (raw_events_ingestion) + dbt build.",
    selection=(
        AssetSelection.keys("raw_events_ingestion")
        | AssetSelection.keys("raw_events_ingestion").downstream()
    ),
)

kafka_ingestion_job = define_asset_job(
    name="kafka_ingestion_job",
    description="Consume available messages from Kafka topic into bronze.",
    selection=AssetSelection.keys("kafka_events_ingestion"),
)


# ---------------------------------------------------------------------------
# Sensors
# ---------------------------------------------------------------------------

@sensor(
    job=batch_pipeline_job,
    minimum_interval_seconds=30,
    default_status=DefaultSensorStatus.RUNNING,
)
def new_csv_file_sensor(context: SensorEvaluationContext):
    """Watch data/raw/ for new CSV files and trigger the batch pipeline automatically."""
    raw_path = assets.RAW_DATA_PATH
    raw_path.mkdir(parents=True, exist_ok=True)

    files = sorted(f for f in os.listdir(raw_path) if f.endswith(".csv"))
    if not files:
        return SkipReason("No CSV files in data/raw/")

    current = "|".join(files)
    if current == (context.cursor or ""):
        return SkipReason("No new files since last check")

    context.update_cursor(current)
    yield RunRequest(run_key=f"csv_batch_{hash(current)}")


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

kafka_ingestion_schedule = ScheduleDefinition(
    name="kafka_ingestion_schedule",
    job=kafka_ingestion_job,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.STOPPED,
)


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": assets.dbt_resource,
    },
    jobs=[batch_pipeline_job, kafka_ingestion_job],
    sensors=[new_csv_file_sensor],
    schedules=[kafka_ingestion_schedule],
)
