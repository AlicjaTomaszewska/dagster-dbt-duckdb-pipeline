"""Dagster assets: bronze CSV ingest into DuckDB and dbt-managed silver/gold models.

Two bronze ingestion paths (both write to the same bronze_raw_events table):
  1. raw_events_ingestion  — batch: reads CSVs from data/raw/, moves to data/processed/
  2. kafka_events_ingestion — streaming: consumes JSON rows from a Kafka topic
"""

from __future__ import annotations

import csv
import json
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
from dagster import asset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from kafka_utils import KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, KAFKA_TOPIC

BASE_DIR = Path(__file__).resolve().parent
RAW_DATA_PATH = BASE_DIR / "data" / "raw"
PROCESSED_DATA_PATH = BASE_DIR / "data" / "processed"
DB_PATH = BASE_DIR / "transform" / "ecommerce_warehouse.duckdb"

DBT_PROJECT_DIR = BASE_DIR / "transform"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

# Match profiles.yml duckdb settings.temp_directory (relative to dbt project / cwd).
(DBT_PROJECT_DIR / "duckdb_spill").mkdir(parents=True, exist_ok=True)


def _resolve_dbt_executable() -> str:
    for candidate in (
        BASE_DIR / ".venv" / "Scripts" / "dbt.exe",
        BASE_DIR / ".venv" / "bin" / "dbt",
    ):
        if candidate.is_file():
            return os.fspath(candidate)
    return "dbt"


dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR),
    dbt_executable=_resolve_dbt_executable(),
)


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """Map dbt source `main.bronze_raw_events` to the Dagster bronze ingestion asset key."""

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return ["raw_events_ingestion"]
        return super().get_asset_key(dbt_resource_props)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _bronze_table_exists(conn: duckdb.DuckDBPyConnection) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = 'bronze_raw_events'
        """
    ).fetchone()
    return row is not None and row[0] > 0


def _file_already_ingested(conn: duckdb.DuckDBPyConnection, file_name: str) -> bool:
    if not _bronze_table_exists(conn):
        return False
    result = conn.execute(
        "SELECT 1 FROM bronze_raw_events WHERE _file_name = ? LIMIT 1",
        [file_name],
    ).fetchone()
    return result is not None


def _load_csv_to_bronze(conn: duckdb.DuckDBPyConnection, csv_path: str, file_name: str):
    """Insert a CSV into bronze_raw_events via read_csv_auto (CREATE or INSERT)."""
    params = [file_name, csv_path]
    try:
        conn.execute(
            "INSERT INTO bronze_raw_events "
            "SELECT *, ? AS _file_name, now() AS _ingested_at "
            "FROM read_csv_auto(?)",
            params,
        )
    except duckdb.CatalogException:
        conn.execute(
            "CREATE TABLE bronze_raw_events AS "
            "SELECT *, ? AS _file_name, now() AS _ingested_at "
            "FROM read_csv_auto(?)",
            params,
        )


# ---------------------------------------------------------------------------
# Bronze assets
# ---------------------------------------------------------------------------

@asset(group_name="bronze")
def raw_events_ingestion():
    """Batch path: load new CSV files from data/raw into bronze_raw_events.

    Files whose _file_name is already present in bronze are skipped (idempotent).
    Successfully loaded files are moved to data/processed/.
    """
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(os.fspath(DB_PATH))
    files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".csv")]

    if not files:
        conn.close()
        return "No new files to process."

    processed_count = 0
    skipped_count = 0

    for file in files:
        file_path = (RAW_DATA_PATH / file).resolve()
        csv_path = file_path.as_posix()

        if _file_already_ingested(conn, file):
            skipped_count += 1
            continue

        _load_csv_to_bronze(conn, csv_path, file)
        shutil.move(file_path, os.fspath(PROCESSED_DATA_PATH / file))
        processed_count += 1

    conn.close()
    return (
        f"Processed {processed_count} file(s)."
        + (f" Skipped {skipped_count} already-ingested file(s)." if skipped_count else "")
    )


@asset(group_name="bronze")
def kafka_events_ingestion(context):
    """Streaming path: consume events from Kafka and load into bronze_raw_events.

    Polls the configured topic for available messages (micro-batch with timeout),
    writes them to a temporary CSV, then uses the same read_csv_auto path as
    batch ingestion for consistent schema handling.

    Falls back gracefully when Kafka or confluent-kafka is unavailable.
    """
    try:
        from confluent_kafka import Consumer, KafkaError  # noqa: WPS433
    except ImportError:
        context.log.error(
            "confluent-kafka not installed. Run: pip install confluent-kafka"
        )
        return "confluent-kafka not installed."

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    try:
        consumer.subscribe([KAFKA_TOPIC])

        rows: list[dict] = []
        empty_polls = 0
        while empty_polls < 10:
            msg = consumer.poll(0.5)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                context.log.error(f"Kafka error: {msg.error()}")
                break
            empty_polls = 0
            try:
                rows.append(json.loads(msg.value().decode("utf-8")))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                context.log.warning(f"Skipping malformed message: {exc}")

        if not rows:
            context.log.info("No new messages in Kafka topic.")
            return "No new messages in Kafka topic."

        batch_id = f"kafka_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Write rows to a temp CSV so read_csv_auto infers schema consistently.
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8",
        )
        try:
            fieldnames = list(rows[0].keys())
            writer = csv.DictWriter(tmp, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            tmp.close()

            tmp_csv = Path(tmp.name).resolve().as_posix()
            conn = duckdb.connect(os.fspath(DB_PATH))
            _load_csv_to_bronze(conn, tmp_csv, batch_id)
            conn.close()
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)

        consumer.commit()
        result = f"Loaded {len(rows)} events from Kafka (batch: {batch_id})"
        context.log.info(result)
        return result
    finally:
        consumer.close()


# ---------------------------------------------------------------------------
# dbt assets (silver + gold)
# ---------------------------------------------------------------------------

@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def ecom_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
