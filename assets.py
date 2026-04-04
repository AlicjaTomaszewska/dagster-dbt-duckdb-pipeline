"""Dagster assets: bronze CSV ingest into DuckDB and dbt-managed silver/gold models."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import duckdb
from dagster import asset
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

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


@asset(group_name="bronze")
def raw_events_ingestion():
    """Load new CSV files from data/raw into bronze_raw_events; move files to data/processed.

    If _file_name already exists in bronze, the file is skipped (not moved).
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

        insert_sql = """
            INSERT INTO bronze_raw_events
            SELECT *, ? AS _file_name, now() AS _ingested_at
            FROM read_csv_auto(?)
        """
        create_sql = """
            CREATE TABLE bronze_raw_events AS
            SELECT *, ? AS _file_name, now() AS _ingested_at
            FROM read_csv_auto(?)
        """
        params = [file, csv_path]
        try:
            conn.execute(insert_sql, params)
        except duckdb.CatalogException:
            conn.execute(create_sql, params)

        shutil.move(file_path, os.fspath(PROCESSED_DATA_PATH / file))
        processed_count += 1

    conn.close()
    return (
        f"Processed {processed_count} file(s)."
        + (f" Skipped {skipped_count} already-ingested file(s)." if skipped_count else "")
    )


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def ecom_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
