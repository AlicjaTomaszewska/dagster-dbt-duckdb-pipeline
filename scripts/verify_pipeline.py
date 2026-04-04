#!/usr/bin/env python3
"""
Smoke test: load fixture CSV into bronze in a fresh DuckDB file, then run dbt build.

Run from repository root:
    python scripts/verify_pipeline.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def _dbt_executable(root: Path) -> str:
    for candidate in (
        root / ".venv" / "Scripts" / "dbt.exe",
        root / ".venv" / "bin" / "dbt",
    ):
        if candidate.is_file():
            return str(candidate)
    return "dbt"


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    import duckdb

    fixture = root / "data" / "fixtures" / "ecommerce_sample.csv"
    if not fixture.is_file():
        print(f"Missing fixture: {fixture}", file=sys.stderr)
        return 1

    spill = root / "transform" / "duckdb_spill"
    spill.mkdir(parents=True, exist_ok=True)

    db_path = root / "transform" / "ecommerce_warehouse.duckdb"
    db_path.unlink(missing_ok=True)

    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE bronze_raw_events AS
        SELECT *, ? AS _file_name, now() AS _ingested_at
        FROM read_csv_auto(?)
        """,
        ["ecommerce_sample.csv", str(fixture.resolve())],
    )
    conn.close()

    dbt = _dbt_executable(root)
    env = {**os.environ, "PYTHONUTF8": "1"}
    transform = root / "transform"
    try:
        subprocess.run(
            [dbt, "compile"],
            cwd=str(transform),
            env=env,
            check=True,
        )
        subprocess.run(
            [dbt, "build"],
            cwd=str(transform),
            env=env,
            check=True,
        )
    except FileNotFoundError:
        print("dbt not found. Activate .venv or install dbt.", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        return exc.returncode

    print("verify_pipeline: dbt OK")

    polars_job = root / "jobs" / "polars_marketing_mart.py"
    pr = subprocess.run(
        [
            sys.executable,
            str(polars_job),
            "--input-glob",
            "data/fixtures/ecommerce_sample.csv",
        ],
        cwd=str(root),
        env=env,
    )
    if pr.returncode != 0:
        print("verify_pipeline: Polars job failed", file=sys.stderr)
        return pr.returncode

    print("verify_pipeline: Polars OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
