#!/usr/bin/env python3
"""
Polars batch job — marketing mart at the same grain as dbt `marketing_report`.

Alternative scalable columnar engine (single entry point, no JVM). Reads denormalized
event CSVs, aggregates, writes Parquet with a clean overwrite (idempotent output path).

Usage:
    python jobs/polars_marketing_mart.py --input-glob "data/fixtures/*.csv" --output-dir data/marts/polars_marketing_report

Default input is the small ``data/fixtures/ecommerce_sample.csv``. Use ``--input-glob "data/fixtures/*.csv"``
only if you mean to scan every CSV in that folder (can be very slow if a huge extract lives there).
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path


def _is_abs_path(s: str) -> bool:
    p = s.replace("\\", "/").strip()
    return os.path.isabs(p) or (len(p) >= 2 and p[0].isalpha() and p[1] == ":")


def _collect_input_paths(root: Path, pattern: str) -> list[Path]:
    """Resolve CSV inputs: single file, or glob (``*`` / ``?``). Uses pathlib (reliable on Windows)."""
    ptn = pattern.replace("\\", "/").strip()
    has_glob = any(ch in ptn for ch in "*?[")

    if not _is_abs_path(ptn):
        rel = ptn.lstrip("/")
        if not has_glob:
            f = root / rel
            return [f] if f.is_file() else []
        return sorted(x for x in root.glob(rel) if x.is_file())

    p = Path(ptn.replace("/", os.sep))
    if not has_glob:
        return [p] if p.is_file() else []
    return sorted(x for x in p.parent.glob(p.name) if x.is_file())


def _resolve_output_dir(root: Path, user_path: str) -> Path:
    """Output directory under repo root unless ``user_path`` is absolute."""
    p = user_path.replace("\\", "/").strip()
    if os.path.isabs(p):
        return Path(p).resolve()
    return (root / p.lstrip("/")).resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-glob",
        default="data/fixtures/ecommerce_sample.csv",
        help=(
            "CSV file or glob under repo root (e.g. data/fixtures/ecommerce_sample.csv). "
            "Default is the small committed sample so the job finishes quickly. "
            "Use data/fixtures/*.csv only if you intend to read every CSV in that folder."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="data/marts/polars_marketing_report",
        help="Output directory for Parquet (removed and rewritten each run).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        import polars as pl
    except ImportError:
        print("Install Polars: pip install -r requirements.txt", file=sys.stderr)
        return 1

    root = Path(__file__).resolve().parents[1]
    paths = _collect_input_paths(root, args.input_glob)

    if not paths:
        hint = root / args.input_glob.replace("\\", "/").lstrip("/")
        print(
            f"No input CSV files found for pattern {args.input_glob!r} "
            f"(resolved under {root}). Example missing file: {hint}",
            file=sys.stderr,
        )
        return 1

    frames = [
        pl.read_csv(p, infer_schema_length=10_000, try_parse_dates=False) for p in paths
    ]
    df = pl.concat(frames, how="vertical_relaxed")

    required = {"event_time", "event_type", "brand", "category_code", "price"}
    missing = required - set(df.columns)
    if missing:
        print(f"Missing required columns: {sorted(missing)}", file=sys.stderr)
        return 1

    df = df.with_columns(
        pl.col("category_code")
        .cast(pl.Utf8)
        .fill_null("")
        .str.split(".")
        .list.first()
        .alias("_category_l1")
    )

    df = df.with_columns(
        pl.coalesce(
            pl.when(pl.col("brand").cast(pl.Utf8).str.strip_chars() != "")
            .then(pl.col("brand").cast(pl.Utf8).str.strip_chars()),
            pl.lit("__UNKNOWN__"),
        ).alias("brand_norm"),
        pl.coalesce(
            pl.when(
                pl.col("_category_l1").cast(pl.Utf8).str.strip_chars() != ""
            )
            .then(pl.col("_category_l1").cast(pl.Utf8).str.strip_chars()),
            pl.lit("__UNKNOWN__"),
        ).alias("category_l1_norm"),
    )

    # CSV uses "... 00:00:00 UTC"; strip suffix so Polars parses reliably on all versions.
    df = df.with_columns(
        pl.col("event_time")
        .cast(pl.Utf8)
        .str.replace(r"\s+UTC\s*$", "", literal=False)
        .str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False)
        .dt.replace_time_zone("UTC")
        .alias("_event_ts")
    )

    df = df.with_columns(pl.col("_event_ts").dt.date().alias("report_date"))

    agg = (
        df.group_by(["report_date", "brand_norm", "category_l1_norm"])
        .agg(
            (pl.col("event_type") == "view").sum().alias("total_views"),
            (pl.col("event_type") == "purchase").sum().alias("total_purchases"),
            pl.when(pl.col("event_type") == "purchase")
            .then(pl.col("price").cast(pl.Float64))
            .otherwise(0.0)
            .sum()
            .alias("revenue"),
        )
        .with_columns(
            pl.when(pl.col("total_views") > 0)
            .then(
                (
                    100.0
                    * pl.col("total_purchases").cast(pl.Float64)
                    / pl.col("total_views").cast(pl.Float64)
                ).round(2)
            )
            .otherwise(None)
            .alias("conversion_rate_pct")
        )
        .with_columns(pl.lit(datetime.now(timezone.utc)).alias("_refreshed_at"))
    )

    out = agg.select(
        pl.col("report_date"),
        pl.col("brand_norm").alias("brand"),
        pl.col("category_l1_norm").alias("category_l1"),
        "total_views",
        "total_purchases",
        "revenue",
        "conversion_rate_pct",
        "_refreshed_at",
    )

    output_dir = _resolve_output_dir(root, args.output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "marketing_report.parquet"
    out.write_parquet(out_path, compression="zstd")
    print(f"Wrote {out.height} rows to {out_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
