#!/usr/bin/env python3
"""Kafka producer — sends ecommerce event CSV rows to a Kafka topic.

Supports two ingestion modes:
  batch  — pushes all rows as fast as possible (historical / backfill data)
  stream — sends rows one-by-one with a configurable delay (simulate real-time)

Usage (from repo root):
    python -m kafka_utils.producer --file data/fixtures/ecommerce_sample.csv
    python -m kafka_utils.producer --file data/raw/2019-Oct.csv --mode stream --delay 0.1
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce-events")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--file", required=True, help="Path to source CSV file")
    p.add_argument(
        "--topic",
        default=_TOPIC,
        help=f"Kafka topic (default: {_TOPIC})",
    )
    p.add_argument(
        "--bootstrap-servers",
        default=_BOOTSTRAP,
        help=f"Kafka brokers (default: {_BOOTSTRAP})",
    )
    p.add_argument(
        "--mode",
        choices=["batch", "stream"],
        default="batch",
        help="batch = all at once; stream = row-by-row with delay",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Seconds between messages in stream mode (default: 0.1)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    try:
        from confluent_kafka import Producer
    except ImportError:
        print(
            "confluent-kafka not installed. Run: pip install confluent-kafka",
            file=sys.stderr,
        )
        return 1

    file_path = Path(args.file).resolve()
    if not file_path.is_file():
        print(f"File not found: {file_path}", file=sys.stderr)
        return 1

    producer = Producer({"bootstrap.servers": args.bootstrap_servers})
    delivery_errors = 0

    def _on_delivery(err, _msg):
        nonlocal delivery_errors
        if err:
            delivery_errors += 1

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            producer.produce(
                args.topic,
                value=json.dumps(row).encode("utf-8"),
                callback=_on_delivery,
            )
            count += 1
            if args.mode == "stream":
                producer.flush()
                time.sleep(args.delay)
            elif count % 10_000 == 0:
                producer.flush()
                print(f"  … sent {count} rows", flush=True)

    producer.flush()

    if delivery_errors:
        print(
            f"Sent {count} messages to '{args.topic}' "
            f"({delivery_errors} delivery errors)",
            file=sys.stderr,
        )
        return 1

    print(
        f"Sent {count} messages to topic '{args.topic}' "
        f"(bootstrap: {args.bootstrap_servers})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
