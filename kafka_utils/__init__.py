"""Kafka configuration shared by the producer script and Dagster consumer asset.

All values are overridable via environment variables.
"""

import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ecommerce-events")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "dagster-bronze-consumer")
