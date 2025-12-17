#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import glob
import json
import os
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales")

CSV_GLOB = os.getenv("CSV_GLOB", "/producer/data/MOCK_DATA*.csv")
SLEEP_MS = int(os.getenv("SLEEP_MS", "0"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    linger_ms=50,
    retries=5,
)

files = sorted(glob.glob(CSV_GLOB))
if not files:
    raise SystemExit(f"No CSV files found by glob: {CSV_GLOB}")

sent = 0
for path in files:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            producer.send(KAFKA_TOPIC, row)
            sent += 1
            if SLEEP_MS > 0:
                time.sleep(SLEEP_MS / 1000.0)

producer.flush()
print(f"Processed a total of {sent} messages")
