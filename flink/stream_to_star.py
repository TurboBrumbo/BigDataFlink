#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import sys
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import execute_values

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy


_PG_CONN = None

def pg_conn():
    global _PG_CONN
    if _PG_CONN is None or _PG_CONN.closed:
        _PG_CONN = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        _PG_CONN.autocommit = True
    return _PG_CONN

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flink-etl")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "bigdataFlink")
PG_USER = os.getenv("PG_USER", "bigdata")
PG_PASSWORD = os.getenv("PG_PASSWORD", "bddb")

PG_SCHEMA = os.getenv("PG_SCHEMA", "dw")


def _parse_date(s: str) -> Optional[datetime.date]:
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _to_int(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


class JsonToDict(MapFunction):
    def map(self, value: str) -> Dict[str, Any]:
        return json.loads(value)


class UpsertToPostgres(MapFunction):

    def open(self, runtime_context):
        self.conn = pg_conn()
        self.conn.autocommit = True

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _ensure_schema_prefix(self, t: str) -> str:
        return t if "." in t else f"{PG_SCHEMA}.{t}"

    def _upsert_dim_customer(self, cur, row: Dict[str, Any]) -> Optional[int]:
        source_customer_id = _to_int(row.get("sale_customer_id") or row.get("customer_id") or row.get("customer_key") or row.get("id"))
        if source_customer_id is None:
            return None

        first = (row.get("customer_first_name") or "").strip()
        last = (row.get("customer_last_name") or "").strip()
        name = (f"{first} {last}".strip()) or None
        country = (row.get("customer_country") or "").strip() or None
        age = _to_int(row.get("customer_age"))
        email = (row.get("customer_email") or "").strip() or None

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema_prefix("dim_customer")}
              (source_customer_id, customer_name, country, age, email)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (source_customer_id) DO UPDATE
              SET customer_name = EXCLUDED.customer_name,
                  country       = EXCLUDED.country,
                  age           = EXCLUDED.age,
                  email         = EXCLUDED.email
            RETURNING customer_key
            """,
            (source_customer_id, name, country, age, email),
        )
        return cur.fetchone()[0]

    def _upsert_dim_seller(self, cur, row: Dict[str, Any]) -> Optional[int]:
        source_seller_id = _to_int(row.get("sale_seller_id") or row.get("seller_id"))
        if source_seller_id is None:
            return None

        first = (row.get("seller_first_name") or "").strip()
        last = (row.get("seller_last_name") or "").strip()
        name = (f"{first} {last}".strip()) or None
        country = (row.get("seller_country") or "").strip() or None
        email = (row.get("seller_email") or "").strip() or None

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema_prefix("dim_seller")}
              (source_seller_id, seller_name, country, email)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (source_seller_id) DO UPDATE
              SET seller_name = EXCLUDED.seller_name,
                  country     = EXCLUDED.country,
                  email       = EXCLUDED.email
            RETURNING seller_key
            """,
            (source_seller_id, name, country, email),
        )
        return cur.fetchone()[0]

    def _upsert_dim_product(self, cur, row: Dict[str, Any]) -> Optional[int]:
        source_product_id = _to_int(row.get("sale_product_id") or row.get("product_id"))
        if source_product_id is None:
            return None

        name = (row.get("product_name") or "").strip() or None
        category = (row.get("product_category") or "").strip() or None
        price = _to_float(row.get("product_price"))
        rating = _to_float(row.get("product_rating"))
        reviews = _to_int(row.get("product_reviews"))

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema_prefix("dim_product")}
              (source_product_id, product_name, category, price, rating, reviews)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_product_id) DO UPDATE
              SET product_name = EXCLUDED.product_name,
                  category     = EXCLUDED.category,
                  price        = EXCLUDED.price,
                  rating       = EXCLUDED.rating,
                  reviews      = EXCLUDED.reviews
            RETURNING product_key
            """,
            (source_product_id, name, category, price, rating, reviews),
        )
        return cur.fetchone()[0]

    def _upsert_dim_store(self, cur, row: Dict[str, Any]) -> Optional[int]:
        name = (row.get("store_name") or "").strip()
        if not name:
            return None

        city = (row.get("store_city") or "").strip() or None
        country = (row.get("store_country") or "").strip() or None
        email = (row.get("store_email") or "").strip() or None

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema_prefix("dim_store")}
              (store_name, city, country, email)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (store_name) DO UPDATE
              SET city    = EXCLUDED.city,
                  country = EXCLUDED.country,
                  email   = EXCLUDED.email
            RETURNING store_key
            """,
            (name, city, country, email),
        )
        return cur.fetchone()[0]

    def _upsert_dim_supplier(self, cur, row: Dict[str, Any]) -> Optional[int]:
        name = (row.get("supplier_name") or "").strip()
        if not name:
            return None

        country = (row.get("supplier_country") or "").strip() or None
        email = (row.get("supplier_email") or "").strip() or None

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema_prefix("dim_supplier")}
              (supplier_name, country, email)
            VALUES (%s, %s, %s)
            ON CONFLICT (supplier_name) DO UPDATE
              SET country = EXCLUDED.country,
                  email   = EXCLUDED.email
            RETURNING supplier_key
            """,
            (name, country, email),
        )
        return cur.fetchone()[0]

    def _upsert_dim_date(self, cur, row: Dict[str, Any]) -> Optional[int]:
        d = _parse_date(row.get("sale_date") or "")
        if d is None:
            return None

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema_prefix("dim_date")}
              (sale_date, year, month, day)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (sale_date) DO UPDATE
              SET year  = EXCLUDED.year,
                  month = EXCLUDED.month,
                  day   = EXCLUDED.day
            RETURNING date_key
            """,
            (d, d.year, d.month, d.day),
        )
        return cur.fetchone()[0]

    def _insert_fact(self, cur, row: Dict[str, Any], keys: Dict[str, Optional[int]]):
        source_sale_id = _to_int(row.get("id") or row.get("sale_id"))
        if source_sale_id is None:
            return

        qty = _to_int(row.get("sale_quantity")) or 0
        revenue = _to_float(row.get("sale_total_price")) or 0.0

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema_prefix("fact_sales")}
              (source_sale_id, customer_key, seller_key, product_key, store_key, supplier_key, date_key,
               sale_quantity, sale_total_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_sale_id) DO UPDATE
              SET customer_key     = EXCLUDED.customer_key,
                  seller_key       = EXCLUDED.seller_key,
                  product_key      = EXCLUDED.product_key,
                  store_key        = EXCLUDED.store_key,
                  supplier_key     = EXCLUDED.supplier_key,
                  date_key         = EXCLUDED.date_key,
                  sale_quantity    = EXCLUDED.sale_quantity,
                  sale_total_price = EXCLUDED.sale_total_price
            """,
            (
                source_sale_id,
                keys.get("customer_key"),
                keys.get("seller_key"),
                keys.get("product_key"),
                keys.get("store_key"),
                keys.get("supplier_key"),
                keys.get("date_key"),
                qty,
                revenue,
            ),
        )

    def map(self, row: Dict[str, Any]) -> str:
        try:
            with self.conn.cursor() as cur:
                keys = {
                    "customer_key": self._upsert_dim_customer(cur, row),
                    "seller_key": self._upsert_dim_seller(cur, row),
                    "product_key": self._upsert_dim_product(cur, row),
                    "store_key": self._upsert_dim_store(cur, row),
                    "supplier_key": self._upsert_dim_supplier(cur, row),
                    "date_key": self._upsert_dim_date(cur, row),
                }
                self._insert_fact(cur, row, keys)
            return "ok"
        except Exception as e:
            return f"error: {type(e).__name__}: {e}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka")

    result = ds.map(JsonToDict(), output_type=Types.MAP(Types.STRING(), Types.STRING())) \
               .map(UpsertToPostgres(), output_type=Types.STRING())

    result.print()

    env.execute("Streaming ETL to Star Schema")


if __name__ == "__main__":
    main()
