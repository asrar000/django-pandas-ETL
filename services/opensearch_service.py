"""OpenSearch helpers for indexing analytics datasets with explicit mappings."""
from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from opensearchpy import OpenSearch, helpers
from pyspark.sql import SparkSession

from config.loader import get
from utils.logger import get_logger

logger = get_logger(__name__)

_TEXT_WITH_KEYWORD = {
    "type": "text",
    "fields": {
        "keyword": {
            "type": "keyword",
            "ignore_above": 256,
        }
    },
}

CUSTOMER_INDEX_MAPPING = {
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "customer_id": {"type": "integer"},
            "full_name": _TEXT_WITH_KEYWORD,
            "email": {"type": "keyword"},
            "email_domain": {"type": "keyword"},
            "city": {"type": "keyword"},
            "customer_tenure_days": {"type": "integer"},
            "total_orders": {"type": "integer"},
            "total_spent": {"type": "double"},
            "avg_order_value": {"type": "double"},
            "lifetime_value_score": {"type": "double"},
            "customer_segment": {"type": "keyword"},
        },
    },
}

ORDER_INDEX_MAPPING = {
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "order_id": {"type": "keyword"},
            "customer_id": {"type": "integer"},
            "order_timestamp": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis",
            },
            "order_date": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis",
            },
            "order_hour": {"type": "integer"},
            "gross_amount": {"type": "double"},
            "total_discount_amount": {"type": "double"},
            "net_amount": {"type": "double"},
            "shipping_cost": {"type": "double"},
            "final_amount": {"type": "double"},
            "total_items": {"type": "integer"},
            "discount_ratio": {"type": "double"},
            "order_complexity_score": {"type": "integer"},
            "dominant_category": {"type": "keyword"},
            "payment_method": {"type": "keyword"},
            "shipping_provider": {"type": "keyword"},
        },
    },
}


def as_bool(value: Any) -> bool:
    """Normalize common truthy and falsy config values into a boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def build_opensearch_client() -> OpenSearch:
    """Build an OpenSearch client from config/config.yml."""
    username = str(get("opensearch.username", "") or "")
    password = str(get("opensearch.password", "") or "")
    verify_certs = as_bool(get("opensearch.verify_certs", False))

    return OpenSearch(
        hosts=[
            {
                "host": get("opensearch.host", "localhost"),
                "port": int(get("opensearch.port", 9200)),
            }
        ],
        http_auth=(username, password) if username and password else None,
        use_ssl=as_bool(get("opensearch.use_ssl", False)),
        verify_certs=verify_certs,
        ssl_assert_hostname=verify_certs,
        ssl_show_warn=verify_certs,
        timeout=int(get("opensearch.timeout", 30)),
        http_compress=True,
    )


def build_json_spark_session() -> SparkSession:
    """Build a temporary local Spark session for DataFrame JSON serialization."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    spark = (
        SparkSession.builder
        .appName("opensearch_json_serializer")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_or_replace_index(
    client: OpenSearch,
    index_name: str,
    mapping: dict[str, Any],
) -> None:
    """Ensure an index exists, preserving any existing index and mapping."""
    exists = client.indices.exists(index=index_name)

    if exists:
        logger.info(f"Reusing existing OpenSearch index → {index_name}")
        return

    client.indices.create(index=index_name, body=mapping)
    logger.info(f"Created OpenSearch index → {index_name}")


def serialize_value(value: Any) -> Any:
    """Convert Pandas- and Python-specific values into JSON-safe primitives."""
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        value = value.to_pydatetime()
    elif hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except Exception:
            pass

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)

    return value


def records_from_dataframe(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a DataFrame into serialized record dictionaries for indexing."""
    if df.empty:
        return []

    normalized_records = [
        {
            field: serialize_value(value)
            for field, value in record.items()
        }
        for record in df.to_dict(orient="records")
    ]

    spark = build_json_spark_session()
    try:
        spark_df = spark.createDataFrame(normalized_records)
        return [json.loads(record) for record in spark_df.toJSON().collect()]
    finally:
        spark.stop()


def bulk_index_dataframe(
    client: OpenSearch,
    df: pd.DataFrame,
    index_name: str,
    mapping: dict[str, Any],
    id_field: str,
) -> int:
    """Create the target index if needed and bulk index all DataFrame rows."""
    create_or_replace_index(client, index_name, mapping)
    records = records_from_dataframe(df)

    if not records:
        logger.info(f"No analytics rows to index for {index_name}")
        return 0

    success_count, failure_count = helpers.bulk(
        client,
        [
            {
                "_index": index_name,
                "_id": record[id_field],
                "_source": record,
            }
            for record in records
        ],
        chunk_size=500,
        refresh="wait_for",
        request_timeout=int(get("opensearch.timeout", 30)),
        stats_only=True,
        raise_on_error=True,
    )

    logger.info(
        f"Indexed {success_count} documents into {index_name} "
        f"({failure_count} failures)"
    )
    return success_count


def index_analytics_to_opensearch(
    customer_df: pd.DataFrame,
    order_df: pd.DataFrame,
) -> dict[str, int]:
    """Create the analytics indices and bulk index the customer and order datasets."""
    client = build_opensearch_client()
    if not client.ping():
        raise RuntimeError(
            "Unable to connect to OpenSearch. Start the local container with "
            "`docker compose up -d opensearch` and retry."
        )

    customer_index = str(get("opensearch.customer_index", "customer_analytics"))
    order_index = str(get("opensearch.order_index", "order_analytics"))

    customer_docs = bulk_index_dataframe(
        client,
        customer_df,
        customer_index,
        CUSTOMER_INDEX_MAPPING,
        "customer_id",
    )
    order_docs = bulk_index_dataframe(
        client,
        order_df,
        order_index,
        ORDER_INDEX_MAPPING,
        "order_id",
    )

    return {
        "customer_docs": customer_docs,
        "order_docs": order_docs,
    }
