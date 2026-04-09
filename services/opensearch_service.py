"""
OpenSearch helpers for indexing analytics datasets with explicit mappings.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from opensearchpy import OpenSearch, helpers

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


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def build_opensearch_client() -> OpenSearch:
    """
    Build an OpenSearch client from config/config.yml.
    """
    username = str(get("opensearch.username", "") or "")
    password = str(get("opensearch.password", "") or "")
    verify_certs = _as_bool(get("opensearch.verify_certs", False))

    return OpenSearch(
        hosts=[
            {
                "host": get("opensearch.host", "localhost"),
                "port": int(get("opensearch.port", 9200)),
            }
        ],
        http_auth=(username, password) if username and password else None,
        use_ssl=_as_bool(get("opensearch.use_ssl", False)),
        verify_certs=verify_certs,
        ssl_assert_hostname=verify_certs,
        ssl_show_warn=verify_certs,
        timeout=int(get("opensearch.timeout", 30)),
        http_compress=True,
    )


def _create_or_replace_index(
    client: OpenSearch,
    index_name: str,
    mapping: dict[str, Any],
) -> None:
    recreate_indexes = _as_bool(get("opensearch.recreate_indexes", True))
    exists = client.indices.exists(index=index_name)

    if recreate_indexes and exists:
        client.indices.delete(index=index_name)
        exists = False
        logger.info(f"Recreated OpenSearch index mapping → {index_name}")

    if not exists:
        client.indices.create(index=index_name, body=mapping)
        logger.info(f"Created OpenSearch index → {index_name}")
    else:
        logger.info(f"Reusing existing OpenSearch index → {index_name}")


def _serialize_value(value: Any) -> Any:
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


def _records_from_dataframe(df: pd.DataFrame) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for record in df.to_dict(orient="records"):
        records.append(
            {
                field: _serialize_value(value)
                for field, value in record.items()
            }
        )
    return records


def _bulk_index_dataframe(
    client: OpenSearch,
    df: pd.DataFrame,
    index_name: str,
    mapping: dict[str, Any],
    id_field: str,
) -> int:
    _create_or_replace_index(client, index_name, mapping)
    records = _records_from_dataframe(df)

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
    """
    Create the analytics indices with explicit mappings and bulk index the
    transformed customer and order datasets.
    """
    client = build_opensearch_client()
    if not client.ping():
        raise RuntimeError(
            "Unable to connect to OpenSearch. Start the local container with "
            "`docker compose up -d opensearch` and retry."
        )

    customer_index = str(get("opensearch.customer_index", "customer_analytics"))
    order_index = str(get("opensearch.order_index", "order_analytics"))

    customer_docs = _bulk_index_dataframe(
        client,
        customer_df,
        customer_index,
        CUSTOMER_INDEX_MAPPING,
        "customer_id",
    )
    order_docs = _bulk_index_dataframe(
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
