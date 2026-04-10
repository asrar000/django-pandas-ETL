"""OpenSearch orchestration entry points for analytics indexing."""

from __future__ import annotations

import pandas as pd

from config.loader import get
from opensearch.client import build_opensearch_client
from opensearch.indexing import bulk_index_dataframe
from opensearch.mappings import CUSTOMER_INDEX_MAPPING, ORDER_INDEX_MAPPING


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
