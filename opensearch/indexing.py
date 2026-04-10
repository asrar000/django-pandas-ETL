"""OpenSearch index lifecycle and bulk indexing helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd
from opensearchpy import OpenSearch, helpers

from config.loader import get
from opensearch.serialization import records_from_dataframe
from utils.logger import get_logger

logger = get_logger(__name__)


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
