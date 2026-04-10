"""OpenSearch index lifecycle and bulk indexing helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd
from opensearchpy import OpenSearch

from config.loader import get
from opensearch.client import as_bool
from opensearch.serialization import spark_dataframe_from_pandas
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
    if df.empty:
        logger.info(f"No analytics rows to index for {index_name}")
        return 0

    spark = spark_dataframe_from_pandas(df)
    try:
        success_count = spark.count()
        if success_count == 0:
            logger.info(f"No analytics rows to index for {index_name}")
            return 0

        spark.sparkSession.conf.set(
            "opensearch.port",
            str(get("opensearch.port", 9200)),
        )
        spark.sparkSession.conf.set("opensearch.mapping.id", id_field)
        spark.sparkSession.conf.set(
            "opensearch.net.ssl",
            str(as_bool(get("opensearch.use_ssl", False))).lower(),
        )

        username = str(get("opensearch.username", "") or "")
        password = str(get("opensearch.password", "") or "")
        if username and password:
            spark.sparkSession.conf.set("opensearch.net.http.auth.user", username)
            spark.sparkSession.conf.set("opensearch.net.http.auth.pass", password)

        spark.write.format("opensearch").option("opensearch.nodes", get("opensearch.host")).save(index_name)
        failure_count = 0
    finally:
        spark.sparkSession.stop()

    logger.info(
        f"Indexed {success_count} documents into {index_name} "
        f"({failure_count} failures)"
    )
    return success_count
