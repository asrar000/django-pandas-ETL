"""Public OpenSearch helpers for analytics indexing."""

from opensearch.client import as_bool, build_opensearch_client
from opensearch.indexing import (
    bulk_index_dataframe,
    create_or_replace_index,
)
from opensearch.mappings import CUSTOMER_INDEX_MAPPING, ORDER_INDEX_MAPPING
from opensearch.runner import opensearch_write
from opensearch.serialization import (
    build_json_spark_session,
    records_from_dataframe,
    spark_dataframe_from_pandas,
    serialize_value,
)

__all__ = [
    "CUSTOMER_INDEX_MAPPING",
    "ORDER_INDEX_MAPPING",
    "as_bool",
    "build_json_spark_session",
    "build_opensearch_client",
    "bulk_index_dataframe",
    "create_or_replace_index",
    "opensearch_write",
    "records_from_dataframe",
    "spark_dataframe_from_pandas",
    "serialize_value",
]
