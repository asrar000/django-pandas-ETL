"""DynamoDB Local table definition for the combined analytics output."""

from __future__ import annotations

from config.loader import get


def get_analytics_table_name() -> str:
    """Return the configured analytics DynamoDB table name."""
    return str(get("dynamodb.analytics_table", "etl_analytics"))


ANALYTICS_TABLE_DEFINITION = {
    "key_name": "record_id",
    "attribute_type": "S",
}