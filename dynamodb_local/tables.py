"""DynamoDB Local table definitions for analytics outputs."""

from __future__ import annotations

from config.loader import get


def get_customer_table_name() -> str:
    """Return the configured customer analytics DynamoDB table name."""
    return str(get("dynamodb.customer_table", "customer_analytics"))


def get_order_table_name() -> str:
    """Return the configured order analytics DynamoDB table name."""
    return str(get("dynamodb.order_table", "order_analytics"))


CUSTOMER_TABLE_DEFINITION = {
    "key_name": "customer_id",
    "attribute_type": "N",
}

ORDER_TABLE_DEFINITION = {
    "key_name": "order_id",
    "attribute_type": "S",
}
