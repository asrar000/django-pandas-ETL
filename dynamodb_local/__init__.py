"""Public helpers for local DynamoDB analytics storage."""

from dynamodb_local.client import build_dynamodb_client, build_dynamodb_resource
from dynamodb_local.tables import (
    CUSTOMER_TABLE_DEFINITION,
    ORDER_TABLE_DEFINITION,
    get_customer_table_name,
    get_order_table_name,
)
from dynamodb_local.writer import (
    delete_table,
    ensure_analytics_tables_exist,
    scan_table_items,
    write_spark_dataframe_to_table,
)


def write_analytics_to_dynamodb(*args, **kwargs):
    """Write the Spark analytics DataFrames to DynamoDB Local."""
    from dynamodb_local.runner import write_analytics_to_dynamodb as _write

    return _write(*args, **kwargs)


def main(argv=None):
    """Run the DynamoDB Local helper CLI."""
    from dynamodb_local.runner import main as _main

    return _main(argv)

__all__ = [
    "CUSTOMER_TABLE_DEFINITION",
    "ORDER_TABLE_DEFINITION",
    "build_dynamodb_client",
    "build_dynamodb_resource",
    "delete_table",
    "ensure_analytics_tables_exist",
    "get_customer_table_name",
    "get_order_table_name",
    "main",
    "scan_table_items",
    "write_analytics_to_dynamodb",
    "write_spark_dataframe_to_table",
]
