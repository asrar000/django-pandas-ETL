"""Public helpers for local DynamoDB analytics storage."""

from dynamodb_local.client import build_dynamodb_resource
from dynamodb_local.tables import (
    ANALYTICS_TABLE_DEFINITION,
    get_analytics_table_name,
)
from dynamodb_local.writer import (
    ensure_analytics_table_exists,
    write_combined_dataframes_to_table,
    load_write_status,
)


def write_analytics_to_dynamodb(*args, **kwargs):
    """Write the combined Spark analytics DataFrame to DynamoDB Local."""
    from dynamodb_local.runner import write_analytics_to_dynamodb as _write

    return _write(*args, **kwargs)


def main(argv=None):
    """Run the DynamoDB Local helper CLI."""
    from dynamodb_local.runner import main as _main

    return _main(argv)


__all__ = [
    "ANALYTICS_TABLE_DEFINITION",
    "build_dynamodb_resource",
    "ensure_analytics_table_exists",
    "get_analytics_table_name",
    "load_write_status",
    "main",
    "write_analytics_to_dynamodb",
    "write_combined_dataframes_to_table",
]