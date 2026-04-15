"""DynamoDB Local orchestration and CLI helpers."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal

from pyspark.sql import DataFrame

from dynamodb_local.client import build_dynamodb_client, build_dynamodb_resource
from dynamodb_local.tables import get_customer_table_name, get_order_table_name
from dynamodb_local.writer import (
    delete_table,
    ensure_analytics_tables_exist,
    scan_table_items,
    write_spark_dataframe_to_table,
)


def json_default(value):
    """Serialize Decimal values for CLI JSON output."""
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Unsupported value for JSON serialization: {type(value)!r}")


def write_analytics_to_dynamodb(
    customer_df: DataFrame,
    order_df: DataFrame,
) -> dict[str, int]:
    """Write both Spark analytics tables to DynamoDB Local."""
    dynamodb = build_dynamodb_resource()
    tables = ensure_analytics_tables_exist(dynamodb)

    customer_items = write_spark_dataframe_to_table(
        customer_df,
        tables["customer_table"],
    )
    order_items = write_spark_dataframe_to_table(
        order_df,
        tables["order_table"],
    )

    return {
        "customer_items": customer_items,
        "order_items": order_items,
    }


def main(argv: list[str] | None = None) -> int:
    """Run DynamoDB Local utility commands from the command line."""
    parser = argparse.ArgumentParser(description="DynamoDB Local helper commands")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list-tables", help="List local DynamoDB tables")

    delete_table_parser = subparsers.add_parser(
        "delete-table",
        help="Delete one local DynamoDB table",
    )
    delete_table_parser.add_argument("table_name")

    subparsers.add_parser("delete-all", help="Delete both analytics tables")

    scan_table_parser = subparsers.add_parser(
        "scan-table",
        help="Scan and print items from one local DynamoDB table",
    )
    scan_table_parser.add_argument("table_name")
    scan_table_parser.add_argument("--limit", type=int, default=10)

    scan_all_parser = subparsers.add_parser(
        "scan-all",
        help="Scan and print items from both analytics tables",
    )
    scan_all_parser.add_argument("--limit", type=int, default=10)

    args = parser.parse_args(argv)
    dynamodb = build_dynamodb_resource()
    client = build_dynamodb_client()

    if args.command == "list-tables":
        print(json.dumps(client.list_tables()["TableNames"], indent=2))
        return 0

    if args.command == "delete-table":
        delete_table(dynamodb, args.table_name)
        return 0

    if args.command == "delete-all":
        delete_table(dynamodb, get_customer_table_name())
        delete_table(dynamodb, get_order_table_name())
        return 0

    if args.command == "scan-table":
        print(
            json.dumps(
                scan_table_items(dynamodb, args.table_name, limit=args.limit),
                indent=2,
                default=json_default,
            )
        )
        return 0

    if args.command == "scan-all":
        payload = {
            get_customer_table_name(): scan_table_items(
                dynamodb,
                get_customer_table_name(),
                limit=args.limit,
            ),
            get_order_table_name(): scan_table_items(
                dynamodb,
                get_order_table_name(),
                limit=args.limit,
            ),
        }
        print(json.dumps(payload, indent=2, default=json_default))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
