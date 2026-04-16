"""DynamoDB Local orchestration helpers."""

from __future__ import annotations

import argparse

import pandas as pd
from pyspark.sql import DataFrame

from dynamodb_local.client import build_dynamodb_resource
from dynamodb_local.writer import (
    ensure_analytics_table_exists,
    load_write_status,
    write_combined_dataframes_to_table,
)
from utils.logger import get_logger

logger = get_logger(__name__)


def write_analytics_to_dynamodb(
    customer_df: DataFrame,
    order_df: DataFrame,
) -> dict:
    """Write both Spark analytics DataFrames into a single DynamoDB Local table.

    Returns a summary dict with ``total``, ``written``, and ``failed`` counts
    plus the full ``status_df`` for downstream use.
    """
    dynamodb = build_dynamodb_resource()
    table = ensure_analytics_table_exists(dynamodb)

    status_df = write_combined_dataframes_to_table(customer_df, order_df, table)

    written = int(status_df["written"].sum())
    total = len(status_df)

    return {
        "total": total,
        "written": written,
        "failed": total - written,
        "status_df": status_df,
    }


def main(argv: list[str] | None = None) -> int:
    """DynamoDB Local CLI — view persisted write-status after an ETL run."""
    parser = argparse.ArgumentParser(description="DynamoDB Local helper commands")
    subparsers = parser.add_subparsers(dest="command", required=True)

    view_parser = subparsers.add_parser(
        "view-status",
        help="Print the write-status of the last ETL run",
    )
    view_parser.add_argument(
        "--failed-only",
        action="store_true",
        help="Show only records that were NOT written successfully",
    )
    view_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of rows printed (default: all)",
    )

    args = parser.parse_args(argv)

    if args.command == "view-status":
        df = load_write_status()
        if df is None:
            print("No write-status file found. Run the ETL pipeline first.")
            return 1

        if args.failed_only:
            df = df[~df["written"]]

        if args.limit is not None:
            df = df.head(args.limit)

        total = len(df) if not args.failed_only else None
        written = int(df["written"].sum()) if not args.failed_only else None
        failed = int((~df["written"]).sum()) if not args.failed_only else len(df)

        if not args.failed_only:
            print(f"\nWrite status summary — {written}/{total} records written successfully")
            if failed:
                print(f"  {failed} record(s) failed\n")
            else:
                print()

        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_colwidth", 60)
        print(df.to_string(index=False))
        print()
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())