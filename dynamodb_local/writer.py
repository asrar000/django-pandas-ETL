"""DynamoDB Local table management and combined write helpers."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any,Iterable, Tuple

import pandas as pd
from pyspark.sql import DataFrame

from config.loader import get
from dynamodb_local.tables import ANALYTICS_TABLE_DEFINITION, get_analytics_table_name
from utils.logger import get_logger
from utils.paths import PROCESSED_DIR

logger = get_logger(__name__)

# Write-status CSV is persisted here so the CLI can read it after the run.
_STATUS_PATH: Path = PROCESSED_DIR / "dynamodb_write_status.csv"


def serialize_dynamodb_value(value: Any) -> Any:
    """Convert Spark row values into DynamoDB-compatible Python values."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        # str() first — avoids carrying IEEE-754 imprecision into Decimal
        return Decimal(str(value))
    if isinstance(value, list):
        return [serialize_dynamodb_value(item) for item in value]
    if isinstance(value, dict):
        return {k: serialize_dynamodb_value(v) for k, v in value.items()}
    return value


def collect_items_from_df(
    df: DataFrame,
    id_field: str,
    prefix: str,
    record_type: str,
) -> Iterable[Tuple[str, dict]]:
    """Yield (record_id, item) for every row in df."""
    for row in df.toLocalIterator():
        d = row.asDict(recursive=True)
        record_id = f"{prefix}#{d.get(id_field, '')}"
        item = {
            "record_id": record_id,
            "record_type": record_type,
        }
        item.update({k: serialize_dynamodb_value(v) for k, v in d.items()})
        yield (record_id, item)


def ensure_analytics_table_exists(dynamodb):
    """Create the combined analytics table if it does not already exist."""
    table_name = get_analytics_table_name()
    key_name = ANALYTICS_TABLE_DEFINITION["key_name"]
    attr_type = ANALYTICS_TABLE_DEFINITION["attribute_type"]

    existing = set(dynamodb.meta.client.list_tables()["TableNames"])
    if table_name in existing:
        logger.info(f"Reusing DynamoDB Local table → {table_name}")
        return dynamodb.Table(table_name)

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": key_name, "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": key_name, "AttributeType": attr_type}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    table.wait_until_exists()
    logger.info(f"Created DynamoDB Local table → {table_name}")
    return table


def write_combined_dataframes_to_table(
    customer_df: DataFrame,
    order_df: DataFrame,
    table,
) -> pd.DataFrame:
    """Write both Spark DataFrames into a single DynamoDB table.

    Records are prefixed so their origin is unambiguous:
      customer rows  →  record_id = "customer#<customer_id>"
      order rows     →  record_id = "order#<order_id>"

    Returns a pandas DataFrame with columns (record_id, written) where
    ``written`` is True for every record that was successfully persisted
    and False for any that failed.
    """
    # ── 1. Materialise all items on the driver ────────────────────────────
    all_items: list[tuple[str, dict]] = []

    # collect customers
    all_items.extend(collect_items_from_df(
        customer_df, id_field="customer_id", prefix="customer", record_type="customer"
    ))

    # collect orders
    all_items.extend(collect_items_from_df(
        order_df, id_field="order_id", prefix="order", record_type="order"
    ))

    failed_ids: set[str] = set()
    try:
        with table.batch_writer(overwrite_by_pkeys=["record_id"]) as batch:
            for _rid, item in all_items:
                batch.put_item(Item=item)
    except Exception as exc:
        logger.error(f"Batch write failed: {exc}")
        failed_ids = {rid for rid, _ in all_items}

    written_count = len(all_items) - len(failed_ids)
    logger.info(
        f"Wrote {written_count}/{len(all_items)} items "
        f"→ DynamoDB Local table '{table.name}'"
    )

    # ── 3. Build and persist write-status DataFrame ───────────────────────
    status_df = pd.DataFrame(
        [
            {"record_id": rid, "written": rid not in failed_ids}
            for rid, _ in all_items
        ],
        columns=["record_id", "written"],
    )

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    status_df.to_csv(_STATUS_PATH, index=False)
    logger.info(f"Write status saved → {_STATUS_PATH}")

    return status_df


def load_write_status() -> pd.DataFrame | None:
    """Load the persisted write-status CSV, or return None if it does not exist."""
    if not _STATUS_PATH.exists():
        return None
    return pd.read_csv(_STATUS_PATH)