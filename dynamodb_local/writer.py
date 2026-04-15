"""DynamoDB Local table management and write helpers."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from botocore.exceptions import ClientError
from pyspark.sql import DataFrame

from dynamodb_local.tables import (
    CUSTOMER_TABLE_DEFINITION,
    ORDER_TABLE_DEFINITION,
    get_customer_table_name,
    get_order_table_name,
)
from utils.logger import get_logger

logger = get_logger(__name__)


def serialize_dynamodb_value(value: Any) -> Any:
    """Convert Spark row values into DynamoDB-compatible Python values."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, list):
        return [serialize_dynamodb_value(item) for item in value]
    if isinstance(value, dict):
        return {
            key: serialize_dynamodb_value(item)
            for key, item in value.items()
        }
    return value


def ensure_table_exists(dynamodb, table_name: str, table_definition: dict[str, str]):
    """Create a DynamoDB Local table if it does not already exist."""
    existing_tables = set(dynamodb.meta.client.list_tables()["TableNames"])
    if table_name in existing_tables:
        logger.info(f"Reusing DynamoDB Local table → {table_name}")
        return dynamodb.Table(table_name)

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                "AttributeName": table_definition["key_name"],
                "KeyType": "HASH",
            }
        ],
        AttributeDefinitions=[
            {
                "AttributeName": table_definition["key_name"],
                "AttributeType": table_definition["attribute_type"],
            }
        ],
        ProvisionedThroughput={
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5,
        },
    )
    table.wait_until_exists()
    logger.info(f"Created DynamoDB Local table → {table_name}")
    return table


def ensure_analytics_tables_exist(dynamodb) -> dict[str, Any]:
    """Ensure both analytics tables exist in DynamoDB Local."""
    customer_table_name = get_customer_table_name()
    order_table_name = get_order_table_name()
    return {
        "customer_table": ensure_table_exists(
            dynamodb,
            customer_table_name,
            CUSTOMER_TABLE_DEFINITION,
        ),
        "order_table": ensure_table_exists(
            dynamodb,
            order_table_name,
            ORDER_TABLE_DEFINITION,
        ),
    }


def write_spark_dataframe_to_table(df: DataFrame, table) -> int:
    """Write a Spark DataFrame to a DynamoDB Local table via batch writes."""
    written = 0
    with table.batch_writer(overwrite_by_pkeys=[table.key_schema[0]["AttributeName"]]) as batch:
        for row in df.toLocalIterator():
            batch.put_item(
                Item={
                    key: serialize_dynamodb_value(value)
                    for key, value in row.asDict(recursive=True).items()
                }
            )
            written += 1
    logger.info(f"Wrote {written} items to DynamoDB Local table → {table.name}")
    return written


def delete_table(dynamodb, table_name: str) -> bool:
    """Delete a DynamoDB Local table if it exists."""
    try:
        table = dynamodb.Table(table_name)
        table.delete()
        table.wait_until_not_exists()
        logger.info(f"Deleted DynamoDB Local table → {table_name}")
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code == "ResourceNotFoundException":
            logger.info(f"DynamoDB Local table not found → {table_name}")
            return False
        raise


def scan_table_items(dynamodb, table_name: str, limit: int = 10) -> list[dict[str, Any]]:
    """Scan up to *limit* items from a DynamoDB Local table."""
    table = dynamodb.Table(table_name)
    response = table.scan(Limit=limit)
    return response.get("Items", [])
