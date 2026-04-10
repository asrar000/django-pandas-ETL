"""OpenSearch serialization helpers."""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from pyspark.sql import SparkSession


def build_json_spark_session() -> SparkSession:
    """Build a temporary local Spark session for DataFrame JSON serialization."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    spark = (
        SparkSession.builder
        .appName("opensearch_json_serializer")
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def serialize_value(value: Any) -> Any:
    """Convert Pandas- and Python-specific values into JSON-safe primitives."""
    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        value = value.to_pydatetime()
    elif hasattr(value, "item") and not isinstance(value, (str, bytes, bytearray)):
        try:
            value = value.item()
        except Exception:
            pass

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)

    return value


def records_from_dataframe(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert a DataFrame into serialized record dictionaries for indexing."""
    if df.empty:
        return []

    normalized_records = [
        {
            field: serialize_value(value)
            for field, value in record.items()
        }
        for record in df.to_dict(orient="records")
    ]

    spark = build_json_spark_session()
    try:
        spark_df = spark.createDataFrame(normalized_records)
        return [json.loads(record) for record in spark_df.toJSON().collect()]
    finally:
        spark.stop()
