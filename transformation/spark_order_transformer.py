"""
Order analytics transformer for the Spark/Iceberg path.
"""
from pyspark.sql import DataFrame, functions as F

from utils.logger import get_logger

logger = get_logger(__name__)


def build_order_analytics_spark(enriched_df: DataFrame) -> DataFrame:
    """
    Shape enriched Spark order records into the order analytics table.
    """
    result = (
        enriched_df
        .select(
            "order_id",
            F.col("customer_id").cast("int").alias("customer_id"),
            F.col("order_timestamp").cast("timestamp").alias("order_timestamp"),
            F.col("order_date").cast("date").alias("order_date"),
            F.col("order_hour").cast("int").alias("order_hour"),
            F.round(F.col("gross_amount"), 2).cast("decimal(14,2)").alias("gross_amount"),
            F.round(F.col("total_discount_amount"), 2).cast("decimal(14,2)").alias("total_discount_amount"),
            F.round(F.col("net_amount"), 2).cast("decimal(14,2)").alias("net_amount"),
            F.round(F.col("shipping_cost"), 2).cast("decimal(10,2)").alias("shipping_cost"),
            F.round(F.col("final_amount"), 2).cast("decimal(14,2)").alias("final_amount"),
            F.col("total_items").cast("int").alias("total_items"),
            F.round(F.col("discount_ratio"), 4).cast("decimal(6,4)").alias("discount_ratio"),
            F.col("order_complexity_score").cast("int").alias("order_complexity_score"),
            "dominant_category",
            "payment_method",
            "shipping_provider",
        )
    )

    logger.info(f"[SPARK] order_analytics → {result.count()} rows")
    return result
