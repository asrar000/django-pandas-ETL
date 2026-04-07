"""
Customer analytics transformer for the Spark/Iceberg path.
"""
from pyspark.sql import DataFrame, functions as F

from utils.logger import get_logger

logger = get_logger(__name__)


def build_customer_analytics_spark(enriched_df: DataFrame) -> DataFrame:
    """
    Aggregate enriched Spark order records into the customer analytics table.
    """
    ltv_score = (
        F.least(F.col("total_spent") / F.lit(10_000.0), F.lit(50.0))
        + F.least(F.col("total_orders") * F.lit(2.0), F.lit(30.0))
        + F.least((F.col("customer_tenure_days") / F.lit(365.0)) * F.lit(10.0), F.lit(20.0))
    )

    result = (
        enriched_df
        .groupBy("customer_id")
        .agg(
            F.first("first_name").alias("first_name"),
            F.first("last_name").alias("last_name"),
            F.first("email").alias("email"),
            F.first("email_domain").alias("email_domain"),
            F.first("city").alias("city"),
            F.first("signup_date").alias("signup_date"),
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("final_amount"), 2).alias("total_spent"),
        )
        .withColumn(
            "full_name",
            F.concat_ws(" ", F.trim(F.col("first_name")), F.trim(F.col("last_name"))),
        )
        .withColumn(
            "customer_tenure_days",
            F.greatest(F.datediff(F.current_date(), F.col("signup_date")), F.lit(0)),
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_spent") / F.col("total_orders"), 2),
        )
        .withColumn("lifetime_value_score", F.round(ltv_score, 2))
        .withColumn(
            "customer_segment",
            F.when(F.col("lifetime_value_score") >= 60, F.lit("High"))
            .when(F.col("lifetime_value_score") >= 30, F.lit("Medium"))
            .otherwise(F.lit("Low")),
        )
        .select(
            F.col("customer_id").cast("int").alias("customer_id"),
            "full_name",
            "email",
            "email_domain",
            "city",
            F.col("customer_tenure_days").cast("int").alias("customer_tenure_days"),
            F.col("total_orders").cast("int").alias("total_orders"),
            F.round(F.col("total_spent"), 2).cast("decimal(14,2)").alias("total_spent"),
            F.round(F.col("avg_order_value"), 2).cast("decimal(14,2)").alias("avg_order_value"),
            F.round(F.col("lifetime_value_score"), 2).cast("decimal(8,2)").alias("lifetime_value_score"),
            "customer_segment",
        )
    )

    logger.info(f"[SPARK] customer_analytics → {result.count()} rows")
    return result
