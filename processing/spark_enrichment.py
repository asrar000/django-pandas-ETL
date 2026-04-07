"""
PySpark enrichment logic mirroring the Pandas enrichment path.
"""
from pyspark.sql import DataFrame, Window, functions as F

from utils.logger import get_logger

logger = get_logger(__name__)


def enrich_all_spark(carts_df: DataFrame, users_df: DataFrame) -> DataFrame:
    """
    Join carts and users in Spark and derive the same analytics-ready fields used
    by the Pandas branch.
    """
    joined = carts_df.alias("c").join(
        users_df.alias("u"),
        F.col("c.userId") == F.col("u.id"),
        "inner",
    )

    email_clean = F.lower(F.trim(F.col("u.email")))
    signup_ts = F.coalesce(
        F.to_timestamp(F.regexp_replace(F.col("u.createdAt"), "Z$", "+00:00")),
        F.current_timestamp(),
    )
    order_ts = F.coalesce(
        F.to_timestamp(F.regexp_replace(F.col("c.orderTimestamp"), "Z$", "+00:00")),
        F.current_timestamp(),
    )

    enriched = (
        joined
        .withColumn(
            "gross_amount",
            F.round(
                F.expr(
                    "aggregate(c.products, CAST(0.0 AS DOUBLE), "
                    "(acc, x) -> acc + (x.price * x.quantity))"
                ),
                2,
            ),
        )
        .withColumn(
            "total_discount_amount",
            F.round(
                F.expr(
                    "aggregate(c.products, CAST(0.0 AS DOUBLE), "
                    "(acc, x) -> acc + (x.price * x.quantity * (x.discountPercentage / 100.0)))"
                ),
                2,
            ),
        )
        .withColumn(
            "total_items",
            F.expr("aggregate(c.products, 0, (acc, x) -> acc + x.quantity)"),
        )
        .withColumn(
            "unique_product_count",
            F.expr("size(array_distinct(transform(c.products, x -> x.id)))"),
        )
        .withColumn("shipping_cost", F.round(F.col("c.shippingCost"), 2))
        .withColumn(
            "net_amount",
            F.round(F.col("gross_amount") - F.col("total_discount_amount"), 2),
        )
        .withColumn(
            "final_amount",
            F.round(F.col("net_amount") + F.col("shipping_cost"), 2),
        )
        .withColumn(
            "discount_ratio",
            F.when(
                F.col("gross_amount") > 0,
                F.round(F.col("total_discount_amount") / F.col("gross_amount"), 4),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "order_complexity_score",
            (F.col("unique_product_count") * F.lit(2) + F.col("total_items")).cast("int"),
        )
        .select(
            F.col("c.orderId").alias("order_id"),
            F.col("u.id").alias("customer_id"),
            F.col("u.firstName").alias("first_name"),
            F.col("u.lastName").alias("last_name"),
            email_clean.alias("email"),
            F.when(
                F.instr(email_clean, "@") > 0,
                F.element_at(F.split(email_clean, "@"), -1),
            ).otherwise(F.lit("")).alias("email_domain"),
            F.coalesce(F.col("u.address.city"), F.lit("Unknown")).alias("city"),
            F.to_date(signup_ts).alias("signup_date"),
            order_ts.alias("order_timestamp"),
            F.to_date(order_ts).alias("order_date"),
            F.hour(order_ts).alias("order_hour"),
            F.col("c.products").alias("products"),
            F.col("c.paymentMethod").alias("payment_method"),
            F.col("c.shippingProvider").alias("shipping_provider"),
            "total_items",
            "gross_amount",
            "total_discount_amount",
            "net_amount",
            "shipping_cost",
            "final_amount",
            "discount_ratio",
            "order_complexity_score",
        )
    )

    category_totals = (
        enriched
        .select("order_id", F.explode_outer("products").alias("product"))
        .select(
            "order_id",
            F.when(
                F.trim(F.col("product.category")) != "",
                F.col("product.category"),
            ).otherwise(F.lit("Unknown")).alias("category"),
            (F.col("product.price") * F.col("product.quantity")).alias("category_amount"),
        )
        .groupBy("order_id", "category")
        .agg(F.sum("category_amount").alias("category_amount"))
    )

    dominant_window = Window.partitionBy("order_id").orderBy(
        F.desc("category_amount"),
        F.asc("category"),
    )
    dominant_category = (
        category_totals
        .withColumn("row_num", F.row_number().over(dominant_window))
        .filter(F.col("row_num") == 1)
        .select("order_id", F.col("category").alias("dominant_category"))
    )

    result = (
        enriched
        .join(dominant_category, "order_id", "left")
        .withColumn(
            "dominant_category",
            F.coalesce(F.col("dominant_category"), F.lit("Unknown")),
        )
        .drop("products")
        .drop("unique_product_count")
    )

    logger.info(f"[SPARK] Enrichment complete — {result.count()} orders enriched")
    return result
