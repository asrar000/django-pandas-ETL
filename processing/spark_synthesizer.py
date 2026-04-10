"""
PySpark synthesizer that mirrors the Pandas synthesis logic for the Iceberg path.
"""
import random
import uuid
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from processing.synthesizer import (
    _CITIES,
    _COURIERS,
    _EMAIL_DOMAINS,
    _FIRST_NAMES,
    _LAST_NAMES,
    _PAYMENT_METHODS,
    _PRODUCTS,
)
from utils.logger import get_logger

logger = get_logger(__name__)

_PRODUCT_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("title", T.StringType(), False),
    T.StructField("category", T.StringType(), False),
    T.StructField("price", T.DoubleType(), False),
    T.StructField("quantity", T.IntegerType(), False),
    T.StructField("discountPercentage", T.DoubleType(), False),
])

_USER_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("firstName", T.StringType(), False),
    T.StructField("lastName", T.StringType(), False),
    T.StructField("email", T.StringType(), False),
    T.StructField(
        "address",
        T.StructType([T.StructField("city", T.StringType(), False)]),
        False,
    ),
    T.StructField("createdAt", T.StringType(), False),
])

_CART_SCHEMA = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("userId", T.IntegerType(), False),
    T.StructField("orderId", T.StringType(), False),
    T.StructField("orderTimestamp", T.StringType(), False),
    T.StructField("products", T.ArrayType(_PRODUCT_SCHEMA), False),
    T.StructField("paymentMethod", T.StringType(), False),
    T.StructField("shippingCost", T.DoubleType(), False),
    T.StructField("shippingProvider", T.StringType(), False),
])


def _random_dt(seed: int, days_ago_min: int, days_ago_max: int) -> datetime:
    """Return a deterministic pseudo-random datetime derived from *seed*."""
    rng = random.Random(seed)
    offset = rng.randint(days_ago_min, days_ago_max)
    base = datetime.now() - timedelta(days=offset)
    return base.replace(
        hour=rng.randint(0, 23),
        minute=rng.randint(0, 59),
        second=rng.randint(0, 59),
        microsecond=0,
    )


def synthesize_orders_spark(
    spark: SparkSession,
    real_carts: list,
    real_users: list,
    num_records: int = 10_000,
) -> tuple[DataFrame, DataFrame]:
    """
    Synthesise Spark DataFrames for carts and users using the same business rules
    as the Pandas pipeline.
    """
    logger.info(
        f"[SPARK] Synthesising {num_records} generic e-commerce orders  "
        f"(API seed: {len(real_carts)} carts, {len(real_users)} users)"
    )

    real_user_pool = {int(user["id"]): user for user in real_users}
    real_user_ids = list(real_user_pool.keys())
    pool_size = len(real_user_ids)

    @F.udf(returnType=T.IntegerType())
    def mapped_user_id(cart_id: int) -> int:
        """Map each generated cart ID to the corresponding user ID."""
        if pool_size == 0:
            return int(cart_id)
        return int(real_user_ids[(cart_id - 1) % pool_size])

    @F.udf(returnType=_USER_SCHEMA)
    def build_user(user_id: int):
        """Construct a Spark user struct from seeded or synthetic values."""
        if pool_size > 0 and user_id in real_user_pool:
            real_user = real_user_pool[user_id]
            created_at = real_user.get("createdAt")
            if not created_at:
                created_at = _random_dt(user_id * 17, 30, 1_460).isoformat()
            return {
                "id": int(user_id),
                "firstName": real_user.get(
                    "firstName",
                    random.Random(user_id).choice(_FIRST_NAMES),
                ),
                "lastName": real_user.get(
                    "lastName",
                    random.Random(user_id + 1).choice(_LAST_NAMES),
                ),
                "email": real_user.get("email", f"user{user_id}@gmail.com"),
                "address": {
                    "city": random.Random(user_id * 19).choice(_CITIES),
                },
                "createdAt": created_at,
            }

        rng = random.Random(user_id)
        first = rng.choice(_FIRST_NAMES)
        last = rng.choice(_LAST_NAMES)
        domain = rng.choice(_EMAIL_DOMAINS)
        signup = _random_dt(user_id * 31, 30, 1_460).isoformat()
        return {
            "id": int(user_id),
            "firstName": first,
            "lastName": last,
            "email": f"{first.lower()}.{last.lower()}{rng.randint(1, 999)}@{domain}",
            "address": {"city": rng.choice(_CITIES)},
            "createdAt": signup,
        }

    @F.udf(returnType=_CART_SCHEMA)
    def build_cart(cart_id: int, user_id: int):
        """Construct a Spark cart struct with deterministic synthetic content."""
        rng = random.Random(cart_id * 1009 + user_id)
        n_cats = rng.randint(1, min(4, len(_PRODUCTS)))
        chosen_cats = rng.sample(list(_PRODUCTS.keys()), n_cats)

        products = []
        for category in chosen_cats:
            name, lo, hi = rng.choice(_PRODUCTS[category])
            price = round(rng.uniform(lo, hi), 2)
            quantity = rng.randint(1, 5)
            discount = round(rng.uniform(0.0, 0.25), 2)
            products.append({
                "id": rng.randint(1_000, 9_999),
                "title": name,
                "category": category,
                "price": price,
                "quantity": quantity,
                "discountPercentage": round(discount * 100, 2),
            })

        courier, ship_lo, ship_hi = rng.choice(_COURIERS)
        order_dt = _random_dt(cart_id * 47, 0, 365)
        return {
            "id": int(cart_id),
            "userId": int(user_id),
            "orderId": str(uuid.uuid4()),
            "orderTimestamp": order_dt.isoformat(),
            "products": products,
            "paymentMethod": rng.choice(_PAYMENT_METHODS),
            "shippingCost": round(rng.uniform(ship_lo, ship_hi), 2),
            "shippingProvider": courier,
        }

    orders_base = spark.range(1, num_records + 1).select(
        F.col("id").cast("int").alias("cart_id")
    )
    if pool_size > 0:
        orders_base = orders_base.withColumn("userId", mapped_user_id("cart_id"))
        user_ids_df = spark.createDataFrame([(uid,) for uid in real_user_ids], ["id"])
    else:
        orders_base = orders_base.withColumn("userId", F.col("cart_id"))
        user_ids_df = spark.range(1, num_records + 1).select(
            F.col("id").cast("int").alias("id")
        )

    carts_df = (
        orders_base
        .withColumn("cart", build_cart("cart_id", "userId"))
        .select("cart.*")
    )
    users_df = (
        user_ids_df
        .withColumn("user", build_user("id"))
        .select("user.*")
    )

    logger.info(
        f"[SPARK] Synthesis complete — {carts_df.count()} orders | "
        f"{users_df.count()} unique customers"
    )
    return carts_df, users_df
