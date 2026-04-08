"""
Management command: etl_postgres
Runs the full ETL workflow, keeping the existing Pandas → PostgreSQL branch and
adding a parallel PySpark → Iceberg branch from the same extracted seed data.

Usage:
    python manage.py etl_postgres
"""
from concurrent.futures import ThreadPoolExecutor

from django.core.management.base import BaseCommand, CommandError

from config.loader import get
from extractor.api_extractor import extract_all
from processing.enrichment import enrich_all
from processing.spark_enrichment import enrich_all_spark
from processing.spark_synthesizer import synthesize_orders_spark
from processing.synthesizer import synthesize_orders
from services.db_service import load_customer_analytics, load_order_analytics
from services.iceberg_service import write_analytics_to_iceberg
from services.spark_service import build_spark_session
from transformation.customer_transformer import build_customer_analytics
from transformation.order_transformer import build_order_analytics
from transformation.spark_customer_transformer import build_customer_analytics_spark
from transformation.spark_order_transformer import build_order_analytics_spark
from utils.logger import get_logger
from utils.paths import ensure_dirs
from utils.writer import write_processed_csv, write_raw_json

logger = get_logger("pipeline.management.commands.etl_postgres")

_DIVIDER = "═" * 62


def _run_pandas_postgres_branch(raw_carts: list, raw_users: list) -> dict:
    """
    Existing Pandas analytics flow used for the processed CSV outputs and the
    PostgreSQL ORM upsert path.
    """
    num_records = int(get("processing.num_synthetic_records", 10_000))

    logger.info(f"[PANDAS] Synthesising {num_records} generic orders…")
    carts, users = synthesize_orders(raw_carts, raw_users, num_records=num_records)
    write_raw_json(carts, "synthesised_carts.json")
    write_raw_json(users, "synthesised_users.json")

    logger.info("[PANDAS] Enriching orders…")
    enriched = enrich_all(carts, users)

    logger.info("[PANDAS] Building analytics tables…")
    customer_df = build_customer_analytics(enriched)
    order_df = build_order_analytics(enriched)

    logger.info("[PANDAS] Writing processed CSV output…")
    write_processed_csv(customer_df, "customer_analytics.csv")
    write_processed_csv(order_df, "order_analytics.csv")

    logger.info("[PANDAS] Loading analytics into PostgreSQL via Django ORM…")
    customer_created, customer_updated = load_customer_analytics(customer_df)
    order_created, order_updated = load_order_analytics(order_df)

    return {
        "customer_rows": len(customer_df),
        "order_rows": len(order_df),
        "customer_created": customer_created,
        "customer_updated": customer_updated,
        "order_created": order_created,
        "order_updated": order_updated,
    }


def _run_spark_iceberg_branch(raw_carts: list, raw_users: list) -> dict:
    """
    Spark analytics flow that mirrors the Pandas business logic and writes the
    resulting analytics tables to Iceberg.
    """
    num_records = int(get("processing.num_synthetic_records", 10_000))
    spark = build_spark_session()

    try:
        logger.info(f"[SPARK] Synthesising {num_records} generic orders…")
        carts_df, users_df = synthesize_orders_spark(
            spark,
            raw_carts,
            raw_users,
            num_records=num_records,
        )

        logger.info("[SPARK] Enriching orders…")
        enriched_df = enrich_all_spark(carts_df, users_df)

        logger.info("[SPARK] Building analytics tables…")
        customer_df = build_customer_analytics_spark(enriched_df).cache()
        order_df = build_order_analytics_spark(enriched_df).cache()

        customer_rows = customer_df.count()
        order_rows = order_df.count()

        logger.info("[SPARK] Writing analytics to Iceberg…")
        write_analytics_to_iceberg(customer_df, order_df)

        customer_df.unpersist()
        order_df.unpersist()

        return {
            "customer_rows": customer_rows,
            "order_rows": order_rows,
        }
    finally:
        spark.stop()


class Command(BaseCommand):
    help = (
        "Run the full ETL pipeline with two analytics branches: Pandas to "
        "processed CSVs and PostgreSQL, and PySpark to Iceberg tables."
    )

    def handle(self, *args, **options) -> None:
        logger.info(_DIVIDER)
        logger.info("      Generic E-Commerce ETL Pipeline  —  Full Run")
        logger.info(_DIVIDER)

        # ── 0. Setup directories ─────────────────────────────────────────────
        ensure_dirs()

        # ── 1. Extract ───────────────────────────────────────────────────────
        logger.info("[STEP 1/2]  Extracting raw data from API…")
        try:
            raw_carts, raw_users = extract_all()
        except Exception as exc:
            logger.warning(f"API extraction failed ({exc}). Continuing with empty seed.")
            raw_carts, raw_users = [], []

        write_raw_json(raw_carts, "raw_carts.json")
        write_raw_json(raw_users, "raw_users.json")

        # ── 2. Dual analytics branches ──────────────────────────────────────
        logger.info(
            "[STEP 2/2]  Running Pandas→PostgreSQL and PySpark→Iceberg branches…"
        )

        pandas_result = None
        spark_result = None
        pandas_error = None
        spark_error = None

        with ThreadPoolExecutor(max_workers=1) as executor:
            pandas_future = executor.submit(
                _run_pandas_postgres_branch,
                raw_carts,
                raw_users,
            )

            try:
                spark_result = _run_spark_iceberg_branch(raw_carts, raw_users)
            except Exception as exc:
                spark_error = exc
                logger.exception("PySpark → Iceberg branch failed")

            try:
                pandas_result = pandas_future.result()
            except Exception as exc:
                pandas_error = exc
                logger.exception("Pandas → PostgreSQL branch failed")

        if pandas_error or spark_error:
            messages = []
            if pandas_error:
                messages.append(f"Pandas/PostgreSQL branch failed: {pandas_error}")
            if spark_error:
                messages.append(f"PySpark/Iceberg branch failed: {spark_error}")
            raise CommandError(" | ".join(messages))

        logger.info(_DIVIDER)
        logger.info(
            "  ✓ PostgreSQL branch  →  "
            f"{pandas_result['customer_rows']:>5} customer rows | "
            f"{pandas_result['order_rows']:>5} order rows"
        )
        logger.info(
            "    ORM upserts        →  "
            f"customers {pandas_result['customer_created']} created / {pandas_result['customer_updated']} updated | "
            f"orders {pandas_result['order_created']} created / {pandas_result['order_updated']} updated"
        )
        logger.info(
            "  ✓ Iceberg branch     →  "
            f"{spark_result['customer_rows']:>5} customer rows | "
            f"{spark_result['order_rows']:>5} order rows"
        )
        logger.info(_DIVIDER)
        logger.info("  Full ETL complete across PostgreSQL and Iceberg.")
        logger.info(_DIVIDER)
