"""
Management command: etl
Runs the full ETL workflow, keeping the existing Pandas branch for CSV,
PostgreSQL, and OpenSearch, plus the PySpark → Iceberg branch from the same
extracted seed data.

Usage:
    python manage.py etl                    # Run both branches
    python manage.py etl --pandas           # Run only Pandas → PostgreSQL/OpenSearch branch
    python manage.py etl --pyspark          # Run only PySpark → Iceberg branch
"""

from django.core.management.base import BaseCommand, CommandError

from config.loader import get
from extractor.api_extractor import extract_all
from processing.enrichment import enrich_all
from processing.spark_enrichment import enrich_all_spark
from processing.spark_synthesizer import synthesize_orders_spark
from processing.synthesizer import synthesize_orders
from services.db_service import load_customer_analytics, load_order_analytics
from services.iceberg_service import write_analytics_to_iceberg
from services.opensearch_service import index_analytics_to_opensearch
from services.spark_service import build_spark_session
from transformation.customer_transformer import build_customer_analytics
from transformation.order_transformer import build_order_analytics
from transformation.spark_customer_transformer import build_customer_analytics_spark
from transformation.spark_order_transformer import build_order_analytics_spark
from utils.logger import get_logger
from utils.paths import ensure_dirs
from utils.writer import write_processed_csv, write_raw_json

logger = get_logger("pipeline.management.commands.etl")

_DIVIDER = "═" * 62


def _run_pandas_postgres_branch(raw_carts: list, raw_users: list) -> dict:
    """
    Pandas analytics flow used for the processed CSV outputs, PostgreSQL ORM
    upserts, and OpenSearch indexing path.
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

    logger.info("[PANDAS] Indexing analytics into OpenSearch…")
    opensearch_result = index_analytics_to_opensearch(customer_df, order_df)

    return {
        "customer_rows": len(customer_df),
        "order_rows": len(order_df),
        "customer_created": customer_created,
        "customer_updated": customer_updated,
        "order_created": order_created,
        "order_updated": order_updated,
        "customer_docs": opensearch_result["customer_docs"],
        "order_docs": opensearch_result["order_docs"],
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
        "processed CSVs, PostgreSQL, and OpenSearch, plus PySpark to "
        "Iceberg tables."
    )

    def add_arguments(self, parser):
        """Register CLI flags that select which analytics branches to run."""
        parser.add_argument(
            '--pandas',
            action='store_true',
            help='Run only the Pandas → PostgreSQL/OpenSearch branch',
        )
        parser.add_argument(
            '--pyspark',
            action='store_true',
            help='Run only the PySpark → Iceberg branch',
        )

    def handle(self, *args, **options) -> None:
        """Execute extraction and run the requested Pandas and/or Spark branches."""
        run_pandas = options['pandas'] or not (options['pandas'] or options['pyspark'])
        run_pyspark = options['pyspark'] or not (options['pandas'] or options['pyspark'])

        if run_pandas and run_pyspark:
            logger.info(_DIVIDER)
            logger.info("      Generic E-Commerce ETL Pipeline  —  Full Run")
            logger.info(_DIVIDER)
        elif run_pandas:
            logger.info(_DIVIDER)
            logger.info("      Generic E-Commerce ETL Pipeline  —  Pandas Branch Only")
            logger.info(_DIVIDER)
        elif run_pyspark:
            logger.info(_DIVIDER)
            logger.info("      Generic E-Commerce ETL Pipeline  —  PySpark Branch Only")
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

        # ── 2. Analytics branches ───────────────────────────────────────────
        if run_pandas and run_pyspark:
            logger.info(
                "[STEP 2/2]  Running Pandas→PostgreSQL/OpenSearch and PySpark→Iceberg branches…"
            )
        elif run_pandas:
            logger.info("[STEP 1/1]  Running Pandas→PostgreSQL/OpenSearch branch…")
        elif run_pyspark:
            logger.info("[STEP 1/1]  Running PySpark→Iceberg branch…")

        pandas_result = None
        spark_result = None
        pandas_error = None
        spark_error = None

        if run_pandas:
            try:
                pandas_result = _run_pandas_postgres_branch(raw_carts, raw_users)
            except Exception as exc:
                pandas_error = exc
                logger.exception("Pandas → PostgreSQL/OpenSearch branch failed")

        if run_pyspark:
            try:
                spark_result = _run_spark_iceberg_branch(raw_carts, raw_users)
            except Exception as exc:
                spark_error = exc
                logger.exception("PySpark → Iceberg branch failed")

        if pandas_error or spark_error:
            messages = []
            if pandas_error:
                messages.append(
                    f"Pandas/PostgreSQL/OpenSearch branch failed: {pandas_error}"
                )
            if spark_error:
                messages.append(f"PySpark/Iceberg branch failed: {spark_error}")
            raise CommandError(" | ".join(messages))

        logger.info(_DIVIDER)
        if pandas_result:
            logger.info(
                "  ✓ PostgreSQL/OpenSearch branch  →  "
                f"{pandas_result['customer_rows']:>5} customer rows | "
                f"{pandas_result['order_rows']:>5} order rows"
            )
            logger.info(
                "    ORM upserts        →  "
                f"customers {pandas_result['customer_created']} created / {pandas_result['customer_updated']} updated | "
                f"orders {pandas_result['order_created']} created / {pandas_result['order_updated']} updated"
            )
            logger.info(
                "    OpenSearch        →  "
                f"customers {pandas_result['customer_docs']} docs | "
                f"orders {pandas_result['order_docs']} docs"
            )
        if spark_result:
            logger.info(
                "  ✓ Iceberg branch     →  "
                f"{spark_result['customer_rows']:>5} customer rows | "
                f"{spark_result['order_rows']:>5} order rows"
            )
        logger.info(_DIVIDER)
        if run_pandas and run_pyspark:
            logger.info("  Full ETL complete across PostgreSQL, OpenSearch, and Iceberg.")
        elif run_pandas:
            logger.info("  Pandas ETL complete across PostgreSQL and OpenSearch.")
        elif run_pyspark:
            logger.info("  PySpark ETL complete.")
        logger.info(_DIVIDER)
