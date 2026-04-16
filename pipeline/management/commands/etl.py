"""
Management command: etl
Runs the full ETL workflow, keeping the existing Pandas branch for CSV,
PostgreSQL, and OpenSearch, plus the PySpark analytics branch from the same
extracted seed data, with Iceberg and DynamoDB Local writes on the Spark side.

Usage:
    python manage.py etl                        # Run both branches
    python manage.py etl --pandas               # Run only Pandas → PostgreSQL/OpenSearch branch
    python manage.py etl --pyspark              # Run only PySpark → Iceberg/DynamoDB Local branch
    python manage.py etl --view-dynamodb-status # Print last DynamoDB write-status (no ETL run)
    python manage.py etl --view-dynamodb-status --failed-only
    python manage.py etl --view-dynamodb-status --limit 20
"""

from django.core.management.base import BaseCommand, CommandError

from config.loader import get
from dynamodb_local.runner import write_analytics_to_dynamodb
from dynamodb_local.writer import load_write_status
from extractor.api_extractor import extract_all
from opensearch.runner import opensearch_write
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

import pandas as pd

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
    opensearch_result = opensearch_write(customer_df, order_df)

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
    resulting analytics tables to Iceberg and DynamoDB Local (single table).
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

        logger.info("[SPARK] Writing analytics to DynamoDB Local…")
        dynamodb_result = write_analytics_to_dynamodb(customer_df, order_df)

        customer_df.unpersist()
        order_df.unpersist()

        return {
            "customer_rows": customer_rows,
            "order_rows": order_rows,
            "dynamodb_total": dynamodb_result["total"],
            "dynamodb_written": dynamodb_result["written"],
            "dynamodb_failed": dynamodb_result["failed"],
        }
    finally:
        spark.stop()


def _print_write_status(failed_only: bool = False, limit: int | None = None) -> None:
    """Load and print the persisted DynamoDB write-status CSV."""
    df = load_write_status()
    if df is None:
        print("No write-status file found. Run the ETL pipeline first.")
        return

    if failed_only:
        df = df[~df["written"]]
        print(f"\nFailed records: {len(df)}\n")
    else:
        total = len(df)
        written = int(df["written"].sum())
        failed = total - written
        print(f"\nDynamoDB write status — {written}/{total} records written successfully")
        if failed:
            print(f"  {failed} record(s) failed")
        print()

    if limit is not None:
        df = df.head(limit)

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_colwidth", 60)
    print(df.to_string(index=False))
    print()


class Command(BaseCommand):
    help = (
        "Run the full ETL pipeline (Pandas → PostgreSQL/OpenSearch and "
        "PySpark → Iceberg/DynamoDB Local), or inspect the last DynamoDB "
        "write-status without running the pipeline."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--pandas",
            action="store_true",
            help="Run only the Pandas → PostgreSQL/OpenSearch branch",
        )
        parser.add_argument(
            "--pyspark",
            action="store_true",
            help="Run only the PySpark → Iceberg/DynamoDB Local branch",
        )
        parser.add_argument(
            "--view-dynamodb-status",
            action="store_true",
            help="Print the DynamoDB write-status from the last run (no ETL run)",
        )
        parser.add_argument(
            "--failed-only",
            action="store_true",
            help="Used with --view-dynamodb-status: show only failed records",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Used with --view-dynamodb-status: limit rows printed",
        )

    def handle(self, *args, **options) -> None:
        # ── Status view shortcut — no ETL run ────────────────────────────
        if options["view_dynamodb_status"]:
            _print_write_status(
                failed_only=options["failed_only"],
                limit=options["limit"],
            )
            return

        run_pandas = options["pandas"] or not (options["pandas"] or options["pyspark"])
        run_pyspark = options["pyspark"] or not (options["pandas"] or options["pyspark"])

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

        # ── 0. Setup directories ─────────────────────────────────────────
        ensure_dirs()

        # ── 1. Extract ───────────────────────────────────────────────────
        logger.info("[STEP 1/2]  Extracting raw data from API…")
        try:
            raw_carts, raw_users = extract_all()
        except Exception as exc:
            logger.warning(f"API extraction failed ({exc}). Continuing with empty seed.")
            raw_carts, raw_users = [], []

        write_raw_json(raw_carts, "raw_carts.json")
        write_raw_json(raw_users, "raw_users.json")

        # ── 2. Analytics branches ────────────────────────────────────────
        if run_pandas and run_pyspark:
            logger.info(
                "[STEP 2/2]  Running Pandas→PostgreSQL/OpenSearch and "
                "PySpark→Iceberg/DynamoDB Local branches…"
            )
        elif run_pandas:
            logger.info("[STEP 1/1]  Running Pandas→PostgreSQL/OpenSearch branch…")
        elif run_pyspark:
            logger.info("[STEP 1/1]  Running PySpark→Iceberg/DynamoDB Local branch…")

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
                logger.exception("PySpark → Iceberg/DynamoDB Local branch failed")

        if pandas_error or spark_error:
            messages = []
            if pandas_error:
                messages.append(f"Pandas/PostgreSQL/OpenSearch branch failed: {pandas_error}")
            if spark_error:
                messages.append(f"PySpark/Iceberg/DynamoDB Local branch failed: {spark_error}")
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
                f"customers {pandas_result['customer_created']} created / "
                f"{pandas_result['customer_updated']} updated | "
                f"orders {pandas_result['order_created']} created / "
                f"{pandas_result['order_updated']} updated"
            )
            logger.info(
                "    OpenSearch        →  "
                f"customers {pandas_result['customer_docs']} docs | "
                f"orders {pandas_result['order_docs']} docs"
            )
        if spark_result:
            logger.info(
                "  ✓ Spark analytics    →  "
                f"{spark_result['customer_rows']:>5} customer rows | "
                f"{spark_result['order_rows']:>5} order rows"
            )
            logger.info(
                "    Iceberg           →  "
                "local.analytics.customer_analytics | "
                "local.analytics.order_analytics"
            )
            logger.info(
                "    DynamoDB Local    →  "
                f"{spark_result['dynamodb_written']}/{spark_result['dynamodb_total']} "
                f"records written to 'etl_analytics' "
                f"({spark_result['dynamodb_failed']} failed)"
            )
            logger.info(
                "    Write status      →  "
                "python manage.py etl --view-dynamodb-status"
            )
        logger.info(_DIVIDER)
        if run_pandas and run_pyspark:
            logger.info("  Full ETL complete across PostgreSQL, OpenSearch, Iceberg, and DynamoDB Local.")
        elif run_pandas:
            logger.info("  Pandas ETL complete across PostgreSQL and OpenSearch.")
        elif run_pyspark:
            logger.info("  PySpark ETL complete across Iceberg and DynamoDB Local.")
        logger.info(_DIVIDER)