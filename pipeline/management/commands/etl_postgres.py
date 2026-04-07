"""
Management command: etl_postgres
Runs the full ETL workflow and then loads the processed analytics CSVs into
PostgreSQL via the existing dump_to_postgres command.

Usage:
    python manage.py etl_postgres
"""
from django.core.management import call_command
from django.core.management.base import BaseCommand

from config.loader import get
from extractor.api_extractor import extract_all
from processing.enrichment import enrich_all
from processing.synthesizer import synthesize_orders
from transformation.customer_transformer import build_customer_analytics
from transformation.order_transformer import build_order_analytics
from utils.logger import get_logger
from utils.paths import ensure_dirs
from utils.writer import write_processed_csv, write_raw_json

logger = get_logger("pipeline.management.commands.etl_postgres")

_DIVIDER = "═" * 62


class Command(BaseCommand):
    help = (
        "Run the full ETL pipeline: extract, synthesise, enrich, transform, "
        "write processed CSVs, and dump them into PostgreSQL."
    )

    def handle(self, *args, **options) -> None:
        logger.info(_DIVIDER)
        logger.info("      Generic E-Commerce ETL Pipeline  —  Full Run")
        logger.info(_DIVIDER)

        # ── 0. Setup directories ─────────────────────────────────────────────
        ensure_dirs()

        # ── 1. Extract ───────────────────────────────────────────────────────
        logger.info("[STEP 1/6]  Extracting raw data from API…")
        try:
            raw_carts, raw_users = extract_all()
        except Exception as exc:
            logger.warning(f"API extraction failed ({exc}). Continuing with empty seed.")
            raw_carts, raw_users = [], []

        write_raw_json(raw_carts, "raw_carts.json")
        write_raw_json(raw_users, "raw_users.json")

        # ── 2. Synthesise ────────────────────────────────────────────────────
        num_records: int = int(get("processing.num_synthetic_records", 10_000))
        logger.info(f"[STEP 2/6]  Synthesising {num_records} generic orders…")
        carts, users = synthesize_orders(raw_carts, raw_users, num_records=num_records)
        write_raw_json(carts, "synthesised_carts.json")
        write_raw_json(users, "synthesised_users.json")

        # ── 3. Enrich ────────────────────────────────────────────────────────
        logger.info("[STEP 3/6]  Enriching orders…")
        enriched = enrich_all(carts, users)

        # ── 4. Transform ─────────────────────────────────────────────────────
        logger.info("[STEP 4/6]  Building analytics tables…")
        customer_df = build_customer_analytics(enriched)
        order_df = build_order_analytics(enriched)

        # ── 5. Write ─────────────────────────────────────────────────────────
        logger.info("[STEP 5/6]  Writing processed output…")
        write_processed_csv(customer_df, "customer_analytics.csv")
        write_processed_csv(order_df, "order_analytics.csv")

        logger.info(_DIVIDER)
        logger.info(f"  ✓ customer_analytics  →  {len(customer_df):>5} rows")
        logger.info(f"  ✓ order_analytics     →  {len(order_df):>5} rows")
        logger.info(_DIVIDER)

        # ── 6. Load ──────────────────────────────────────────────────────────
        logger.info("[STEP 6/6]  Dumping processed analytics to PostgreSQL…")
        call_command("dump_to_postgres")

        logger.info(_DIVIDER)
        logger.info("  Full ETL + PostgreSQL load complete.")
        logger.info(_DIVIDER)
