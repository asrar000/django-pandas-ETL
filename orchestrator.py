#!/usr/bin/env python
"""
BDT E-Commerce ETL Pipeline — Orchestrator
===========================================
Single entry point. Run:
    python orchestrator.py

Pipeline stages:
    1. Extract   — fetch carts & users from dummyjson.com (with retries)
    2. Synthesise — generate BDT-priced Bangladeshi e-commerce orders
    3. Enrich    — merge cart + user data, compute derived fields
    4. Transform — build customer_analytics & order_analytics DataFrames
    5. Write     — persist processed CSVs to data/processed/

After this completes, run:
    python manage.py dump_to_postgres
to load the CSVs into PostgreSQL.
"""
import os
import sys
from pathlib import Path

# ── Django bootstrap (must precede any Django / ORM imports) ─────────────────
_BASE = Path(__file__).resolve().parent
if str(_BASE) not in sys.path:
    sys.path.insert(0, str(_BASE))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "etl_core.settings")

import django  # noqa: E402
django.setup()
# ─────────────────────────────────────────────────────────────────────────────

from config.loader import get
from utils.paths import ensure_dirs
from utils.logger import get_logger
from utils.writer import write_raw_json, write_processed_csv
from extractor.api_extractor import extract_all
from processing.synthesizer import synthesize_orders
from processing.enrichment import enrich_all
from transformation.customer_transformer import build_customer_analytics
from transformation.order_transformer import build_order_analytics

logger = get_logger("orchestrator")

_DIVIDER = "═" * 62


def run() -> None:
    logger.info(_DIVIDER)
    logger.info("      BDT E-Commerce ETL Pipeline  —  Orchestrator")
    logger.info(_DIVIDER)

    # ── 0. Setup directories ─────────────────────────────────────────────────
    ensure_dirs()

    # ── 1. Extract ───────────────────────────────────────────────────────────
    logger.info("[STEP 1/5]  Extracting raw data from API…")
    try:
        raw_carts, raw_users = extract_all()
    except Exception as exc:
        logger.warning(f"API extraction failed ({exc}). Continuing with empty seed.")
        raw_carts, raw_users = [], []

    write_raw_json(raw_carts, "raw_carts.json")
    write_raw_json(raw_users, "raw_users.json")

    # ── 2. Synthesise ────────────────────────────────────────────────────────
    num_records: int = int(get("processing.num_synthetic_records", 500))
    logger.info(f"[STEP 2/5]  Synthesising {num_records} BDT orders…")
    carts, users = synthesize_orders(raw_carts, raw_users, num_records=num_records)
    write_raw_json(carts, "synthesised_carts.json")
    write_raw_json(users, "synthesised_users.json")

    # ── 3. Enrich ────────────────────────────────────────────────────────────
    logger.info("[STEP 3/5]  Enriching orders…")
    enriched = enrich_all(carts, users)

    # ── 4. Transform ─────────────────────────────────────────────────────────
    logger.info("[STEP 4/5]  Building analytics tables…")
    customer_df = build_customer_analytics(enriched)
    order_df    = build_order_analytics(enriched)

    # ── 5. Write ─────────────────────────────────────────────────────────────
    logger.info("[STEP 5/5]  Writing processed output…")
    write_processed_csv(customer_df, "customer_analytics.csv")
    write_processed_csv(order_df,    "order_analytics.csv")

    # ── Summary ──────────────────────────────────────────────────────────────
    logger.info(_DIVIDER)
    logger.info(f"  ✓ customer_analytics  →  {len(customer_df):>5} rows")
    logger.info(f"  ✓ order_analytics     →  {len(order_df):>5} rows")
    logger.info(_DIVIDER)
    logger.info(
        "  ETL complete.\n"
        "  Next → python manage.py dump_to_postgres"
    )
    logger.info(_DIVIDER)


if __name__ == "__main__":
    run()
