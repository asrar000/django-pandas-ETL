"""
Management command: dump_to_postgres
Reads processed CSVs and loads them into PostgreSQL via the db_service.

Usage:
    python manage.py dump_to_postgres
"""
from django.core.management.base import BaseCommand, CommandError

from services.db_service import load_customer_analytics, load_order_analytics
from utils.writer import read_processed_csv
from utils.logger import get_logger

logger = get_logger(__name__)


class Command(BaseCommand):
    help = "Load processed CSV files into PostgreSQL using Django ORM (upsert)."

    def handle(self, *args, **options) -> None:
        self.stdout.write(self.style.MIGRATE_HEADING(
            "\n╔══════════════════════════════════╗\n"
            "║   PostgreSQL Dump — BDT ETL      ║\n"
            "╚══════════════════════════════════╝\n"
        ))

        # ── Read processed CSVs ───────────────────────────────────────────────
        try:
            logger.info("Reading customer_analytics.csv…")
            customer_df = read_processed_csv("customer_analytics.csv")

            logger.info("Reading order_analytics.csv…")
            order_df = read_processed_csv("order_analytics.csv")
        except FileNotFoundError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(
            f"  customer_analytics: {len(customer_df):>5} rows\n"
            f"  order_analytics:    {len(order_df):>5} rows\n"
        )

        # ── Load into DB ──────────────────────────────────────────────────────
        try:
            c_new, c_upd = load_customer_analytics(customer_df)
            o_new, o_upd = load_order_analytics(order_df)
        except Exception as exc:
            logger.exception("DB load failed")
            raise CommandError(f"DB load failed: {exc}") from exc

        # ── Summary ───────────────────────────────────────────────────────────
        self.stdout.write(self.style.SUCCESS(
            f"\n✓ CustomerAnalytics  →  {c_new} created | {c_upd} updated"
        ))
        self.stdout.write(self.style.SUCCESS(
            f"✓ OrderAnalytics     →  {o_new} created | {o_upd} updated"
        ))
        self.stdout.write(self.style.SUCCESS(
            "\n✅  Dump complete! Data is now queryable via Django ORM / psql.\n"
        ))
