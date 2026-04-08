"""
Management command: view_iceberg_tables
Displays Iceberg analytics tables after ETL pipeline execution.

Usage:
    python manage.py view_iceberg_tables
"""
from django.core.management.base import BaseCommand

from config.loader import get
from services.spark_service import build_spark_session


class Command(BaseCommand):
    help = "Query and display sample data from the Iceberg analytics tables."

    def handle(self, *args, **options):
        spark = build_spark_session()
        catalog = get("iceberg.catalog", "local")
        database = get("iceberg.database", "analytics")
        customer_table = f"{catalog}.{database}.customer_analytics"
        order_table = f"{catalog}.{database}.order_analytics"

        self.stdout.write("Viewing Iceberg Analytics Tables")
        self.stdout.write("=" * 40)

        # Check if tables exist
        if not spark.catalog.tableExists(customer_table):
            self.stdout.write(
                self.style.ERROR(f"Table {customer_table} does not exist. Run the ETL pipeline first.")
            )
            return
        if not spark.catalog.tableExists(order_table):
            self.stdout.write(
                self.style.ERROR(f"Table {order_table} does not exist. Run the ETL pipeline first.")
            )
            return

        # Describe tables
        self.stdout.write(f"\nCustomer Analytics Table: {customer_table}")
        spark.sql(f"DESCRIBE {customer_table}").show()

        self.stdout.write(f"\nOrder Analytics Table: {order_table}")
        spark.sql(f"DESCRIBE {order_table}").show()

        # Sample data
        self.stdout.write(f"\nSample Customer Analytics (first 5 rows):")
        spark.sql(f"SELECT * FROM {customer_table} LIMIT 5").show()

        self.stdout.write(f"\nSample Order Analytics (first 5 rows):")
        spark.sql(f"SELECT * FROM {order_table} LIMIT 5").show()

        # Row counts
        customer_count = spark.sql(f"SELECT COUNT(*) as count FROM {customer_table}").collect()[0]["count"]
        order_count = spark.sql(f"SELECT COUNT(*) as count FROM {order_table}").collect()[0]["count"]
        self.stdout.write(f"\nTotal rows - Customer Analytics: {customer_count}, Order Analytics: {order_count}")

        spark.stop()