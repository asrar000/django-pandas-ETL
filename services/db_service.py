"""
Database Service — loads transformed DataFrames into PostgreSQL via Django ORM.
All DB interaction in the pipeline goes through this module.
"""
import pandas as pd
from django.db import transaction
from django.utils import timezone

from pipeline.models import CustomerAnalytics, OrderAnalytics
from utils.logger import get_logger

logger = get_logger(__name__)


def _bulk_upsert(model_class, records: list[dict], unique_field: str) -> tuple[int, int]:
    """
    Generic upsert helper: update_or_create for every record.
    Returns (created_count, updated_count).
    """
    created = updated = 0
    with transaction.atomic():
        for record in records:
            _, is_new = model_class.objects.update_or_create(
                **{unique_field: record[unique_field]},
                defaults=record,
            )
            if is_new:
                created += 1
            else:
                updated += 1
    logger.info(f"{model_class.__name__}: {created} created | {updated} updated")
    return created, updated


def load_customer_analytics(df: pd.DataFrame) -> tuple[int, int]:
    """
    Upsert all rows in *df* into the CustomerAnalytics model.
    Unique key: customer_id.
    """
    logger.info(f"Loading {len(df)} customer rows → DB…")
    records: list[dict] = df.to_dict(orient="records")

    for r in records:
        r["customer_id"]         = int(r["customer_id"])
        r["total_orders"]        = int(r["total_orders"])
        r["customer_tenure_days"]= int(r["customer_tenure_days"])
        r["total_spent"]         = float(r["total_spent"])
        r["avg_order_value"]     = float(r["avg_order_value"])
        r["lifetime_value_score"]= float(r["lifetime_value_score"])
        r["email_domain"]        = str(r.get("email_domain", ""))

    return _bulk_upsert(CustomerAnalytics, records, "customer_id")


def load_order_analytics(df: pd.DataFrame) -> tuple[int, int]:
    """
    Upsert all rows in *df* into the OrderAnalytics model.
    Unique key: order_id.
    """
    logger.info(f"Loading {len(df)} order rows → DB…")
    records: list[dict] = df.to_dict(orient="records")

    for r in records:
        r["customer_id"]          = int(r["customer_id"])
        r["order_hour"]           = int(r["order_hour"])
        r["gross_amount"]         = float(r["gross_amount"])
        r["total_discount_amount"]= float(r["total_discount_amount"])
        r["net_amount"]           = float(r["net_amount"])
        r["shipping_cost"]        = float(r["shipping_cost"])
        r["final_amount"]         = float(r["final_amount"])
        r["total_items"]          = int(r["total_items"])
        r["discount_ratio"]       = float(r["discount_ratio"])
        r["order_complexity_score"] = int(r["order_complexity_score"])
        r["dominant_category"]      = str(r.get("dominant_category", ""))
        order_timestamp = pd.to_datetime(r.get("order_timestamp"), errors="coerce")
        if not pd.isna(order_timestamp):
            order_timestamp = order_timestamp.to_pydatetime()
            if timezone.is_naive(order_timestamp):
                order_timestamp = timezone.make_aware(
                    order_timestamp,
                    timezone.get_current_timezone(),
                )
            r["order_timestamp"] = order_timestamp
        # Pandas Timestamp → Python date (Django DateField)
        if hasattr(r.get("order_date"), "date"):
            r["order_date"] = r["order_date"].date()

    return _bulk_upsert(OrderAnalytics, records, "order_id")
