"""
Order Analytics Transformer.
Shapes enriched order-level records into the order_analytics table.
"""
import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def build_order_analytics(enriched_orders: list) -> pd.DataFrame:
    """
    Build order_analytics table from enriched orders.

    Columns produced:
        order_id, customer_id, order_timestamp, order_date, order_hour,
        gross_amount, total_discount_amount,
        net_amount, shipping_cost, final_amount,
        total_items, discount_ratio, payment_method, shipping_provider
    """
    logger.info("Building order_analytics…")
    df = pd.DataFrame(enriched_orders)

    cols = [
        "order_id", "customer_id", "order_timestamp", "order_date", "order_hour",
        "gross_amount", "total_discount_amount",
        "net_amount", "shipping_cost", "final_amount",
        "total_items", "discount_ratio", "payment_method", "shipping_provider",
    ]
    order_df = df[cols].copy()

    # ── Type normalisation ────────────────────────────────────────────────────
    order_df["order_timestamp"]        = pd.to_datetime(order_df["order_timestamp"], errors="coerce")
    order_df["order_date"]             = pd.to_datetime(order_df["order_date"], errors="coerce")
    order_df["order_hour"]             = order_df["order_hour"].astype(int)
    order_df["gross_amount"]           = order_df["gross_amount"].round(2)
    order_df["total_discount_amount"]  = order_df["total_discount_amount"].round(2)
    order_df["net_amount"]             = order_df["net_amount"].round(2)
    order_df["shipping_cost"]          = order_df["shipping_cost"].round(2)
    order_df["final_amount"]           = order_df["final_amount"].round(2)
    order_df["total_items"]            = order_df["total_items"].astype(int)
    order_df["discount_ratio"]         = order_df["discount_ratio"].round(4)

    logger.info(f"order_analytics → {len(order_df)} rows")
    return order_df
