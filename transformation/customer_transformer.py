"""
Customer Analytics Transformer.
Aggregates enriched order-level data into one row per customer.
"""
from datetime import date

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


# ── Derived-field helpers (pure functions — easy to unit-test) ────────────────

def _lifetime_value_score(total_spent: float, total_orders: int, tenure_days: int) -> float:
    """
    Weighted LTV score (0–100):
        Monetary  → up to 50 pts  (total_spent / 10 000, capped)
        Frequency → up to 30 pts  (total_orders × 2, capped)
        Recency   → up to 20 pts  (tenure_years × 10, capped)
    """
    monetary  = min(total_spent / 10_000, 50.0)
    frequency = min(total_orders * 2.0,   30.0)
    recency   = min(tenure_days / 365.0 * 10.0, 20.0)
    return round(monetary + frequency + recency, 2)


def _customer_segment(ltv: float) -> str:
    if ltv >= 60:
        return "High"
    if ltv >= 30:
        return "Medium"
    return "Low"


# ── Public transformer ────────────────────────────────────────────────────────

def build_customer_analytics(enriched_orders: list) -> pd.DataFrame:
    """
    Aggregate enriched orders into customer_analytics table.

    Columns produced:
        customer_id, full_name, email, email_domain, city,
        customer_tenure_days, total_orders, total_spent,
        avg_order_value, lifetime_value_score, customer_segment
    """
    logger.info("Building customer_analytics…")
    df = pd.DataFrame(enriched_orders)

    today = pd.Timestamp(date.today())

    agg = (
        df.groupby("customer_id", as_index=False)
        .agg(
            first_name            =("first_name",    "first"),
            last_name             =("last_name",     "first"),
            email                 =("email",         "first"),
            email_domain          =("email_domain",  "first"),
            city                  =("city",          "first"),
            signup_date           =("signup_date",   "first"),
            total_orders          =("order_id",      "nunique"),
            total_spent           =("final_amount",  "sum"),
        )
    )

    agg["full_name"] = (
        agg["first_name"].str.strip() + " " + agg["last_name"].str.strip()
    )
    agg["email"]        = agg["email"].str.lower()
    agg["signup_date"]  = pd.to_datetime(agg["signup_date"], errors="coerce")
    agg["customer_tenure_days"] = (
        (today - agg["signup_date"]).dt.days.clip(lower=0).fillna(0).astype(int)
    )
    agg["total_spent"]       = agg["total_spent"].round(2)
    agg["avg_order_value"]   = (agg["total_spent"] / agg["total_orders"]).round(2)
    agg["lifetime_value_score"] = agg.apply(
        lambda r: _lifetime_value_score(
            r["total_spent"], r["total_orders"], r["customer_tenure_days"]
        ),
        axis=1,
    )
    agg["customer_segment"] = agg["lifetime_value_score"].apply(_customer_segment)

    final_cols = [
        "customer_id", "full_name", "email", "email_domain", "city",
        "customer_tenure_days", "total_orders", "total_spent",
        "avg_order_value", "lifetime_value_score", "customer_segment",
    ]
    result = agg[final_cols].copy()
    logger.info(f"customer_analytics → {len(result)} rows")
    return result
