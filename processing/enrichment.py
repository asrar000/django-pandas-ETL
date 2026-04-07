"""
Enrichment — merges cart + user data and derives only the fields required by
the downstream analytics tables.
"""
from datetime import datetime

from utils.logger import get_logger

logger = get_logger(__name__)


# ── Per-order enrichment ──────────────────────────────────────────────────────

def enrich_order(cart: dict, user: dict) -> dict:
    """
    Merge one cart with its owning user and compute only the downstream fields
    needed by the analytics transformers.
    """
    products = cart.get("products", [])
    unique_product_ids = {p.get("id") for p in products if p.get("id") is not None}

    # ── Monetary aggregates ───────────────────────────────────────────────────
    gross = sum(p["price"] * p["quantity"] for p in products)
    disc  = sum(
        p["price"] * p["quantity"] * (p["discountPercentage"] / 100.0)
        for p in products
    )
    net      = gross - disc
    shipping = float(cart.get("shippingCost", 0))
    final    = net + shipping

    discount_ratio = round(disc / gross, 4) if gross > 0 else 0.0
    total_items = sum(p["quantity"] for p in products)
    order_complexity_score = len(unique_product_ids) * 2 + total_items

    category_totals: dict[str, float] = {}
    for product in products:
        category = str(product.get("category", "")).strip() or "Unknown"
        category_totals[category] = category_totals.get(category, 0.0) + (
            product["price"] * product["quantity"]
        )
    dominant_category = (
        max(category_totals.items(), key=lambda item: (item[1], item[0]))[0]
        if category_totals
        else "Unknown"
    )

    # ── Temporal fields ───────────────────────────────────────────────────────
    raw_ts: str = cart.get("orderTimestamp", datetime.now().isoformat())
    try:
        dt = datetime.fromisoformat(raw_ts)
    except (ValueError, TypeError):
        dt = datetime.now()

    raw_signup: str = user.get("createdAt", datetime.now().isoformat())
    try:
        signup_dt = datetime.fromisoformat(raw_signup.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        signup_dt = datetime.now()

    # ── Email enrichment ──────────────────────────────────────────────────────
    email_raw: str    = user.get("email", "")
    email_clean: str  = email_raw.strip().lower()
    email_domain: str = email_clean.split("@", 1)[1] if "@" in email_clean else ""

    address = user.get("address", {})

    return {
        # Identifiers
        "order_id":            cart.get("orderId", str(cart.get("id", ""))),
        "customer_id":         user["id"],
        # Customer raw fields
        "first_name":          user.get("firstName", ""),
        "last_name":           user.get("lastName", ""),
        "email":               email_clean,
        "email_domain":        email_domain,
        "city":                address.get("city", "Unknown"),
        "signup_date":         signup_dt.date().isoformat(),
        # Order temporal
        "order_timestamp":     raw_ts,
        "order_date":          dt.date().isoformat(),
        "order_hour":          dt.hour,
        # Products (kept for downstream use)
        "products":            products,
        "payment_method":      cart.get("paymentMethod", ""),
        "shipping_provider":   cart.get("shippingProvider", ""),
        # Derived monetary
        "total_items":               total_items,
        "gross_amount":              round(gross, 2),
        "total_discount_amount":     round(disc,  2),
        "net_amount":                round(net,   2),
        "shipping_cost":             round(shipping, 2),
        "final_amount":              round(final, 2),
        "discount_ratio":            discount_ratio,
        "order_complexity_score":    order_complexity_score,
        "dominant_category":         dominant_category,
    }


def enrich_all(carts: list, users: list) -> list:
    """
    Enrich every cart in *carts* using the matching user from *users*.
    Carts whose userId has no corresponding user are skipped with a warning.
    """
    user_map = {u["id"]: u for u in users}
    enriched: list[dict] = []
    skipped = 0

    for cart in carts:
        uid  = cart.get("userId")
        user = user_map.get(uid)
        if user is None:
            skipped += 1
            continue
        enriched.append(enrich_order(cart, user))

    if skipped:
        logger.warning(f"Skipped {skipped} carts with no matching user")
    logger.info(f"Enrichment complete — {len(enriched)} orders enriched")
    return enriched
