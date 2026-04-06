"""
Enrichment — merges cart + user data and derives all computed fields.
Called once per order; all derived-field logic lives here.
"""
from datetime import datetime

from config.loader import get
from utils.logger import get_logger

logger = get_logger(__name__)

_CURRENCY: str = get("processing.currency", "BDT")


# ── Per-order enrichment ──────────────────────────────────────────────────────

def enrich_order(cart: dict, user: dict) -> dict:
    """
    Merge one cart with its owning user and compute all derived fields.

    Derived fields produced here (consumed downstream by transformers):
      gross_amount, total_discount_amount, net_amount, final_amount,
      discount_ratio, total_items, order_complexity_score, dominant_category,
      order_date, order_hour, customer_tenure_days (partial — needs today's date),
      email (lowercased), email_domain.
    """
    products = cart.get("products", [])

    # ── Monetary aggregates ───────────────────────────────────────────────────
    gross = sum(p["price"] * p["quantity"] for p in products)
    disc  = sum(
        p["price"] * p["quantity"] * (p["discountPercentage"] / 100.0)
        for p in products
    )
    net      = gross - disc
    shipping = float(cart.get("shippingCost", 0))
    final    = net + shipping

    # ── Item & category aggregates ────────────────────────────────────────────
    total_items  = sum(p["quantity"] for p in products)
    unique_cats  = list({p["category"] for p in products})

    cat_spend: dict[str, float] = {}
    for p in products:
        cat = p["category"]
        cat_spend[cat] = cat_spend.get(cat, 0.0) + (
            p["price"] * p["quantity"] * (1 - p["discountPercentage"] / 100.0)
        )
    dominant_cat = max(cat_spend, key=cat_spend.get) if cat_spend else "unknown"

    # order_complexity_score = (unique product categories × 2) + total items
    complexity = (len(unique_cats) * 2) + total_items

    discount_ratio = round(disc / gross, 4) if gross > 0 else 0.0

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
    email_domain: str = email_clean.split("@")[-1] if "@" in email_clean else ""

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
        "gross_amount":              round(gross, 2),
        "total_discount_amount":     round(disc,  2),
        "net_amount":                round(net,   2),
        "shipping_cost":             round(shipping, 2),
        "final_amount":              round(final, 2),
        "discount_ratio":            discount_ratio,
        # Derived order metrics
        "total_items":               total_items,
        "order_complexity_score":    complexity,
        "dominant_category":         dominant_cat,
        "unique_categories":         unique_cats,
        "currency":                  _CURRENCY,
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
