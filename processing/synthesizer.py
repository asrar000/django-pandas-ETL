"""
Synthesizer — generates realistic generic e-commerce data.

The API seed (real carts/users) is used as a structural template.
We always synthesise up to num_synthetic_records orders with:
  • Globally recognizable first/last names and cities
  • Products with realistic USD price ranges
  • Common e-commerce payment methods
  • Standard shipping providers
"""
import random
import uuid
from datetime import datetime, timedelta

from config.loader import get
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Generic e-commerce reference data ────────────────────────────────────────
_FIRST_NAMES = [
    "Alex", "Taylor", "Jordan", "Morgan", "Casey", "Avery", "Jamie", "Riley",
    "Noah", "Emma", "Liam", "Olivia", "Ethan", "Sophia", "Mia", "Lucas",
    "Isabella", "Aiden", "Harper", "Amelia", "Daniel", "Chloe", "Ella",
    "Mason", "Zoe", "Levi", "Aria", "Logan", "Hannah", "Owen", "Luna",
]

_LAST_NAMES = [
    "Smith", "Johnson", "Brown", "Garcia", "Lee", "Martinez", "Davis",
    "Kim", "Patel", "Anderson", "Miller", "Clark", "Wright", "Lopez",
    "Walker", "Hall", "Young", "Allen", "Scott", "Green", "Baker", "Adams",
]

_CITIES = [
    "New York", "Los Angeles", "Chicago", "Toronto", "London", "Berlin",
    "Paris", "Amsterdam", "Sydney", "Singapore", "Dubai", "Tokyo",
    "Madrid", "Dublin", "Mumbai", "Sao Paulo", "Mexico City", "Cape Town",
]

_EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com", "icloud.com",
    "proton.me", "examplemail.com",
]

_PAYMENT_METHODS = [
    "Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay",
    "Bank Transfer", "Gift Card", "Cash on Delivery", "Buy Now, Pay Later",
]

_COURIERS = [
    ("UPS",               4, 12),
    ("FedEx",             5, 18),
    ("DHL",               8, 22),
    ("USPS",              4, 10),
    ("Amazon Logistics",  3,  9),
    ("Regional Carrier",  5, 14),
]

# Product catalogue with (name, min_USD_price, max_USD_price)
_PRODUCTS: dict[str, list[tuple[str, float, float]]] = {
    "electronics": [
        ("4K Smart TV 55-inch",   399, 899),
        ("Bluetooth Speaker",      29, 129),
        ("Wireless Earbuds",       49, 199),
        ("USB-C Hub 7-in-1",       24,  79),
        ("Laptop Stand",           25,  89),
    ],
    "clothing": [
        ("Classic Cotton T-Shirt",  15,  35),
        ("Premium Hoodie",          35,  95),
        ("Slim Fit Jeans",          30,  85),
        ("Running Shorts",          18,  45),
        ("Winter Jacket",           60, 180),
    ],
    "groceries": [
        ("Organic Coffee Beans 1lb", 12, 24),
        ("Extra Virgin Olive Oil 1L", 10, 22),
        ("Pasta Variety Pack",        8, 18),
        ("Protein Bars Box",         14, 32),
        ("Sparkling Water 12-pack",   6, 16),
    ],
    "home-appliances": [
        ("Air Fryer 5qt",          69, 179),
        ("Robot Vacuum",          149, 399),
        ("Blender",                39, 119),
        ("Espresso Machine",      129, 299),
        ("Compact Microwave",      89, 229),
    ],
    "beauty": [
        ("Hydrating Face Serum",   18,  48),
        ("Moisturizer",            15,  38),
        ("Eau de Parfum 50ml",     35, 120),
        ("Shampoo 500ml",          10,  28),
        ("Sunscreen SPF50",        12,  32),
    ],
    "books": [
        ("Business Strategy Hardcover", 18, 42),
        ("Mystery Novel Paperback",    12, 24),
        ("Productivity Journal",       14, 32),
        ("Cookbook Collection",        20, 44),
        ("Programming Handbook",       24, 60),
    ],
    "sports": [
        ("Yoga Mat",                18,  45),
        ("Resistance Bands Set",    15,  40),
        ("Insulated Water Bottle",  14,  32),
        ("Adjustable Dumbbell",     80, 220),
        ("Fitness Tracker",         49, 149),
    ],
    "mobile": [
        ("iPhone 15",             699, 999),
        ("Samsung Galaxy S24",    649, 949),
        ("Google Pixel 8",        549, 799),
        ("OnePlus 12",            599, 899),
        ("Moto G Power",          199, 329),
    ],
    "accessories": [
        ("Screen Protector 2-pack", 12,  28),
        ("Phone Case",              10,  35),
        ("USB-C Charging Cable",     9,  24),
        ("Portable SSD 1TB",        79, 149),
        ("Wireless Mouse",          19,  59),
    ],
    "furniture": [
        ("Ergonomic Office Chair", 129, 349),
        ("Standing Desk",          199, 499),
        ("Bookshelf",               59, 179),
        ("Coffee Table",            69, 199),
        ("Nightstand Set",          79, 189),
    ],
}


# ── Internal generators ───────────────────────────────────────────────────────

def _random_dt(days_ago_min: int = 0, days_ago_max: int = 365) -> datetime:
    offset = random.randint(days_ago_min, days_ago_max)
    base = datetime.now() - timedelta(days=offset)
    # Add random hour/minute/second
    return base.replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        microsecond=0,
    )


def _make_user(user_id: int) -> dict:
    first = random.choice(_FIRST_NAMES)
    last  = random.choice(_LAST_NAMES)
    domain = random.choice(_EMAIL_DOMAINS)
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@{domain}"
    signup = _random_dt(days_ago_min=30, days_ago_max=1_460)
    return {
        "id":        user_id,
        "firstName": first,
        "lastName":  last,
        "email":     email,
        "address":   {"city": random.choice(_CITIES)},
        "createdAt": signup.isoformat(),
    }


def _make_cart(cart_id: int, user_id: int) -> dict:
    n_cats = random.randint(1, min(4, len(_PRODUCTS)))
    chosen_cats = random.sample(list(_PRODUCTS.keys()), n_cats)

    products = []
    for cat in chosen_cats:
        name, lo, hi = random.choice(_PRODUCTS[cat])
        price    = round(random.uniform(lo, hi), 2)
        qty      = random.randint(1, 5)
        discount = round(random.uniform(0.0, 0.25), 2)   # 0–25 %
        products.append({
            "id":                 random.randint(1_000, 9_999),
            "title":              name,
            "category":           cat,
            "price":              price,
            "quantity":           qty,
            "discountPercentage": round(discount * 100, 2),
        })

    courier, ship_lo, ship_hi = random.choice(_COURIERS)
    order_dt = _random_dt(days_ago_min=0, days_ago_max=365)

    return {
        "id":               cart_id,
        "userId":           user_id,
        "orderId":          str(uuid.uuid4()),
        "orderTimestamp":   order_dt.isoformat(),
        "products":         products,
        "paymentMethod":    random.choice(_PAYMENT_METHODS),
        "shippingCost":     round(random.uniform(ship_lo, ship_hi), 2),
        "shippingProvider": courier,
    }


# ── Public interface ──────────────────────────────────────────────────────────

def synthesize_orders(
    real_carts: list,
    real_users: list,
    num_records: int = 10_000,
) -> tuple[list, list]:
    """
    Synthesise *num_records* generic e-commerce orders.

    Strategy:
      • Use real API users as name/email seeds where possible.
      • Always generate generic city, payment method, products, and pricing.
      • Supplement with fully synthetic users when the API pool is exhausted.

    Returns:
        (carts_list, users_list)
    """
    logger.info(
        f"Synthesising {num_records} generic e-commerce orders  "
        f"(API seed: {len(real_carts)} carts, {len(real_users)} users)"
    )

    real_user_pool = {u["id"]: u for u in real_users}
    pool_size = len(real_users)

    synthesised_users: dict[int, dict] = {}
    synthesised_carts: list[dict]       = []

    for i in range(1, num_records + 1):
        # Cycle through real user IDs; overflow into synthetic IDs
        if pool_size > 0:
            real_uid = list(real_user_pool.keys())[(i - 1) % pool_size]
            ru = real_user_pool[real_uid]
            uid = real_uid
            if uid not in synthesised_users:
                synthesised_users[uid] = {
                    "id":        uid,
                    "firstName": ru.get("firstName", random.choice(_FIRST_NAMES)),
                    "lastName":  ru.get("lastName",  random.choice(_LAST_NAMES)),
                    "email":     ru.get("email",     f"user{uid}@gmail.com"),
                    "address":   {"city": random.choice(_CITIES)},
                    "createdAt": ru.get("createdAt", _random_dt(30, 1_460).isoformat()),
                }
        else:
            uid = i
            if uid not in synthesised_users:
                synthesised_users[uid] = _make_user(uid)

        synthesised_carts.append(_make_cart(cart_id=i, user_id=uid))

    users_list = list(synthesised_users.values())
    logger.info(
        f"Synthesis complete — {len(synthesised_carts)} orders | "
        f"{len(users_list)} unique customers"
    )
    return synthesised_carts, users_list
