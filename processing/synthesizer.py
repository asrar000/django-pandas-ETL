"""
Synthesizer — generates realistic Bangladeshi Taka (BDT) e-commerce data.

The API seed (real carts/users) is used as a structural template.
We always synthesise up to num_synthetic_records orders with:
  • Bangladeshi first/last names and cities
  • Products with BDT price ranges
  • Payment methods common in Bangladesh (bKash, Nagad, etc.)
  • Local courier/delivery providers
"""
import random
import uuid
from datetime import datetime, timedelta

from config.loader import get
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Bangladeshi reference data ────────────────────────────────────────────────
_BD_FIRST_NAMES = [
    "Rahim", "Karim", "Hasan", "Hussain", "Ahmed", "Ali", "Reza", "Farhan",
    "Sakib", "Tanvir", "Rafiq", "Sabbir", "Mahmud", "Arif", "Belal",
    "Nadia", "Fatima", "Ayesha", "Taslima", "Sumaiya", "Mim", "Tanha",
    "Ritu", "Puja", "Jannatul", "Rabeya", "Shahin", "Shafiul", "Mostak",
    "Zahid", "Nasrin", "Parvin", "Rubel", "Monir", "Sohel",
]

_BD_LAST_NAMES = [
    "Rahman", "Islam", "Hossain", "Ahmed", "Khan", "Chowdhury", "Akhter",
    "Begum", "Molla", "Sheikh", "Talukder", "Sarkar", "Mondal", "Roy",
    "Das", "Biswas", "Paul", "Ghosh", "Dey", "Saha", "Alam", "Bhuiyan",
]

_BD_CITIES = [
    "Dhaka", "Chattogram", "Sylhet", "Rajshahi", "Khulna", "Barishal",
    "Mymensingh", "Rangpur", "Comilla", "Narayanganj", "Gazipur",
    "Tongi", "Bogura", "Jessore", "Dinajpur", "Manikganj", "Narsingdi",
]

_EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "bd.com", "grameenphone.com", "robi.com.bd",
]

_PAYMENT_METHODS = [
    "bKash", "Nagad", "Rocket", "Upay", "Card", "Cash on Delivery",
    "DBBL Nexus", "SureCash",
]

_COURIERS = [
    ("Sundarban Courier", 60, 120),
    ("SA Paribahan",       80, 150),
    ("Pathao Courier",     50,  90),
    ("Redx",               55,  95),
    ("Paperfly",           60, 100),
    ("eCourier",           70, 110),
]

# Product catalogue with (name, min_BDT_price, max_BDT_price)
_BD_PRODUCTS: dict[str, list[tuple[str, float, float]]] = {
    "electronics": [
        ("Smart TV 32\"",        22_000, 32_000),
        ("Bluetooth Speaker",     1_200,  3_500),
        ("Power Bank 10000mAh",     900,  1_800),
        ("USB Hub 4-Port",          300,    700),
        ("Laptop Charger",          800,  1_600),
    ],
    "clothing": [
        ("Panjabi Set",             600,  1_600),
        ("Salwar Kameez",           800,  2_200),
        ("Jeans Pant",              700,  1_900),
        ("T-Shirt Pack (3 pcs)",    350,    900),
        ("Saree Cotton",          1_000,  3_500),
    ],
    "groceries": [
        ("Mustard Oil 1L",          180,    240),
        ("Basmati Rice 5kg",        450,    650),
        ("Masoor Daal 1kg",          90,    140),
        ("Hilsha Fish 1kg",         800,  1_500),
        ("Soyabean Oil 5L",         750,    950),
    ],
    "home-appliances": [
        ("Ceiling Fan",           3_500,  7_000),
        ("Rice Cooker 1.8L",      1_800,  3_800),
        ("Blender 600W",          1_200,  2_500),
        ("Electric Iron",           600,  1_300),
        ("Water Purifier",        4_500, 10_000),
    ],
    "beauty": [
        ("Herbal Soap Pack (6)",    120,    220),
        ("Shampoo 400ml",           150,    280),
        ("Perfume EDP 50ml",        400,  1_400),
        ("Coconut Oil 200ml",        80,    160),
        ("Sunscreen SPF50",         200,    500),
    ],
    "books": [
        ("SSC Math Guide",          150,    260),
        ("Bangla Novel",            120,    320),
        ("HSC Physics",             200,    300),
        ("Islamic Books Set",       350,    700),
        ("English Grammar (Advanced)", 200, 420),
    ],
    "sports": [
        ("Cricket Bat (Kashmir Willow)", 800, 2_200),
        ("Football Size 5",         400,    950),
        ("Badminton Racket Set",    350,    800),
        ("Gym Gloves",              200,    550),
        ("Yoga Mat",                350,    750),
    ],
    "mobile": [
        ("Symphony Z50",          8_000, 11_000),
        ("Walton Primo",          7_500, 10_000),
        ("Samsung Galaxy A14",   18_000, 23_000),
        ("Xiaomi Redmi 12",      20_000, 26_000),
        ("Realme C55",           16_000, 21_000),
    ],
    "accessories": [
        ("Screen Guard Pack (3)",    80,    160),
        ("Phone Case Silicone",      60,    130),
        ("Wired Earphone",          150,    450),
        ("Memory Card 64GB",        500,    850),
        ("OTG Adapter",              80,    160),
    ],
    "furniture": [
        ("Plastic Chair Set (4)",  1_200,  2_200),
        ("Study Table",           3_000,  7_000),
        ("Wooden Wardrobe",      12_000, 26_000),
        ("Steel Single Bed",      5_000, 10_000),
        ("Bookshelf 3-Tier",      2_500,  5_000),
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
    first = random.choice(_BD_FIRST_NAMES)
    last  = random.choice(_BD_LAST_NAMES)
    domain = random.choice(_EMAIL_DOMAINS)
    email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@{domain}"
    signup = _random_dt(days_ago_min=30, days_ago_max=1_460)
    return {
        "id":        user_id,
        "firstName": first,
        "lastName":  last,
        "email":     email,
        "address":   {"city": random.choice(_BD_CITIES)},
        "createdAt": signup.isoformat(),
    }


def _make_cart(cart_id: int, user_id: int) -> dict:
    n_cats = random.randint(1, min(4, len(_BD_PRODUCTS)))
    chosen_cats = random.sample(list(_BD_PRODUCTS.keys()), n_cats)

    products = []
    for cat in chosen_cats:
        name, lo, hi = random.choice(_BD_PRODUCTS[cat])
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
    num_records: int = 500,
) -> tuple[list, list]:
    """
    Synthesise *num_records* BDT orders.

    Strategy:
      • Use real API users as name/email seeds where possible.
      • Always generate Bangladeshi city, payment method, products and prices.
      • Supplement with fully synthetic users when the API pool is exhausted.

    Returns:
        (carts_list, users_list)
    """
    logger.info(
        f"Synthesising {num_records} BDT orders  "
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
                    "firstName": ru.get("firstName", random.choice(_BD_FIRST_NAMES)),
                    "lastName":  ru.get("lastName",  random.choice(_BD_LAST_NAMES)),
                    "email":     ru.get("email",     f"user{uid}@gmail.com"),
                    "address":   {"city": random.choice(_BD_CITIES)},
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
