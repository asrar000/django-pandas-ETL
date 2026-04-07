# 🛒 Generic E-Commerce ETL Pipeline

A production-structured Django + Pandas + PostgreSQL ETL pipeline that extracts seed e-commerce data, synthesises realistic **generic e-commerce** orders at scale, enriches and transforms them into two analytical tables, and loads the results into PostgreSQL.

---

## 📐 Architecture Overview

```
orchestrator.py          ← single entry point (Step 1–5)
│
├── extractor/           ← Step 1 : HTTP fetch with retries & exponential back-off
├── processing/          ← Step 2 : synthetic order generation  |  Step 3 : enrichment
├── transformation/      ← Step 4 : customer_analytics & order_analytics builders
├── utils/               ← paths · logger · writer  (shared, imported everywhere)
├── services/            ← Step 5 (DB) : ORM upsert helpers
│
├── pipeline/            ← Django app : models · admin · management command
│   └── management/commands/dump_to_postgres.py
│
├── etl_core/            ← Django project : settings · urls · wsgi
├── config/              ← config.yml  +  loader.py
│
├── data/
│   ├── raw/             ← JSON snapshots from API & synthesiser
│   └── processed/       ← customer_analytics.csv & order_analytics.csv
└── logs/                ← etl_pipeline.log
```

---

## 📦 Prerequisites

| Tool | Version |
|------|---------|
| Python | ≥ 3.11 |
| Docker + Docker Compose | any recent version |
| pip | ≥ 23 |

---

## 🚀 Quick Start (two commands)

### Step 0 — Install dependencies & start Postgres

```bash
# Create & activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python packages
pip install -r requirements.txt

# Start PostgreSQL container (runs in background)
docker compose up -d

# Wait ~10 s for Postgres to be healthy, then run migrations
python manage.py makemigrations
python manage.py migrate
```

---

### Command 1 — Run the full ETL pipeline

```bash
python orchestrator.py
```

**What this does:**
1. Fetches raw carts & users from `https://dummyjson.com` (with retry + back-off)
2. Synthesises **10,000 generic orders** with globally recognizable names, cities, products, payment methods, and pricing
3. Enriches every order (gross/net/discount/complexity/dominant-category …)
4. Transforms into `customer_analytics` and `order_analytics` DataFrames
5. Writes `data/processed/customer_analytics.csv` and `data/processed/order_analytics.csv`

Logs stream to the console **and** `logs/etl_pipeline.log`.

---

### Command 2 — Dump processed data into PostgreSQL

```bash
python manage.py dump_to_postgres
```

**What this does:**
- Reads the two processed CSVs
- Upserts every row into PostgreSQL using Django ORM (`update_or_create`)
- Safe to run multiple times — duplicate rows are updated, not duplicated

---

## ⚙️ Configuration

All configuration lives in **`config/config.yml`** — no `.env` file needed.

```yaml
api:
  base_url: "https://dummyjson.com"
  max_retries: 5          # retry attempts before giving up
  backoff_factor: 2       # wait = 2^attempt seconds

processing:
  num_synthetic_records: 10000   # change this to generate more/fewer orders
  currency: "USD"
  currency_symbol: "$"

database:
  host: "localhost"
  port: 5432
  name: "etl_db"
  user: "etl_user"
  password: "etl_password"
```

Values are loaded lazily via `config/loader.py` using dot-notation:
```python
from config.loader import get
db_host = get("database.host")   # → "localhost"
```

---

## 🗄️ Database Tables

### `customer_analytics`
| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | INT (unique) | Source user ID |
| `full_name` | VARCHAR | first + last name |
| `email` | VARCHAR | lowercased |
| `email_domain` | VARCHAR | extracted from email |
| `city` | VARCHAR | customer city |
| `customer_tenure_days` | INT | today − signup_date |
| `total_orders` | INT | distinct order count |
| `total_spent` | DECIMAL | Σ final_amount (default: USD) |
| `avg_order_value` | DECIMAL | total_spent / total_orders |
| `lifetime_value_score` | DECIMAL | weighted LTV (0–100) |
| `customer_segment` | VARCHAR | High / Medium / Low |

### `order_analytics`
| Column | Type | Description |
|--------|------|-------------|
| `order_id` | VARCHAR (unique) | UUID |
| `customer_id` | INT | FK reference |
| `order_date` | DATE | extracted from timestamp |
| `order_hour` | INT | 0–23 |
| `total_items` | INT | Σ quantities |
| `gross_amount` | DECIMAL | Σ price×qty (default: USD) |
| `total_discount_amount` | DECIMAL | Σ discounts |
| `net_amount` | DECIMAL | gross − discount |
| `shipping_cost` | DECIMAL | courier fee |
| `final_amount` | DECIMAL | net + shipping |
| `discount_ratio` | DECIMAL | discount / gross |
| `order_complexity_score` | INT | (unique_cats×2) + items |
| `dominant_category` | VARCHAR | highest-spend category |
| `payment_method` | VARCHAR | Credit Card, PayPal, Apple Pay… |
| `currency` | VARCHAR | defaults to USD |

---

## 🔍 Querying Data

```bash
# Connect to running container
docker compose exec db psql -U etl_user -d etl_db

# Sample queries
SELECT customer_segment, COUNT(*), ROUND(AVG(total_spent)) AS avg_spent
FROM customer_analytics
GROUP BY customer_segment
ORDER BY avg_spent DESC;

SELECT dominant_category, COUNT(*) AS orders, ROUND(SUM(final_amount)) AS revenue
FROM order_analytics
GROUP BY dominant_category
ORDER BY revenue DESC;
```

Via Django shell:
```bash
python manage.py shell
```
```python
from pipeline.models import CustomerAnalytics, OrderAnalytics

# Top 5 High-value customers
CustomerAnalytics.objects.filter(customer_segment="High").order_by("-total_spent")[:5]

# Total revenue
from django.db.models import Sum
OrderAnalytics.objects.aggregate(total=Sum("final_amount"))
```

---

## 🐳 Docker Reference

```bash
docker compose up -d          # start Postgres in background
docker compose ps             # check health
docker compose logs -f db     # stream Postgres logs
docker compose down           # stop (data persists in volume)
docker compose down -v        # stop AND delete all data
```

---

## 📁 Project Layout

```
ecommerce_etl/
├── config/
│   ├── config.yml            ← all config values
│   └── loader.py             ← dot-notation config accessor
├── extractor/
│   └── api_extractor.py      ← HTTP + retry/backoff
├── processing/
│   ├── synthesizer.py        ← generic synthetic data generator
│   └── enrichment.py        ← derived field computation
├── transformation/
│   ├── customer_transformer.py
│   └── order_transformer.py
├── services/
│   └── db_service.py         ← ORM upsert helpers
├── utils/
│   ├── logger.py             ← shared logger factory
│   ├── paths.py              ← all path constants
│   └── writer.py             ← read/write helpers
├── etl_core/                 ← Django project
│   ├── settings.py
│   ├── urls.py
│   └── wsgi.py
├── pipeline/                 ← Django app
│   ├── models.py             ← CustomerAnalytics, OrderAnalytics
│   ├── admin.py
│   └── management/commands/
│       └── dump_to_postgres.py
├── data/
│   ├── raw/
│   └── processed/
├── logs/
├── manage.py
├── orchestrator.py           ← Command 1
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

## 🛠 Troubleshooting

| Problem | Fix |
|---------|-----|
| `connection refused` on port 5432 | Run `docker compose up -d` and wait 15 s |
| `ModuleNotFoundError: config` | Make sure you activated the venv and are in the project root directory |
| `FileNotFoundError: customer_analytics.csv` | Run `python orchestrator.py` before the dump command |
| API fetch fails (network issue) | The pipeline auto-falls back to fully synthetic data — no action needed |
| `django.db.utils.OperationalError` | Check `config/config.yml` DB credentials match `docker-compose.yml` |
