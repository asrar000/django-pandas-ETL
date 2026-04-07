# 🛒 Generic E-Commerce ETL Pipeline

A production-structured Django ETL project with two parallel analytics paths from the same extracted seed data:

- a Pandas path that writes processed CSVs and upserts PostgreSQL through Django ORM
- a PySpark path that mirrors the same synthesize, enrich, and transform logic and writes Iceberg tables

---

## 📐 Architecture Overview

```
pipeline/management/commands/etl_postgres.py  ← single entry point
│
├── extractor/           ← Step 1 : shared HTTP extraction with retries/back-off
├── processing/          ← Pandas + PySpark synthesis and enrichment logic
├── transformation/      ← Pandas + PySpark analytics builders
├── utils/               ← paths · logger · writer  (shared, imported everywhere)
├── services/            ← PostgreSQL ORM loaders + Spark/Iceberg helpers
│
├── pipeline/            ← Django app : models · admin · management command
│   └── management/commands/
│       └── etl_postgres.py
│
├── etl_core/            ← Django project : settings · urls · wsgi
├── config/              ← config.yml  +  loader.py
│
├── data/
│   ├── raw/             ← JSON snapshots from API & Pandas synthesiser
│   ├── processed/       ← customer_analytics.csv & order_analytics.csv
│   └── iceberg/         ← local Iceberg warehouse files
└── logs/                ← etl_pipeline.log
```

---

## 📦 Prerequisites

| Tool                    | Version            |
| ----------------------- | ------------------ |
| Python                  | ≥ 3.11             |
| Java                    | ≥ 17               |
| Docker + Docker Compose | any recent version |
| pip                     | ≥ 23               |

---

## 🚀 Quick Start

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

### Command 1 — Run the full ETL, PostgreSQL load, and Iceberg write

```bash
python manage.py etl_postgres
```

**What this does:**

1. Fetches raw carts & users from `https://dummyjson.com` (with retry + back-off)
2. Launches the existing Pandas branch for synthesize → enrich → transform → CSV → PostgreSQL
3. Launches the parallel PySpark branch for synthesize → enrich → transform → Iceberg
4. Writes `data/processed/customer_analytics.csv` and `data/processed/order_analytics.csv`
5. Upserts both analytics datasets into PostgreSQL using Django ORM
6. Creates or replaces the Iceberg tables `local.analytics.customer_analytics` and `local.analytics.order_analytics`

Logs stream to the console **and** `logs/etl_pipeline.log`.

---

## ⚙️ Configuration

All configuration lives in **`config/config.yml`** — no `.env` file needed.

```yaml
api:
  base_url: "https://dummyjson.com"
  max_retries: 5 # retry attempts before giving up
  backoff_factor: 2 # wait = 2^attempt seconds

processing:
  num_synthetic_records: 10000 # change this to generate more/fewer orders
  currency_symbol: "$"

spark:
  app_name: "ecommerce_etl_iceberg"
  master: "local[*]"
  shuffle_partitions: 8

iceberg:
  catalog: "local"
  database: "analytics"
  warehouse_dir: "data/iceberg/warehouse"
  package: "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"

database:
  host: "localhost"
  port: 5432
  name: "etl_db"
  user: "etl_user"
  password: "etl_password"
```

The Spark branch writes to a local Hadoop Iceberg catalog rooted at `data/iceberg/warehouse`.

Values are loaded lazily via `config/loader.py` using dot-notation:

```python
from config.loader import get
db_host = get("database.host")   # → "localhost"
```

---

## 🗄️ Database Tables

### `customer_analytics`

| Column                 | Type         | Description                     |
| ---------------------- | ------------ | ------------------------------- |
| `customer_id`          | INT (unique) | Source user ID                  |
| `full_name`            | VARCHAR      | first + last name               |
| `email`                | VARCHAR      | lowercased                      |
| `email_domain`         | VARCHAR      | extracted from normalized email |
| `city`                 | VARCHAR      | customer city                   |
| `customer_tenure_days` | INT          | today − signup_date             |
| `total_orders`         | INT          | distinct order count            |
| `total_spent`          | DECIMAL      | Σ final_amount (default: USD)   |
| `avg_order_value`      | DECIMAL      | total_spent / total_orders      |
| `lifetime_value_score` | DECIMAL      | weighted LTV (0–100)            |
| `customer_segment`     | VARCHAR      | High / Medium / Low             |

### `order_analytics`

| Column                   | Type             | Description                                      |
| ------------------------ | ---------------- | ------------------------------------------------ |
| `order_id`               | VARCHAR (unique) | UUID                                             |
| `customer_id`            | INT              | FK reference                                     |
| `order_timestamp`        | TIMESTAMP        | original order timestamp from the source payload |
| `order_date`             | DATE             | extracted from timestamp                         |
| `order_hour`             | INT              | 0–23                                             |
| `gross_amount`           | DECIMAL          | Σ price×qty (default: USD)                       |
| `total_discount_amount`  | DECIMAL          | Σ discounts                                      |
| `net_amount`             | DECIMAL          | gross − discount                                 |
| `shipping_cost`          | DECIMAL          | courier fee                                      |
| `final_amount`           | DECIMAL          | net + shipping                                   |
| `total_items`            | INT              | Σ quantities                                     |
| `discount_ratio`         | DECIMAL          | discount / gross                                 |
| `order_complexity_score` | INT              | (unique products × 2) + total_items              |
| `dominant_category`      | VARCHAR          | category with the highest gross contribution     |
| `payment_method`         | VARCHAR          | Credit Card, PayPal, Apple Pay…                  |
| `shipping_provider`      | VARCHAR          | UPS, FedEx, DHL…                                 |

---

## 🧊 Iceberg Tables

The PySpark branch writes the same analytics datasets to Iceberg tables:

- `local.analytics.customer_analytics`
- `local.analytics.order_analytics`

The local warehouse path is:

- `data/iceberg/warehouse`

Each `etl_postgres` run replaces the latest Iceberg table contents while keeping Iceberg table metadata and snapshots managed by the table format.

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

SELECT payment_method, COUNT(*) AS orders, ROUND(SUM(final_amount)) AS revenue
FROM order_analytics
GROUP BY payment_method
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

Via PySpark SQL:

```bash
.venv/bin/python
```

```python
from services.spark_service import build_spark_session

spark = build_spark_session()
spark.sql("SELECT COUNT(*) FROM local.analytics.customer_analytics").show()
spark.sql("SELECT payment_method, SUM(final_amount) AS revenue FROM local.analytics.order_analytics GROUP BY payment_method").show()
spark.stop()
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
│   ├── spark_synthesizer.py  ← Spark synthetic data generator
│   ├── enrichment.py         ← Pandas branch derived fields
│   └── spark_enrichment.py   ← Spark branch derived fields
├── transformation/
│   ├── customer_transformer.py
│   ├── order_transformer.py
│   ├── spark_customer_transformer.py
│   └── spark_order_transformer.py
├── services/
│   ├── db_service.py         ← ORM upsert helpers
│   ├── spark_service.py      ← Spark session + Iceberg catalog config
│   └── iceberg_service.py    ← Iceberg table writers
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
│       └── etl_postgres.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── iceberg/
├── logs/
├── manage.py
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

## 🛠 Troubleshooting

| Problem                                        | Fix                                                                                                  |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `connection refused` on port 5432              | Run `docker compose up -d` and wait 15 s                                                             |
| `ModuleNotFoundError: config`                  | Make sure you activated the venv and are in the project root directory                               |
| `FileNotFoundError: customer_analytics.csv`    | Run `python manage.py etl_postgres` before the dump command                                          |
| API fetch fails (network issue)                | The pipeline auto-falls back to fully synthetic data — no action needed                              |
| `django.db.utils.OperationalError`             | Check `config/config.yml` DB credentials match `docker-compose.yml`                                  |
| Spark/Iceberg startup is slow on the first run | Spark downloads the Iceberg runtime JAR once into `~/.ivy2`; later runs are faster                   |
| Iceberg write fails before Spark starts        | Make sure Java is installed and the machine can reach Maven Central for the Iceberg package download |
