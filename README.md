# 🛒 Generic E-Commerce ETL Pipeline

A production-structured Django ETL project with two parallel analytics paths from the same extracted seed data:

- a Pandas path that writes processed CSVs, upserts PostgreSQL through Django ORM, and indexes OpenSearch
- a PySpark path that mirrors the same synthesize, enrich, and transform logic and writes Iceberg tables plus a single combined DynamoDB Local table

---

## 📐 Architecture Overview

```
pipeline/management/commands/etl.py  ← single entry point
│
├── extractor/           ← Step 1 : shared HTTP extraction with retries/back-off
├── processing/          ← Pandas + PySpark synthesis and enrichment logic
├── transformation/      ← Pandas + PySpark analytics builders
├── opensearch/          ← OpenSearch client · mappings · serialization · indexing
├── dynamodb_local/      ← local DynamoDB resource · table management · write-status CLI
├── utils/               ← paths · logger · writer  (shared, imported everywhere)
├── services/            ← PostgreSQL ORM loaders + Spark/Iceberg helpers
│
├── pipeline/            ← Django app : models · admin · management command
│   └── management/commands/
│       └── etl.py
│
├── etl_core/            ← Django project : settings · urls · wsgi
├── config/              ← config.yml  +  loader.py
│
├── data/
│   ├── raw/             ← JSON snapshots from API & Pandas synthesiser
│   ├── processed/       ← customer_analytics.csv, order_analytics.csv,
│   │                      dynamodb_write_status.csv
│   └── iceberg/         ← local Iceberg warehouse files
└── logs/                ← etl_pipeline.log
```

---

## 📦 Prerequisites

| Tool                    | Version                    |
| ----------------------- | -------------------------- |
| Python                  | 3.10 or 3.11               |
| Java                    | ≥ 17 (PySpark branch only) |
| Docker + Docker Compose | any recent version         |
| pip                     | any recent version         |

Java is only required when running the PySpark → Iceberg + DynamoDB Local path.
Docker is required for the local PostgreSQL, OpenSearch, DynamoDB Local, and DynamoDB Admin services.

---

## 🚀 Quick Start

### Step 0 — Install dependencies & start all services

```bash
# Create & activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install Python packages
pip install -r requirements.txt

# Create the DynamoDB Local data directory (needed once — bind mount requires it writable)
mkdir -p dynamodb_data && chmod 777 dynamodb_data

# Start all containers in background
docker compose up -d db opensearch dynamodb-local dynamodb-admin

# Wait ~10 s for Postgres to be healthy, then run migrations
python manage.py makemigrations
python manage.py migrate
```

- OpenSearch is exposed at `http://localhost:9200`
- DynamoDB Local is exposed at `http://localhost:8000`
- DynamoDB Admin dashboard is exposed at **`http://localhost:8001`**

If the OpenSearch container exits immediately on Ubuntu with a `vm.max_map_count` bootstrap error, that host-level setting must be adjusted once by an administrator.

---

### Command 1 — Run the ETL pipeline

```bash
python manage.py etl                    # Run both branches
python manage.py etl --pandas           # Run only Pandas → PostgreSQL + OpenSearch branch
python manage.py etl --pyspark          # Run only PySpark → Iceberg + DynamoDB Local branch
```

**What this does:**

1. Fetches raw carts & users from `https://dummyjson.com` (with retry + back-off)
2. **(Pandas branch)** synthesize → enrich → transform → CSV → PostgreSQL → OpenSearch
3. **(PySpark branch)** synthesize → enrich → transform → Iceberg → DynamoDB Local
4. Writes `data/processed/customer_analytics.csv` and `data/processed/order_analytics.csv`
5. Upserts both analytics datasets into PostgreSQL using Django ORM
6. Creates the OpenSearch indices `customer_analytics` and `order_analytics` if needed and bulk-indexes documents
7. Creates or replaces the Iceberg tables `local.analytics.customer_analytics` and `local.analytics.order_analytics`
8. Creates the DynamoDB Local table `etl_analytics` if needed and writes **all** customer and order rows into it as a single combined table, then saves a write-status report to `data/processed/dynamodb_write_status.csv`

Logs stream to the console **and** `logs/etl_pipeline.log`.

---

### Command 2 — Inspect DynamoDB write status

After a run, you can check exactly which records were written without scanning DynamoDB:

```bash
# Full write-status report
python manage.py etl --view-dynamodb-status

# Show only records that failed to write
python manage.py etl --view-dynamodb-status --failed-only

# Limit output to the first N rows
python manage.py etl --view-dynamodb-status --limit 20

# Combine flags
python manage.py etl --view-dynamodb-status --failed-only --limit 10
```

This reads `data/processed/dynamodb_write_status.csv` — no Docker or network access needed. The report looks like:

```
DynamoDB write status — 10500/10500 records written successfully

              record_id  written
  customer#1              True
  customer#2              True
  ...
  order#abc-123           True
  order#abc-124           True
```

---

## ⚙️ Configuration

All configuration lives in **`config/config.yml`** — no `.env` file needed.

```yaml
api:
  base_url: "https://dummyjson.com"
  max_retries: 5        # retry attempts before giving up
  backoff_factor: 2     # wait = 2^attempt seconds

processing:
  num_synthetic_records: 10000   # change this to generate more/fewer orders
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

opensearch:
  host: "localhost"
  port: 9200
  use_ssl: false
  verify_certs: false
  timeout: 30
  customer_index: "customer_analytics"
  order_index: "order_analytics"

dynamodb:
  endpoint_url: "http://localhost:8000"
  region_name: "us-east-1"
  access_key_id: "dummy"
  secret_access_key: "dummy"
  analytics_table: "etl_analytics"    # single combined table for all analytics rows
```

Values are loaded lazily via `config/loader.py` using dot-notation:

```python
from config.loader import get
db_host = get("database.host")   # → "localhost"
```

---

## 🗄️ Database Tables (PostgreSQL)

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

## 🔎 OpenSearch Indices

The Pandas branch indexes the transformed analytics tables into OpenSearch with explicit mappings and `dynamic: strict`.

### `customer_analytics` index mapping

| Field                  | OpenSearch type             |
| ---------------------- | --------------------------- |
| `customer_id`          | `integer`                   |
| `full_name`            | `text` + `keyword` subfield |
| `email`                | `keyword`                   |
| `email_domain`         | `keyword`                   |
| `city`                 | `keyword`                   |
| `customer_tenure_days` | `integer`                   |
| `total_orders`         | `integer`                   |
| `total_spent`          | `double`                    |
| `avg_order_value`      | `double`                    |
| `lifetime_value_score` | `double`                    |
| `customer_segment`     | `keyword`                   |

### `order_analytics` index mapping

| Field                    | OpenSearch type |
| ------------------------ | --------------- |
| `order_id`               | `keyword`       |
| `customer_id`            | `integer`       |
| `order_timestamp`        | `date`          |
| `order_date`             | `date`          |
| `order_hour`             | `integer`       |
| `gross_amount`           | `double`        |
| `total_discount_amount`  | `double`        |
| `net_amount`             | `double`        |
| `shipping_cost`          | `double`        |
| `final_amount`           | `double`        |
| `total_items`            | `integer`       |
| `discount_ratio`         | `double`        |
| `order_complexity_score` | `integer`       |
| `dominant_category`      | `keyword`       |
| `payment_method`         | `keyword`       |
| `shipping_provider`      | `keyword`       |

---

## 🧊 Iceberg Tables

The PySpark branch writes both analytics datasets to Iceberg:

- `local.analytics.customer_analytics`
- `local.analytics.order_analytics`

Local warehouse path: `data/iceberg/warehouse`

Each `etl` run replaces the latest contents while keeping Iceberg metadata and snapshots intact.

---

## 📘 DynamoDB Local — Single Combined Table

The PySpark branch writes **both** customer and order analytics rows into a single DynamoDB Local table:

| Setting         | Value          |
| --------------- | -------------- |
| Table name      | `etl_analytics` |
| Partition key   | `record_id` (String) |
| Local endpoint  | `http://localhost:8000` |

### Record ID scheme

All rows share one table. Origin is encoded in the partition key prefix:

| Source            | `record_id` format      | `record_type` value |
| ----------------- | ----------------------- | ------------------- |
| Customer analytics | `customer#<customer_id>` | `"customer"`        |
| Order analytics    | `order#<order_id>`       | `"order"`           |

### Write-status report

After every run, `data/processed/dynamodb_write_status.csv` is written with one row per record:

| Column      | Type    | Description                              |
| ----------- | ------- | ---------------------------------------- |
| `record_id` | string  | Prefixed key (`customer#…` / `order#…`)  |
| `written`   | boolean | `True` = persisted successfully, `False` = failed |

The table is created automatically on first run. Browse it visually at **`http://localhost:8001`** (DynamoDB Admin dashboard).

---

## 🔍 Querying Data

### PostgreSQL

```bash
docker compose exec db psql -U etl_user -d etl_db
```

```sql
SELECT customer_segment, COUNT(*), ROUND(AVG(total_spent)) AS avg_spent
FROM customer_analytics
GROUP BY customer_segment
ORDER BY avg_spent DESC;

SELECT payment_method, COUNT(*) AS orders, ROUND(SUM(final_amount)) AS revenue
FROM order_analytics
GROUP BY payment_method
ORDER BY revenue DESC;
```

### Django shell

```bash
python manage.py shell
```

```python
from pipeline.models import CustomerAnalytics, OrderAnalytics
from django.db.models import Sum

CustomerAnalytics.objects.filter(customer_segment="High").order_by("-total_spent")[:5]
OrderAnalytics.objects.aggregate(total=Sum("final_amount"))
```

### OpenSearch

```bash
# Index health
curl http://localhost:9200/_cat/indices?v

# Document counts
curl http://localhost:9200/customer_analytics/_count?pretty
curl http://localhost:9200/order_analytics/_count?pretty

# Sample documents (first 10)
curl http://localhost:9200/customer_analytics/_search?pretty

# Top 5 customers by spend
curl -X GET 'http://localhost:9200/customer_analytics/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{"size": 5, "sort": [{"total_spent": "desc"}]}'
```

### DynamoDB Local — write-status (no scanning required)

```bash
# Full write-status report
python manage.py etl --view-dynamodb-status

# Only records that failed
python manage.py etl --view-dynamodb-status --failed-only

# First 20 rows
python manage.py etl --view-dynamodb-status --limit 20
```

### DynamoDB Admin dashboard

Open **`http://localhost:8001`** in a browser. The dashboard connects to the local container automatically and lets you browse the `etl_analytics` table, inspect individual items, and purge the table if needed — all without AWS CLI or credentials.

### Iceberg (PySpark SQL)

```bash
python manage.py shell
```

```python
from services.spark_service import build_spark_session

spark = build_spark_session()
spark.sql("SELECT COUNT(*) FROM local.analytics.customer_analytics").show()
spark.sql("""
    SELECT payment_method, SUM(final_amount) AS revenue
    FROM local.analytics.order_analytics
    GROUP BY payment_method
""").show()
spark.stop()
```

---

## 🐳 Docker Reference

```bash
# Start all services (add dynamodb-admin to the original set)
docker compose up -d db opensearch dynamodb-local dynamodb-admin

# Service status
docker compose ps

# Stream logs
docker compose logs -f dynamodb-local
docker compose logs -f dynamodb-admin
docker compose logs -f db
docker compose logs -f opensearch

# Stop (data persists in volumes / bind mount)
docker compose down

# Stop AND wipe all data
docker compose down -v
```

> **Note:** `dynamodb_data/` is a bind mount (not a Docker named volume). It persists on your host at `./dynamodb_data/`. `docker compose down -v` removes named volumes but not the bind mount directory — delete `./dynamodb_data/` manually if you want a full reset.

---

## 📁 Project Layout

```
ecommerce_etl/
├── config/
│   ├── config.yml                   ← all config values
│   └── loader.py                    ← dot-notation config accessor
├── extractor/
│   └── api_extractor.py             ← HTTP + retry/backoff
├── processing/
│   ├── synthesizer.py               ← generic synthetic data generator
│   ├── spark_synthesizer.py         ← Spark synthetic data generator
│   ├── enrichment.py                ← Pandas branch derived fields
│   └── spark_enrichment.py          ← Spark branch derived fields
├── transformation/
│   ├── customer_transformer.py
│   ├── order_transformer.py
│   ├── spark_customer_transformer.py
│   └── spark_order_transformer.py
├── opensearch/
│   ├── client.py                    ← OpenSearch client/config helpers
│   ├── mappings.py                  ← index mappings
│   ├── serialization.py             ← DataFrame → JSON document helpers
│   ├── indexing.py                  ← index creation + bulk indexing flow
│   ├── runner.py                    ← OpenSearch orchestration entry point
│   └── __init__.py
├── dynamodb_local/
│   ├── client.py                    ← boto3 DynamoDB resource (localhost:8000)
│   ├── tables.py                    ← single combined table name + key definition
│   ├── writer.py                    ← table creation, batch write, write-status CSV
│   ├── runner.py                    ← orchestration + view-status CLI entry point
│   └── __init__.py
├── services/
│   ├── db_service.py                ← ORM upsert helpers
│   ├── spark_service.py             ← Spark session + Iceberg catalog config
│   └── iceberg_service.py           ← Iceberg table writers
├── utils/
│   ├── logger.py                    ← shared logger factory
│   ├── paths.py                     ← all path constants
│   └── writer.py                    ← read/write helpers
├── etl_core/                        ← Django project
│   ├── settings.py
│   ├── urls.py
│   └── wsgi.py
├── pipeline/                        ← Django app
│   ├── models.py                    ← CustomerAnalytics, OrderAnalytics
│   ├── admin.py
│   └── management/commands/
│       └── etl.py                   ← all CLI flags live here
├── data/
│   ├── raw/
│   ├── processed/                   ← includes dynamodb_write_status.csv
│   └── iceberg/
├── dynamodb_data/                   ← bind-mounted DynamoDB Local SQLite storage
├── logs/
├── manage.py
├── requirements.txt
├── docker-compose.yml
└── README.md
```

---

## 🛠 Troubleshooting

| Problem | Fix |
| ------- | --- |
| `connection refused` on port 5432 | Run `docker compose up -d` and wait 15 s |
| `ModuleNotFoundError: config` | Activate the venv and run from the project root |
| `FileNotFoundError: customer_analytics.csv` | Run `python manage.py etl` before reading the file |
| API fetch fails (network issue) | Pipeline auto-falls back to fully synthetic data — no action needed |
| `django.db.utils.OperationalError` | Check `config/config.yml` DB credentials match `docker-compose.yml` |
| Spark/Iceberg startup is slow on first run | Spark downloads the Iceberg runtime JAR once into `.ivy2/`; later runs are faster |
| Iceberg write fails before Spark starts | Confirm Java ≥ 17 is installed and Maven Central is reachable |
| `EndpointConnectionError` on port 8000 | Run `docker compose up -d dynamodb-local` and retry |
| DynamoDB Admin dashboard at 8001 is blank | DynamoDB Local must be running first; wait a few seconds and refresh |
| `SQLiteException [14]` in DynamoDB Local logs | The `dynamodb_data/` directory is missing or not writable — run `mkdir -p dynamodb_data && chmod 777 dynamodb_data` then `docker compose down -v && docker compose up -d dynamodb-local` |
| `No write-status file found` on `--view-dynamodb-status` | Run the ETL pipeline at least once with `--pyspark` first |