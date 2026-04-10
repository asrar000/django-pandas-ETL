# 🛒 Generic E-Commerce ETL Pipeline

A production-structured Django ETL project with two parallel analytics paths from the same extracted seed data:

- a Pandas path that writes processed CSVs, upserts PostgreSQL through Django ORM, and indexes OpenSearch
- a PySpark path that mirrors the same synthesize, enrich, and transform logic and writes Iceberg tables

---

## 📐 Architecture Overview

```
pipeline/management/commands/etl.py  ← single entry point
│
├── extractor/           ← Step 1 : shared HTTP extraction with retries/back-off
├── processing/          ← Pandas + PySpark synthesis and enrichment logic
├── transformation/      ← Pandas + PySpark analytics builders
├── opensearch/          ← OpenSearch client · mappings · serialization · indexing
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
│   ├── processed/       ← customer_analytics.csv & order_analytics.csv
│   └── iceberg/         ← local Iceberg warehouse files
└── logs/                ← etl_pipeline.log
```

---

## 📦 Prerequisites

| Tool                    | Version            |
| ----------------------- | ------------------ |
| Python                  | 3.10 or 3.11       |
| Java                    | ≥ 17 (PySpark / Iceberg branch only) |
| Docker + Docker Compose | any recent version |
| pip                     | any recent version |

Java is only required when running the PySpark → Iceberg path.
Docker is required for the local PostgreSQL and OpenSearch services.

---

## 🚀 Quick Start

### Step 0 — Install dependencies & start PostgreSQL + OpenSearch

```bash
# Create & activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install Python packages
pip install -r requirements.txt

# Start PostgreSQL + OpenSearch containers (runs in background)
docker compose up -d db opensearch

# Wait ~10 s for Postgres to be healthy, then run migrations
python manage.py makemigrations
python manage.py migrate
```

OpenSearch is exposed locally on `http://localhost:9200`, so you can manage it
through plain `docker compose` and REST calls without sudo.
If the OpenSearch container exits immediately on Ubuntu with a
`vm.max_map_count` bootstrap error, that host-level setting must be adjusted
once by an administrator.

---

### Command 1 — Run the full ETL, PostgreSQL load, OpenSearch indexing, and Iceberg write

```bash
python manage.py etl                    # Run both branches
python manage.py etl --pandas           # Run only Pandas → PostgreSQL + OpenSearch branch
python manage.py etl --pyspark          # Run only PySpark → Iceberg branch
```

**What this does:**

1. Fetches raw carts & users from `https://dummyjson.com` (with retry + back-off)
2. Launches the Pandas branch for synthesize → enrich → transform → CSV → PostgreSQL → OpenSearch (if --pandas or no flag)
3. Launches the parallel PySpark branch for synthesize → enrich → transform → Iceberg (if --pyspark or no flag)
4. Writes `data/processed/customer_analytics.csv` and `data/processed/order_analytics.csv` (Pandas branch)
5. Upserts both analytics datasets into PostgreSQL using Django ORM (Pandas branch)
6. Creates the OpenSearch indices `customer_analytics` and `order_analytics` if needed and bulk-indexes the transformed analytics documents (Pandas branch)
7. Creates or replaces the Iceberg tables `local.analytics.customer_analytics` and `local.analytics.order_analytics` (PySpark branch)

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

opensearch:
  host: "localhost"
  port: 9200
  use_ssl: false
  verify_certs: false
  timeout: 30
  recreate_indexes: true
  customer_index: "customer_analytics"
  order_index: "order_analytics"
```

The Spark branch writes to a local Hadoop Iceberg catalog rooted at `data/iceberg/warehouse`.
The Pandas branch creates the OpenSearch indices when they are missing and then
bulk-loads the transformed analytics documents into them.

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

## 🔎 OpenSearch Indices

The Pandas branch indexes the transformed analytics tables into OpenSearch with
explicit mappings and `dynamic: strict` so only the expected schema is stored.

### `customer_analytics` index mapping

| Field                  | OpenSearch type |
| ---------------------- | --------------- |
| `customer_id`          | `integer`       |
| `full_name`            | `text` + `keyword` subfield |
| `email`                | `keyword`       |
| `email_domain`         | `keyword`       |
| `city`                 | `keyword`       |
| `customer_tenure_days` | `integer`       |
| `total_orders`         | `integer`       |
| `total_spent`          | `double`        |
| `avg_order_value`      | `double`        |
| `lifetime_value_score` | `double`        |
| `customer_segment`     | `keyword`       |

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

The PySpark branch writes the same analytics datasets to Iceberg tables:

- `local.analytics.customer_analytics`
- `local.analytics.order_analytics`

The local warehouse path is:

- `data/iceberg/warehouse`

Each `etl` run replaces the latest Iceberg table contents while keeping Iceberg table metadata and snapshots managed by the table format.

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

To view the PostgreSQL tables using Pandas:

```bash
jupyter notebook pandas_viewer.ipynb
```

This notebook connects to PostgreSQL and displays sample data and counts for the analytics tables.

To inspect the OpenSearch indices and mappings:

```bash
curl http://localhost:9200/_cat/indices?v
curl http://localhost:9200/customer_analytics/_mapping?pretty
curl http://localhost:9200/order_analytics/_mapping?pretty
```

To view the indexed documents stored in OpenSearch:

```bash
# Document counts
curl http://localhost:9200/customer_analytics/_count?pretty
curl http://localhost:9200/order_analytics/_count?pretty

# Search returns the first 10 documents by default
curl http://localhost:9200/customer_analytics/_search?pretty
curl 'http://localhost:9200/order_analytics/_search?pretty&size=5'

# Example: top 5 customers by total_spent
curl -X GET 'http://localhost:9200/customer_analytics/_search?pretty' \
  -H 'Content-Type: application/json' \
  -d '{
    "size": 5,
    "sort": [{"total_spent": "desc"}]
  }'
```

The document payload is returned under `hits.hits[*]._source`.

To view the Iceberg tables after ETL:

```bash
jupyter notebook view_iceberg_tables.ipynb
```

This notebook displays table schemas, sample rows, and row counts for the analytics tables.

To view the raw data files using PySpark:

```bash
jupyter notebook raw_viewer.ipynb
```

This notebook loads and displays the 4 raw JSON files with schemas and samples.

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
docker compose up -d db opensearch   # start Postgres + OpenSearch in background
docker compose ps             # check health
docker compose logs -f db     # stream Postgres logs
docker compose logs -f opensearch   # stream OpenSearch logs
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
├── opensearch/
│   ├── client.py             ← OpenSearch client/config helpers
│   ├── mappings.py           ← index mappings
│   ├── serialization.py      ← DataFrame → JSON document helpers
│   ├── indexing.py           ← index creation + bulk indexing flow
│   ├── runner.py             ← OpenSearch orchestration entry point
│   └── __init__.py           ← public OpenSearch package API
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
│       └── etl.py
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
| `FileNotFoundError: customer_analytics.csv`    | Run `python manage.py etl` before the dump command                                          |
| API fetch fails (network issue)                | The pipeline auto-falls back to fully synthetic data — no action needed                              |
| `django.db.utils.OperationalError`             | Check `config/config.yml` DB credentials match `docker-compose.yml`                                  |
| Spark/Iceberg startup is slow on the first run | Spark downloads the Iceberg runtime JAR once into `~/.ivy2`; later runs are faster                   |
| Iceberg write fails before Spark starts        | Make sure Java is installed and the machine can reach Maven Central for the Iceberg package download |
