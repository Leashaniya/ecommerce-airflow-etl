# 🛒 Automated E-Commerce ETL Pipeline with Apache Airflow

An Airflow DAG that runs every day, extracts that day's e-commerce **orders and delivery data**,
cleans and enriches it with Python (pandas), and loads it into **PostgreSQL** for reporting.
The load is idempotent and checked by data quality tests after every run.

**Data:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(Kaggle): 99,441 real orders from Sep 2016 to Oct 2018.

---

## 🗺️ How it works

```
create_tables ──┐
                ├──► extract ──► transform ──► load_fact_orders ──► build_daily_summary ──► quality_checks
check_sources ──┘
```

| Task | What it does |
|---|---|
| `create_tables` | Creates the PostgreSQL tables if they don't exist |
| `check_sources` | Fails fast if a source CSV is missing |
| `extract` | Reads the orders purchased on the run's date, plus their items, payments and customers. Writes them to `data/staging/<date>/` |
| `transform` | Fixes types, rejects rows with missing keys (saved with a reason), removes duplicate orders, joins everything into one row per order, and calculates delivery metrics |
| `load_fact_orders` | Deletes that day's rows and inserts them again in one transaction (idempotent) |
| `build_daily_summary` | Upserts one row of daily KPIs: orders, revenue, late %, average delivery days |
| `quality_checks` | Row count matches, no duplicate `order_id`, no negative values, delivery never before purchase, summary exists |

**Scheduling:** `@daily`, with `catchup=True` between 1 and 7 Aug 2018. The dataset is historical,
so each run replays one day as if those orders had just arrived.
**Reliability:** `retries=2` with a 1-minute delay, `max_active_runs=1` so days run in order,
and only file paths (never data) are passed between tasks via XCom.

## 📊 Results (1–7 Aug 2018)

| | |
|---|---|
| Orders loaded | 2,190 across 7 daily runs |
| Delivered on time / late / not delivered | 1,786 / 341 / 63 |
| Late delivery rate | 11–20% per day |
| Average delivery time | 7.5–9.2 days |
| Re-running a day | Same row count (2,190 before and after), so no duplicates |

## 🧱 Tables (PostgreSQL `ecommerce_dw`)

- **`fact_orders`**: one row per order, with customer city/state, item count, order value,
  payment type and delivery metrics (`delivery_days`, `is_late`, `delivery_status`)
- **`daily_delivery_summary`**: one row per day, the numbers an analyst checks every morning

## 📂 Project structure

```
ecommerce-airflow-etl/
├── dags/ecommerce_orders_etl.py   the Airflow DAG
├── etl/
│   ├── config.py                  paths and settings
│   ├── extract.py                 read one day of data
│   ├── transform.py               clean, join, delivery metrics
│   ├── load.py                    idempotent load + daily summary
│   └── quality.py                 data quality checks
├── sql/schema.sql                 table definitions
├── airflow.env                    environment settings for Airflow
├── data/raw/                      Olist CSVs (download from Kaggle)
└── requirements.txt
```

## ▶️ How to run

Requirements: Python 3.10+ and a local PostgreSQL (this project uses Postgres.app).

```bash
# 1. Install
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.14.txt"

# 2. Data: download the Olist dataset from Kaggle and put these 4 files in data/raw/
#    olist_orders_dataset.csv, olist_order_items_dataset.csv,
#    olist_order_payments_dataset.csv, olist_customers_dataset.csv

# 3. Database
createdb ecommerce_dw

# 4. Airflow
source airflow.env
airflow db migrate
airflow dags test ecommerce_orders_etl 2018-08-01   # run one day
airflow standalone                                  # or start the UI at http://localhost:8080
```

## 🔧 What I'd improve next

- **Late delivery updates:** an order's delivery date arrives days after it is purchased. Today
  each day is loaded once. In production I'd re-process recent days, or load incrementally on an
  `updated_at` watermark or CDC.
- Read from a real source database or cloud storage (S3 / ADLS) instead of local CSVs.
- Move from pandas to Spark if daily volume grows beyond a single machine.
- Failure alerts (email or Slack) and a run log table with row counts.
- Run Airflow in Docker, or on a managed service (MWAA, Astronomer).
