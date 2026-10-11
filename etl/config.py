"""Project paths and settings shared by the ETL steps."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
SQL_DIR = PROJECT_ROOT / "sql"

# Airflow connection id for the target PostgreSQL database
# (defined by the AIRFLOW_CONN_ECOMMERCE_DW environment variable).
POSTGRES_CONN_ID = "ecommerce_dw"

# Olist source files used by the pipeline.
SOURCE_FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "customers": "olist_customers_dataset.csv",
}
