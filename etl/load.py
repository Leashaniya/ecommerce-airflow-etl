"""Load: write the transformed orders into PostgreSQL, then build the daily summary.

The load is idempotent: it deletes that day's rows and inserts them again in
one transaction. Re-running the same day (a retry or a backfill) gives the
same result instead of duplicating rows.
"""

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.config import POSTGRES_CONN_ID, SQL_DIR

FACT_COLUMNS = [
    "order_id", "customer_id", "customer_city", "customer_state", "order_status",
    "purchase_ts", "purchase_date", "approved_ts", "delivered_carrier_ts",
    "delivered_customer_ts", "estimated_delivery_ts", "item_count", "items_value",
    "freight_value", "order_value", "payment_value", "payment_type",
    "delivery_days", "estimated_days", "is_late", "delivery_status",
]


def _hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def create_tables() -> None:
    _hook().run((SQL_DIR / "schema.sql").read_text())


def load_fact_orders(fact_path: str, run_date: str) -> int:
    df = pd.read_csv(fact_path)[FACT_COLUMNS]
    for col in ["item_count", "delivery_days", "estimated_days"]:
        df[col] = df[col].astype("Int64")
    df["is_late"] = df["is_late"].astype("boolean")
    # Convert pandas missing values (NaN / NA) into SQL NULLs.
    df = df.astype(object).where(df.notna(), None)
    rows = [tuple(r) for r in df.itertuples(index=False)]

    conn = _hook().get_conn()
    try:
        with conn, conn.cursor() as cur:  # one transaction: commit on success, roll back on error
            cur.execute("DELETE FROM fact_orders WHERE purchase_date = %s", (run_date,))
            deleted = cur.rowcount
            if rows:
                placeholders = ", ".join(["%s"] * len(FACT_COLUMNS))
                cur.executemany(
                    f"INSERT INTO fact_orders ({', '.join(FACT_COLUMNS)}) VALUES ({placeholders})",
                    rows,
                )
    finally:
        conn.close()

    print(f"Loaded {len(rows)} orders for {run_date} (replaced {deleted} existing rows)")
    return len(rows)


def build_daily_summary(run_date: str) -> None:
    _hook().run(
        """
        INSERT INTO daily_delivery_summary AS s (
            purchase_date, total_orders, total_revenue, avg_order_value,
            delivered_orders, late_orders, late_pct, avg_delivery_days, updated_at
        )
        SELECT
            purchase_date,
            COUNT(*),
            SUM(order_value),
            ROUND(AVG(order_value), 2),
            COUNT(*) FILTER (WHERE delivery_status <> 'not_delivered'),
            COUNT(*) FILTER (WHERE is_late),
            ROUND(100.0 * COUNT(*) FILTER (WHERE is_late)
                  / NULLIF(COUNT(*) FILTER (WHERE delivery_status <> 'not_delivered'), 0), 2),
            ROUND(AVG(delivery_days), 2),
            now()
        FROM fact_orders
        WHERE purchase_date = %(run_date)s
        GROUP BY purchase_date
        ON CONFLICT (purchase_date) DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            total_revenue = EXCLUDED.total_revenue,
            avg_order_value = EXCLUDED.avg_order_value,
            delivered_orders = EXCLUDED.delivered_orders,
            late_orders = EXCLUDED.late_orders,
            late_pct = EXCLUDED.late_pct,
            avg_delivery_days = EXCLUDED.avg_delivery_days,
            updated_at = now();
        """,
        parameters={"run_date": run_date},
    )
