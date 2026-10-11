"""Transform: clean the staged data and build one row per order.

Steps:
1. Fix types (text timestamps -> real timestamps).
2. Reject rows that can't be used (missing keys) and save them with a reason.
3. Remove duplicate orders.
4. Summarise order items and payments to one row per order.
5. Join customers, items and payments onto the orders.
6. Calculate the delivery metrics (delivery days, late or on time).
"""

from pathlib import Path

import pandas as pd

from etl.config import REJECTED_DIR, STAGING_DIR
from etl.extract import read_staged

TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def _clean_orders(orders: pd.DataFrame, run_date: str) -> pd.DataFrame:
    for col in TIMESTAMP_COLUMNS:
        orders[col] = pd.to_datetime(orders[col], errors="coerce")

    bad = orders["order_id"].isna() | orders["customer_id"].isna() | orders["order_purchase_timestamp"].isna()
    if bad.any():
        REJECTED_DIR.mkdir(parents=True, exist_ok=True)
        rejected = orders[bad].assign(reject_reason="missing order_id, customer_id or purchase timestamp")
        rejected.to_csv(REJECTED_DIR / f"orders_{run_date}.csv", index=False)
        print(f"Rejected {bad.sum()} orders -> {REJECTED_DIR}")

    return orders[~bad].drop_duplicates(subset="order_id", keep="last")


def _summarise_items(items: pd.DataFrame) -> pd.DataFrame:
    return items.groupby("order_id").agg(
        item_count=("order_item_id", "count"),
        items_value=("price", "sum"),
        freight_value=("freight_value", "sum"),
    ).reset_index()


def _summarise_payments(payments: pd.DataFrame) -> pd.DataFrame:
    totals = payments.groupby("order_id")["payment_value"].sum().rename("payment_value")
    # The main payment type is the one that paid the most for the order.
    main_type = (
        payments.sort_values("payment_value", ascending=False)
        .drop_duplicates("order_id")
        .set_index("order_id")["payment_type"]
    )
    return pd.concat([totals, main_type], axis=1).reset_index()


def _add_delivery_metrics(df: pd.DataFrame) -> pd.DataFrame:
    delivered = df["order_delivered_customer_date"]
    df["delivery_days"] = (delivered - df["order_purchase_timestamp"]).dt.days
    df["estimated_days"] = (df["order_estimated_delivery_date"] - df["order_purchase_timestamp"]).dt.days

    # is_late stays empty for orders that haven't been delivered yet.
    df["is_late"] = (delivered > df["order_estimated_delivery_date"]).where(delivered.notna())
    df["delivery_status"] = "not_delivered"
    df.loc[df["is_late"] == False, "delivery_status"] = "on_time"  # noqa: E712
    df.loc[df["is_late"] == True, "delivery_status"] = "late"  # noqa: E712
    return df


def transform_orders(paths: dict[str, str], run_date: str) -> str:
    data = read_staged(paths)
    orders = _clean_orders(data["orders"], run_date)

    customers = data["customers"][["customer_id", "customer_city", "customer_state"]].drop_duplicates("customer_id")
    customers["customer_city"] = customers["customer_city"].str.strip().str.title()
    customers["customer_state"] = customers["customer_state"].str.strip().str.upper()

    df = (
        orders.merge(customers, on="customer_id", how="left")
        .merge(_summarise_items(data["order_items"]), on="order_id", how="left")
        .merge(_summarise_payments(data["payments"]), on="order_id", how="left")
    )

    # Orders with no items or payments recorded count as zero, not missing.
    df[["item_count", "items_value", "freight_value", "payment_value"]] = (
        df[["item_count", "items_value", "freight_value", "payment_value"]].fillna(0)
    )
    df["order_value"] = df["items_value"] + df["freight_value"]
    df = _add_delivery_metrics(df)

    out = df.rename(columns={
        "order_purchase_timestamp": "purchase_ts",
        "order_approved_at": "approved_ts",
        "order_delivered_carrier_date": "delivered_carrier_ts",
        "order_delivered_customer_date": "delivered_customer_ts",
        "order_estimated_delivery_date": "estimated_delivery_ts",
    })
    out["purchase_date"] = out["purchase_ts"].dt.date

    path = Path(STAGING_DIR) / run_date / "fact_orders.csv"
    out.to_csv(path, index=False)
    print(f"Transformed {len(out)} orders -> {path}")
    return str(path)
