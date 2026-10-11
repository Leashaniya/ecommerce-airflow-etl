"""Extract: read the source CSVs and keep only the orders for one day.

Each daily run replays one day of the historical Olist data, as if that
day's orders had just arrived. That makes the pipeline incremental: a run
only touches the orders purchased inside its own time window.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd

from etl.config import RAW_DIR, SOURCE_FILES, STAGING_DIR


def check_source_files() -> list[str]:
    """Fail fast if any source file is missing."""
    missing = [name for name in SOURCE_FILES.values() if not (RAW_DIR / name).exists()]
    if missing:
        raise FileNotFoundError(f"Missing source files in {RAW_DIR}: {missing}")
    return [str(RAW_DIR / name) for name in SOURCE_FILES.values()]


def extract_orders(window_start: datetime, window_end: datetime, run_date: str) -> dict[str, str]:
    """Write this window's orders and their related rows to the staging folder.

    Returns the staging file paths (small strings are safe to pass via XCom;
    the data itself never goes through XCom).
    """
    orders = pd.read_csv(RAW_DIR / SOURCE_FILES["orders"])
    purchase_ts = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce")

    start = pd.Timestamp(window_start).tz_localize(None)
    end = pd.Timestamp(window_end).tz_localize(None)
    orders = orders[(purchase_ts >= start) & (purchase_ts < end)]

    order_ids = set(orders["order_id"])
    items = pd.read_csv(RAW_DIR / SOURCE_FILES["order_items"])
    payments = pd.read_csv(RAW_DIR / SOURCE_FILES["payments"])
    customers = pd.read_csv(RAW_DIR / SOURCE_FILES["customers"])

    items = items[items["order_id"].isin(order_ids)]
    payments = payments[payments["order_id"].isin(order_ids)]
    customers = customers[customers["customer_id"].isin(set(orders["customer_id"]))]

    out_dir = STAGING_DIR / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, df in [("orders", orders), ("order_items", items), ("payments", payments), ("customers", customers)]:
        path = out_dir / f"{name}.csv"
        df.to_csv(path, index=False)
        paths[name] = str(path)

    print(f"Extracted {len(orders)} orders for {start:%Y-%m-%d} -> {out_dir}")
    return paths


def read_staged(paths: dict[str, str]) -> dict[str, pd.DataFrame]:
    return {name: pd.read_csv(Path(path)) for name, path in paths.items()}
