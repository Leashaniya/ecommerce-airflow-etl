-- Target tables in the ecommerce_dw PostgreSQL database.

-- One row per order, with customer, item, payment and delivery details.
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id              TEXT PRIMARY KEY,
    customer_id           TEXT NOT NULL,
    customer_city         TEXT,
    customer_state        TEXT,
    order_status          TEXT,
    purchase_ts           TIMESTAMP NOT NULL,
    purchase_date         DATE NOT NULL,
    approved_ts           TIMESTAMP,
    delivered_carrier_ts  TIMESTAMP,
    delivered_customer_ts TIMESTAMP,
    estimated_delivery_ts TIMESTAMP,
    item_count            INTEGER,
    items_value           NUMERIC(12, 2),
    freight_value         NUMERIC(12, 2),
    order_value           NUMERIC(12, 2),
    payment_value         NUMERIC(12, 2),
    payment_type          TEXT,
    delivery_days         INTEGER,
    estimated_days        INTEGER,
    is_late               BOOLEAN,
    delivery_status       TEXT,
    loaded_at             TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_purchase_date ON fact_orders (purchase_date);

-- One row per day: the numbers an analyst looks at every morning.
CREATE TABLE IF NOT EXISTS daily_delivery_summary (
    purchase_date      DATE PRIMARY KEY,
    total_orders       INTEGER,
    total_revenue      NUMERIC(14, 2),
    avg_order_value    NUMERIC(12, 2),
    delivered_orders   INTEGER,
    late_orders        INTEGER,
    late_pct           NUMERIC(5, 2),
    avg_delivery_days  NUMERIC(6, 2),
    updated_at         TIMESTAMP NOT NULL DEFAULT now()
);
