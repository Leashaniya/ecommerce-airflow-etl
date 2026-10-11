"""Data quality checks that run after the load. Any failure fails the task."""

from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.config import POSTGRES_CONN_ID


def run_quality_checks(run_date: str, expected_rows: int) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    def scalar(sql: str):
        return hook.get_first(sql, parameters={"d": run_date})[0]

    checks = {
        "row count matches what was loaded":
            scalar("SELECT COUNT(*) FROM fact_orders WHERE purchase_date = %(d)s") == expected_rows,
        "no duplicate order_id":
            scalar("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM fact_orders WHERE purchase_date = %(d)s") == 0,
        "no negative order values":
            scalar("SELECT COUNT(*) FROM fact_orders WHERE purchase_date = %(d)s AND order_value < 0") == 0,
        "delivery is never before purchase":
            scalar("SELECT COUNT(*) FROM fact_orders WHERE purchase_date = %(d)s AND delivery_days < 0") == 0,
        "daily summary exists":
            expected_rows == 0
            or scalar("SELECT COUNT(*) FROM daily_delivery_summary WHERE purchase_date = %(d)s") == 1,
    }

    for name, passed in checks.items():
        print(f"[{'PASS' if passed else 'FAIL'}] {name}")
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise ValueError(f"Data quality checks failed for {run_date}: {failed}")
