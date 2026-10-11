"""Daily ETL for e-commerce order and delivery data.

Each run processes one day of orders (the run's logical date):

    create_tables ──┐
                    ├──> extract ──> transform ──> load_fact_orders ──> build_daily_summary ──> quality_checks
    check_sources ──┘

The dataset is historical (Olist, 2016-2018), so the schedule replays it one
day at a time, as if each day's orders had just arrived.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task

# Make the project's etl package importable from the dags folder.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl import extract as ex  # noqa: E402
from etl import load as ld  # noqa: E402
from etl import quality as qc  # noqa: E402
from etl import transform as tr  # noqa: E402

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="ecommerce_orders_etl",
    description="Daily ETL of e-commerce orders and deliveries into PostgreSQL",
    schedule="@daily",
    start_date=datetime(2018, 8, 1),
    end_date=datetime(2018, 8, 7),
    catchup=True,  # run every missed day between start_date and end_date
    max_active_runs=1,  # process days one at a time, in order
    default_args=default_args,
    tags=["ecommerce", "etl"],
)
def ecommerce_orders_etl():

    @task
    def create_tables():
        ld.create_tables()

    @task
    def check_sources():
        return ex.check_source_files()

    @task
    def extract(ds=None):
        start = datetime.fromisoformat(ds)
        return ex.extract_orders(start, start + timedelta(days=1), run_date=ds)

    @task
    def transform(paths: dict, ds=None):
        return tr.transform_orders(paths, run_date=ds)

    @task
    def load_fact_orders(fact_path: str, ds=None):
        return ld.load_fact_orders(fact_path, run_date=ds)

    @task
    def build_daily_summary(loaded_rows: int, ds=None):
        ld.build_daily_summary(run_date=ds)
        return loaded_rows

    @task
    def quality_checks(loaded_rows: int, ds=None):
        qc.run_quality_checks(run_date=ds, expected_rows=loaded_rows)

    # Passing a task's return value into the next task also sets the dependency.
    staged = extract()
    [create_tables(), check_sources()] >> staged
    loaded = load_fact_orders(transform(staged))
    quality_checks(build_daily_summary(loaded))


ecommerce_orders_etl()
