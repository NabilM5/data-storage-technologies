"""HW2 ELT pipeline: load raw -> build with dbt -> test -> publish.

Interval (date_from, date_to) and scenario are DAG params, not run time.
Publish runs only after all tests pass.
"""
from __future__ import annotations

import hashlib
import os

import pendulum

try:  # Airflow 3
    from airflow.sdk import DAG, Param, get_current_context, task
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.decorators import task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context

from airflow.providers.standard.operators.bash import BashOperator

DBT = "/opt/dbt-venv/bin/dbt"
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DATA_DIR = os.environ.get("HW02_DATA_DIR", "/opt/airflow/data")
LOADER = "/opt/airflow/loader/load_raw.py"
SCENARIOS = ["baseline", "duplicate", "history_overlap", "late_fix", "second_source", "zero_duration"]
DBT_VARS = '{"date_from": "{{ params.date_from }}", "date_to": "{{ params.date_to }}"}'


def dbt_cmd(cmd: str) -> str:
    """dbt command for the interval."""
    return f"{DBT} {cmd} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --vars '{DBT_VARS}'"


def _connect():
    import psycopg2

    return psycopg2.connect(
        host=os.environ["DWH_HOST"], port=int(os.environ["DWH_PORT"]),
        dbname=os.environ["DWH_DB"], user=os.environ["DWH_USER"],
        password=os.environ["DWH_PASSWORD"],
    )


with DAG(
    dag_id="hw02_citibike_elt",
    description="Citi Bike JC: raw -> dbt (stg, dds, mart_candidate) -> tests -> publish",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,          # no overlapping runs
    params={
        "date_from": Param("2026-08-03", type="string", format="date", description="First day of the interval (local NY date of started_at)"),
        "date_to": Param("2026-08-09", type="string", format="date", description="Last day of the interval, inclusive"),
        "scenario": Param("baseline", type="string", enum=SCENARIOS, description="baseline or one labelled test input"),
    },
    tags=["hw02", "dbt", "citibike"],
) as dag:

    @task
    def check_input():
        """Slice file exists and has rows in the interval."""
        import csv

        p = get_current_context()["params"]
        date_from, date_to = p["date_from"], p["date_to"]
        if date_to < date_from:
            raise ValueError(f"date_to {date_to} < date_from {date_from}")
        path = os.path.join(DATA_DIR, "slice", "trips_2026-08-03_2026-08-09.csv")
        with open(path, newline="") as f:
            rows = [r for r in csv.DictReader(f) if date_from <= r["started_at"][:10] <= date_to]
        if not rows:
            raise ValueError(f"no trips between {date_from} and {date_to} in {path}")
        print(f"interval {date_from}..{date_to}: {len(rows)} trips in the slice; scenario={p['scenario']}")

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=f"python {LOADER} run --scenario {{{{ params.scenario }}}}",
        env={**os.environ, "HW02_DATA_DIR": DATA_DIR},
    )

    dbt_build_candidate = BashOperator(
        task_id="dbt_build_candidate",
        bash_command=dbt_cmd("run"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd("test"),
    )

    @task
    def publish():
        """Copy the tested interval into mart in one transaction."""
        ctx = get_current_context()
        p, run_id = ctx["params"], ctx["run_id"]
        date_from, date_to = p["date_from"], p["date_to"]
        cols = ("row_key, calendar_date, is_weekend, source_system, station_id, station_name, "
                "area, member_casual, trips, electric_trips, total_duration_sec, avg_duration_sec")
        with _connect() as conn, conn.cursor() as cur:
            cur.execute("select pg_advisory_xact_lock(4202)")   # one publisher at a time
            cur.execute(
                f"select coalesce(string_agg(row_key || '|' || trips || '|' || electric_trips || '|' "
                f"|| total_duration_sec, ',' order by row_key), '') from mart_candidate.daily_station_trips "
                f"where calendar_date between %s and %s", (date_from, date_to))
            checksum = hashlib.md5(cur.fetchone()[0].encode()).hexdigest()
            cur.execute("delete from mart.daily_station_trips where calendar_date between %s and %s",
                        (date_from, date_to))
            removed = cur.rowcount
            cur.execute(
                f"insert into mart.daily_station_trips ({cols}, run_id, published_at) "
                f"select {cols}, %s, now() from mart_candidate.daily_station_trips "
                f"where calendar_date between %s and %s", (run_id, date_from, date_to))
            inserted = cur.rowcount
            cur.execute(
                "insert into ops.publications (run_id, scenario, date_from, date_to, rows_published, checksum) "
                "values (%s, %s, %s, %s, %s, %s)", (run_id, p["scenario"], date_from, date_to, inserted, checksum))
            conn.commit()
        print(f"published {inserted} rows for {date_from}..{date_to} (replaced {removed}); "
              f"checksum={checksum}; run_id={run_id}")

    check_input() >> load_raw >> dbt_build_candidate >> dbt_test >> publish()
