"""Load CSV files into the raw layer.

    python3 loader/load_raw.py run --scenario baseline
    python3 loader/load_raw.py file --kind trips --source-system citibike_jc <path>

A batch is (source_system, source_file); reloading a file replaces its batch,
so reruns do not duplicate. Connection comes from the DWH_* env vars.
"""
import argparse
import csv
import os
import uuid

import psycopg2
from psycopg2.extras import execute_values

DATA_DIR = os.environ.get("HW02_DATA_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"))

TRIP_COLS = ["ride_id", "rideable_type", "started_at", "ended_at",
             "start_station_name", "start_station_id", "end_station_name",
             "end_station_id", "start_lat", "start_lng", "end_lat", "end_lng",
             "member_casual"]
STATION_COLS = ["station_id", "station_name", "area", "lat", "lng", "valid_from", "valid_to"]
KINDS = {"trips": ("raw.trips", TRIP_COLS), "stations": ("raw.stations", STATION_COLS)}

BASELINE = [
    ("trips", "citibike_jc", "slice/trips_2026-08-03_2026-08-09.csv"),
    ("stations", "citibike_jc", "slice/stations_2026-08.csv"),
]
# scenario -> (kind, source_system, file); scenario files are synthetic
SCENARIOS = {
    "baseline": None,
    "duplicate": ("trips", "citibike_jc", "scenarios/duplicate.csv"),
    "late_fix": ("trips", "citibike_jc", "scenarios/late_fix.csv"),
    "second_source": ("trips", "partner_feed", "scenarios/second_source.csv"),
    "history_overlap": ("stations", "citibike_jc", "scenarios/history_overlap.csv"),
    "zero_duration": ("trips", "citibike_jc", "scenarios/zero_duration.csv"),
}


def connect():
    return psycopg2.connect(
        host=os.environ.get("DWH_HOST", "localhost"),
        port=int(os.environ.get("DWH_PORT", "5433")),
        dbname=os.environ.get("DWH_DB", "dwh"),
        user=os.environ.get("DWH_USER", "dwh"),
        password=os.environ.get("DWH_PASSWORD", "dwh"),
    )


def load_file(cur, kind, source_system, rel_path):
    table, cols = KINDS[kind]
    path = os.path.join(DATA_DIR, rel_path)
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        missing = [c for c in cols if c not in reader.fieldnames]
        if missing:
            raise SystemExit(f"{rel_path}: missing columns {missing}")
        rows = [[r[c] if r[c] != "" else None for c in cols] for r in reader]
    if not rows:
        raise SystemExit(f"{rel_path}: no rows")
    batch_id = str(uuid.uuid4())
    cur.execute(f"DELETE FROM {table} WHERE source_system = %s AND source_file = %s", (source_system, rel_path))
    replaced = cur.rowcount
    execute_values(
        cur,
        f"INSERT INTO {table} ({', '.join(cols)}, source_system, source_file, batch_id) VALUES %s",
        [r + [source_system, rel_path, batch_id] for r in rows],
    )
    print(f"{table}: {len(rows)} rows from {rel_path} (source_system={source_system}, "
          f"batch={batch_id[:8]}, replaced {replaced} rows of the previous batch)")
    return len(rows)


def reset_scenarios(cur):
    for table in ("raw.trips", "raw.stations"):
        cur.execute(f"DELETE FROM {table} WHERE source_file LIKE 'scenarios/%%'")
        if cur.rowcount:
            print(f"{table}: removed {cur.rowcount} scenario rows")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p_run = sub.add_parser("run", help="baseline files + one scenario")
    p_run.add_argument("--scenario", choices=sorted(SCENARIOS), default="baseline")
    p_file = sub.add_parser("file", help="load one CSV as a batch")
    p_file.add_argument("path", help="path relative to the data directory")
    p_file.add_argument("--kind", choices=sorted(KINDS), required=True)
    p_file.add_argument("--source-system", required=True)
    args = ap.parse_args()

    with connect() as conn, conn.cursor() as cur:
        if args.cmd == "file":
            load_file(cur, args.kind, args.source_system, args.path)
        else:
            for kind, system, rel in BASELINE:
                load_file(cur, kind, system, rel)
            reset_scenarios(cur)
            spec = SCENARIOS[args.scenario]
            if spec:
                load_file(cur, *spec)
            print(f"scenario: {args.scenario}")
        conn.commit()


if __name__ == "__main__":
    main()
