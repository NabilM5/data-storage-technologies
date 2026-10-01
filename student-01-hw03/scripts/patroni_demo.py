#!/usr/bin/env python3
"""Prove Patroni roles, replication, switchover and replica read-only mode."""

from datetime import datetime, timezone
import json
import time

from common import PROJECT, command, write_evidence


NODES = ("patroni1", "patroni2")
SLICE = PROJECT.parent / "student-01-hw02" / "data" / "slice" / "trips_2026-08-03_2026-08-09.csv"
COLUMNS = (
    "ride_id, rideable_type, started_at, ended_at, start_station_name, "
    "start_station_id, end_station_name, end_station_id, start_lat, "
    "start_lng, end_lat, end_lng, member_casual"
)


def node_psql(node, sql, database="postgres", check=True, plain=False):
    args = ["docker", "compose", "exec", "-T", "-e", "PGPASSWORD=postgres",
            node, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
            "-U", "postgres", "-d", database]
    if plain:
        args.extend(["-qAt"])
    return command(args, data=sql, check=check)


def status(node):
    result = command([
        "docker", "compose", "exec", "-T", node,
        "curl", "-fsS", "http://localhost:8008/patroni"
    ])
    return json.loads(result.stdout)


def roles():
    values = {node: status(node)["role"] for node in NODES}
    leader = next(node for node, role in values.items() if role in ("primary", "master"))
    replica = next(node for node, role in values.items() if role == "replica")
    return leader, replica, values


def patronictl_list(node="patroni1"):
    return command([
        "docker", "compose", "exec", "-T", node,
        "patronictl", "-c", "/etc/patroni/patroni.yml", "list"
    ]).stdout


def wait_for_roles(old_leader, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            leader, replica, values = roles()
            if leader != old_leader:
                return leader, replica, values
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError("Patroni roles did not change before the timeout")


def wait_for_marker(node, marker, timeout=30):
    deadline = time.time() + timeout
    sql = f"SELECT count(*) FROM ops.replication_marker WHERE marker = '{marker}';"
    while time.time() < deadline:
        result = node_psql(node, sql, database="dwh", plain=True, check=False)
        if result.returncode == 0 and result.stdout.strip() == "1":
            return
        time.sleep(1)
    raise RuntimeError(f"marker {marker} did not reach {node}")


def load_control_slice(leader):
    create_database = r"""
SELECT 'CREATE ROLE dwh LOGIN PASSWORD ''dwh'' CREATEDB'
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dwh')\gexec
SELECT 'CREATE DATABASE dwh OWNER dwh'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'dwh')\gexec
"""
    node_psql(leader, create_database)

    ddl = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS raw.trip_import (
    ride_id text, rideable_type text, started_at text, ended_at text,
    start_station_name text, start_station_id text, end_station_name text,
    end_station_id text, start_lat text, start_lng text, end_lat text,
    end_lng text, member_casual text
);
CREATE TABLE IF NOT EXISTS dds.fact_trip (
    trip_key text PRIMARY KEY, source_system text NOT NULL, ride_id text NOT NULL,
    start_station_id text, start_station_name text, end_station_id text,
    rideable_type text NOT NULL, member_casual text NOT NULL,
    started_at timestamp NOT NULL, ended_at timestamp NOT NULL,
    duration_sec integer NOT NULL CHECK (duration_sec >= 0)
);
CREATE TABLE IF NOT EXISTS mart.daily_station_trips (
    row_key text PRIMARY KEY, calendar_date date NOT NULL, is_weekend boolean NOT NULL,
    source_system text NOT NULL, station_id text NOT NULL, station_name text NOT NULL,
    area text NOT NULL, member_casual text NOT NULL, trips integer NOT NULL,
    electric_trips integer NOT NULL, total_duration_sec bigint NOT NULL,
    avg_duration_sec numeric(10,1) NOT NULL, run_id text NOT NULL,
    published_at timestamptz NOT NULL
);
CREATE TABLE IF NOT EXISTS ops.replication_marker (
    marker text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);
TRUNCATE mart.daily_station_trips, dds.fact_trip, raw.trip_import;
"""
    node_psql(leader, ddl, database="dwh")

    copy_sql = f"\\copy raw.trip_import ({COLUMNS}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    args = ["docker", "compose", "exec", "-T", "-e", "PGPASSWORD=postgres",
            leader, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
            "-U", "postgres", "-d", "dwh", "-c", copy_sql]
    command(args, data=SLICE.read_bytes())

    build = """
INSERT INTO dds.fact_trip
SELECT 'citibike_jc:' || ride_id, 'citibike_jc', ride_id,
       start_station_id, start_station_name, end_station_id,
       rideable_type, member_casual, started_at::timestamp, ended_at::timestamp,
       floor(extract(epoch FROM (ended_at::timestamp - started_at::timestamp)))::integer
FROM raw.trip_import;

INSERT INTO mart.daily_station_trips
SELECT started_at::date::text || '|citibike_jc|' || start_station_id || '|' || member_casual,
       started_at::date, extract(isodow FROM started_at) IN (6, 7), 'citibike_jc',
       start_station_id, max(start_station_name),
       CASE WHEN start_station_id LIKE 'JC%' THEN 'Jersey City'
            WHEN start_station_id LIKE 'HB%' THEN 'Hoboken' ELSE 'Other' END,
       member_casual, count(*)::integer,
       count(*) FILTER (WHERE rideable_type = 'electric_bike')::integer,
       sum(duration_sec)::bigint, round(sum(duration_sec)::numeric / count(*), 1),
       'hw03-patroni', now()
FROM dds.fact_trip
GROUP BY started_at::date, extract(isodow FROM started_at),
         start_station_id, member_casual;
"""
    node_psql(leader, build, database="dwh")


def main():
    leader, replica, before_roles = roles()
    before_list = patronictl_list()
    load_control_slice(leader)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    old_marker = f"before_switchover_{stamp}"
    node_psql(leader, f"INSERT INTO ops.replication_marker(marker) VALUES ('{old_marker}');", database="dwh")
    wait_for_marker(replica, old_marker)

    before_sql = ""
    for node in NODES:
        before_sql += f"{node}: " + node_psql(
            node,
            "SELECT pg_is_in_recovery(), count(*) FROM ops.replication_marker;",
            database="dwh", plain=True
        ).stdout

    switch = command([
        "docker", "compose", "exec", "-T", leader,
        "patronictl", "-c", "/etc/patroni/patroni.yml", "switchover", "hw03",
        "--leader", leader, "--candidate", replica, "--force"
    ])
    new_leader, new_replica, after_roles = wait_for_roles(leader)
    after_list = patronictl_list()

    wait_for_marker(new_leader, old_marker)
    new_marker = f"after_switchover_{stamp}"
    node_psql(new_leader, f"INSERT INTO ops.replication_marker(marker) VALUES ('{new_marker}');", database="dwh")
    wait_for_marker(new_replica, new_marker)

    marker_rows = node_psql(new_leader, """
SELECT marker FROM ops.replication_marker
WHERE marker LIKE '%switchover_%'
ORDER BY created_at DESC LIMIT 2;
""", database="dwh").stdout

    rejected_marker = f"rejected_on_replica_{stamp}"
    rejected = node_psql(
        new_replica,
        f"INSERT INTO ops.replication_marker(marker) VALUES ('{rejected_marker}');",
        database="dwh", check=False
    )
    rejected_count = node_psql(
        new_leader,
        f"SELECT count(*) FROM ops.replication_marker WHERE marker = '{rejected_marker}';",
        database="dwh", plain=True
    ).stdout.strip()

    control = node_psql(new_leader, """
SELECT count(*) AS fact_rows FROM dds.fact_trip;
SELECT count(*) AS mart_rows, sum(trips) AS trips,
       sum(total_duration_sec) AS total_duration_sec
FROM mart.daily_station_trips;
""", database="dwh").stdout

    text = (
        "HW3 PATRONI EVIDENCE\n\n"
        f"Roles before (REST API): {before_roles}\n"
        "patronictl list before:\n" + before_list + "\n"
        f"SQL roles and marker count before:\n{before_sql}\n"
        f"First marker replicated: {old_marker}\n\n"
        "Switchover command output:\n" + switch.stdout + switch.stderr + "\n"
        f"Roles after (REST API): {after_roles}\n"
        "patronictl list after:\n" + after_list + "\n"
        f"Old and new markers visible on new leader {new_leader}:\n{marker_rows}\n"
        f"New marker replicated to current replica {new_replica}: {new_marker}\n\n"
        f"Write attempted on current replica {new_replica}; exit code: {rejected.returncode}\n"
        "Replica write error:\n" + rejected.stdout + rejected.stderr + "\n"
        f"Rejected marker count on leader (must be 0): {rejected_count}\n\n"
        "Control slice after switchover:\n" + control
    )
    write_evidence("patroni.txt", text)


if __name__ == "__main__":
    main()
