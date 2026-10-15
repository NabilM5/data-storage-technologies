#!/usr/bin/env python3
"""Measure PostgreSQL, two ClickHouse sort orders, and the mart."""

import csv
import io
import re
import statistics
import uuid

from common import PROJECT, clickhouse, command, postgres, read_sql, save_evidence


def pg_query(name):
    return read_sql(f"sql/postgres/{name}.sql")


def ch_query(name, table):
    return read_sql(f"sql/clickhouse/{name}.sql").replace("{{TABLE}}", table)


def mart_query(name):
    query_number = name.split("_")[0]
    return read_sql(f"sql/clickhouse/{query_number}_mart.sql")


def pg_plan(sql):
    return postgres(f"EXPLAIN (ANALYZE, BUFFERS) {sql};")


def pg_metrics(plan):
    time_match = re.search(r"Execution Time: ([0-9.]+) ms", plan)
    buffer_match = re.search(r"Buffers: shared hit=(\d+)(?: read=(\d+))?", plan)
    if not time_match:
        raise RuntimeError("PostgreSQL execution time was not found")
    hit = int(buffer_match.group(1)) if buffer_match else 0
    read = int(buffer_match.group(2) or 0) if buffer_match else 0
    return float(time_match.group(1)), hit, read


def measure_postgres(name):
    sql = pg_query(name)
    pg_plan(sql)
    plans = [pg_plan(sql) for _ in range(5)]
    metrics = [pg_metrics(plan) for plan in plans]
    return {
        "system": "postgres",
        "variant": "btree_index",
        "query": name,
        "times": [item[0] for item in metrics],
        "read_rows": [],
        "read_bytes": [],
        "memory": [],
        "buffers_hit": [item[1] for item in metrics],
        "buffers_read": [item[2] for item in metrics],
        "plan": plans[0],
    }


def run_clickhouse(sql):
    query_id = f"hw4_{uuid.uuid4().hex}"
    args = [
        "docker", "compose", "exec", "-T", "clickhouse",
        "clickhouse-client", "--user", "dwh", "--password", "dwh",
        "--database", "dwh", "--query_id", query_id,
        "--max_threads", "2", "--log_queries", "1",
        "--query", sql + " FORMAT Null",
    ]
    command(args)
    clickhouse("SYSTEM FLUSH LOGS")
    metrics = clickhouse(f"""
        SELECT query_duration_ms, read_rows, read_bytes, memory_usage
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        FORMAT TSV
    """).strip()
    if not metrics:
        raise RuntimeError(f"ClickHouse metrics not found for {query_id}")
    duration, rows, bytes_read, memory = metrics.split("\t")
    return float(duration), int(rows), int(bytes_read), int(memory)


def measure_clickhouse(name, variant, sql):
    run_clickhouse(sql)
    metrics = [run_clickhouse(sql) for _ in range(5)]
    plan = clickhouse(f"EXPLAIN indexes = 1 {sql} FORMAT TSVRaw")
    return {
        "system": "clickhouse",
        "variant": variant,
        "query": name,
        "times": [item[0] for item in metrics],
        "read_rows": [item[1] for item in metrics],
        "read_bytes": [item[2] for item in metrics],
        "memory": [item[3] for item in metrics],
        "buffers_hit": [],
        "buffers_read": [],
        "plan": plan,
    }


def values(items):
    return ";".join(str(item) for item in items)


def main():
    postgres("ANALYZE hw4.trips;")

    rows = []
    for name in ("q1_narrow", "q2_wide"):
        rows.append(measure_postgres(name))
        rows.append(measure_clickhouse(
            name, "station_time_order", ch_query(name, "trips_by_station")
        ))
        rows.append(measure_clickhouse(
            name, "trip_key_order", ch_query(name, "trips_by_key")
        ))
        rows.append(measure_clickhouse(name, "incremental_mart", mart_query(name)))

    pg_settings = postgres("""
        SHOW shared_buffers;
        SHOW max_parallel_workers_per_gather;
        SHOW work_mem;
        SHOW jit;
        SELECT version();
    """)
    ch_settings = clickhouse("""
        SELECT name, value
        FROM system.settings
        WHERE name IN ('max_threads', 'use_query_cache')
        ORDER BY name
        FORMAT TSV;
        SELECT version() FORMAT TSV;
    """)
    background = clickhouse("""
        SELECT table, count() AS active_parts, sum(rows), sum(bytes_on_disk)
        FROM system.parts
        WHERE active AND database = 'dwh'
        GROUP BY table
        ORDER BY table
        FORMAT TSV;
        SELECT count() AS running_merges FROM system.merges FORMAT TSV;
    """)

    csv_output = io.StringIO()
    writer = csv.writer(csv_output)
    writer.writerow([
        "system", "variant", "query", "times_ms", "median_ms", "min_ms",
        "max_ms", "read_rows", "read_bytes", "memory_usage",
        "shared_hit_blocks", "shared_read_blocks",
    ])

    plans = []
    for row in rows:
        times = row["times"]
        writer.writerow([
            row["system"], row["variant"], row["query"], values(times),
            f"{statistics.median(times):.3f}", f"{min(times):.3f}",
            f"{max(times):.3f}", values(row["read_rows"]),
            values(row["read_bytes"]), values(row["memory"]),
            values(row["buffers_hit"]), values(row["buffers_read"]),
        ])
        plans.append(
            f"\n{'=' * 72}\n{row['system']} / {row['variant']} / {row['query']}\n"
            f"{'=' * 72}\n{row['plan']}"
        )

    (PROJECT / "evidence").mkdir(exist_ok=True)
    (PROJECT / "evidence" / "measurements.csv").write_text(csv_output.getvalue())
    save_evidence(
        "plans.txt",
        "HW4 QUERY PLANS\n" + "".join(plans),
        show=False,
    )
    save_evidence(
        "benchmark_environment.txt",
        "HW4 BENCHMARK ENVIRONMENT\n\n"
        "Meters: PostgreSQL Execution Time; ClickHouse query_duration_ms.\n"
        "Method: one discarded warm-up followed by five measured runs.\n"
        "ClickHouse max_threads was fixed to 2 for measured queries.\n\n"
        f"PostgreSQL settings:\n{pg_settings}\n"
        f"ClickHouse settings:\n{ch_settings}\n"
        f"ClickHouse parts and merges after measurement:\n{background}\n",
    )
    print(csv_output.getvalue())


if __name__ == "__main__":
    main()
