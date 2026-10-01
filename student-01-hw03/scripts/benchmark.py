#!/usr/bin/env python3
"""Measure the two queries on the original, indexed and partitioned tables."""

import csv
import io
import re
import statistics
import time

from common import PROJECT, psql, write_evidence


TABLES = {
    "original": "dds.fact_trip",
    "index": "dds.fact_trip",
    "partitioned_index": "hw3.fact_trip_partitioned",
}


def query(name, table):
    text = (PROJECT / "sql" / f"{name}.sql").read_text()
    return text.replace("{{FACT_TABLE}}", table).strip().rstrip(";")


def explain(sql):
    return psql(f"EXPLAIN (ANALYZE, BUFFERS) {sql};").stdout


def execution_time(plan):
    match = re.search(r"Execution Time: ([0-9.]+) ms", plan)
    if not match:
        raise RuntimeError("Execution Time was not found in EXPLAIN output")
    return float(match.group(1))


def measure(name, variant, table):
    sql = query(name, table)
    explain(sql)  # one warm-up
    plans = [explain(sql) for _ in range(5)]
    times = [execution_time(plan) for plan in plans]
    return {
        "query": name,
        "variant": variant,
        "times": times,
        "median": statistics.median(times),
        "plan": plans[0],
    }


def difference_count(name, table):
    sql = query(name, table)
    baseline = f"hw3.{name}_baseline"
    compare = f"""
WITH new_result AS ({sql}),
different AS (
    (TABLE new_result EXCEPT ALL TABLE {baseline})
    UNION ALL
    (TABLE {baseline} EXCEPT ALL TABLE new_result)
)
SELECT count(*) FROM different;
"""
    return int(psql(compare, plain=True).stdout.strip())


def main():
    psql("""
DROP INDEX IF EXISTS dds.idx_fact_trip_station_started;
DROP TABLE IF EXISTS hw3.fact_trip_partitioned CASCADE;
ANALYZE dds.fact_trip;
DROP TABLE IF EXISTS hw3.narrow_baseline;
DROP TABLE IF EXISTS hw3.wide_baseline;
""")

    for name in ("narrow", "wide"):
        psql(f"CREATE TABLE hw3.{name}_baseline AS {query(name, 'dds.fact_trip')};")

    settings = psql("""
SHOW shared_buffers;
SHOW work_mem;
SHOW max_parallel_workers_per_gather;
SHOW jit;
SHOW random_page_cost;
""").stdout

    rows = []
    rows.extend(measure(name, "original", TABLES["original"]) for name in ("narrow", "wide"))

    started = time.perf_counter()
    psql("CREATE INDEX idx_fact_trip_station_started ON dds.fact_trip (start_station_id, started_at);")
    index_build_ms = (time.perf_counter() - started) * 1000
    psql("ANALYZE dds.fact_trip;")
    rows.extend(measure(name, "index", TABLES["index"]) for name in ("narrow", "wide"))

    psql((PROJECT / "sql" / "partitioning.sql").read_text())
    rows.extend(
        measure(name, "partitioned_index", TABLES["partitioned_index"])
        for name in ("narrow", "wide")
    )

    total = int(psql("SELECT count(*) FROM dds.fact_trip;", plain=True).stdout.strip())
    narrow_rows = int(psql("""
SELECT count(*) FROM dds.fact_trip
WHERE start_station_id = 'JC115'
  AND started_at >= '2026-08-05' AND started_at < '2026-08-06';
""", plain=True).stdout.strip())
    wide_rows = int(psql("""
SELECT count(*) FROM dds.fact_trip
WHERE started_at >= '2026-07-31' AND started_at < '2026-09-01';
""", plain=True).stdout.strip())

    index_size = psql("""
SELECT pg_size_pretty(pg_relation_size('dds.idx_fact_trip_station_started'));
""", plain=True).stdout.strip()

    output = io.StringIO()
    output.write("HW3 OPTIMIZATION EVIDENCE\n\n")
    output.write("Measured value: server Execution Time from EXPLAIN ANALYZE.\n")
    output.write("Each row: one discarded warm-up, then five measured runs.\n\n")
    output.write("Execution settings:\n" + settings + "\n")
    output.write(f"Total fact rows: {total}\n")
    output.write(f"Narrow matching rows: {narrow_rows} ({narrow_rows / total:.4%})\n")
    output.write(f"Wide matching rows: {wide_rows} ({wide_rows / total:.4%})\n")
    output.write(f"Index build client time: {index_build_ms:.3f} ms\n")
    output.write(f"Index size: {index_size}\n\n")

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["query", "variant", "matching_rows", "times_ms", "median_ms", "difference_rows"])
    for row in rows:
        matching = narrow_rows if row["query"] == "narrow" else wide_rows
        diff = difference_count(row["query"], TABLES[row["variant"]])
        times_text = ";".join(f"{value:.3f}" for value in row["times"])
        writer.writerow([row["query"], row["variant"], matching,
                         times_text, f"{row['median']:.3f}", diff])
        output.write(
            f"{row['query']} / {row['variant']}\n"
            f"times ms: {times_text}\nmedian ms: {row['median']:.3f}\n"
            f"two-way EXCEPT ALL difference rows: {diff}\n{row['plan']}\n"
        )

    (PROJECT / "evidence").mkdir(exist_ok=True)
    (PROJECT / "evidence" / "optimization_results.csv").write_text(csv_buffer.getvalue())
    write_evidence("optimization.txt", output.getvalue())


if __name__ == "__main__":
    main()

