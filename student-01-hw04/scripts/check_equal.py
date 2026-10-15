#!/usr/bin/env python3
"""Compare every loaded row and every analytical result."""

from hashlib import sha256

from common import clickhouse, postgres, read_sql, save_evidence


PG_CANONICAL = r"""
SELECT concat_ws('|',
    trip_key, source_system, ride_id, rideable_type,
    to_char(started_at, 'YYYY-MM-DD HH24:MI:SS.MS'),
    to_char(ended_at, 'YYYY-MM-DD HH24:MI:SS.MS'),
    start_station_id, start_station_name, end_station_id, end_station_name,
    member_casual, duration_sec::text, ((fare_amount * 100)::bigint)::text
)
FROM hw4.trips
ORDER BY trip_key;
"""

CH_CANONICAL = r"""
SELECT concat(
    trip_key, '|', source_system, '|', ride_id, '|', rideable_type, '|',
    toString(started_at), '|', toString(ended_at), '|',
    start_station_id, '|', start_station_name, '|', end_station_id, '|',
    end_station_name, '|', member_casual, '|', toString(duration_sec), '|',
    toString(toInt64(fare_amount * 100))
)
FROM {table}
ORDER BY trip_key
FORMAT TSVRaw
"""


def digest(text):
    return sha256(text.encode()).hexdigest()


def same(label, expected, actual):
    if expected != actual:
        expected_lines = expected.splitlines()
        actual_lines = actual.splitlines()
        line_number = 1
        expected_line = "<missing>"
        actual_line = "<missing>"
        for line_number in range(1, max(len(expected_lines), len(actual_lines)) + 1):
            expected_line = expected_lines[line_number - 1] if line_number <= len(expected_lines) else "<missing>"
            actual_line = actual_lines[line_number - 1] if line_number <= len(actual_lines) else "<missing>"
            if expected_line != actual_line:
                break
        raise RuntimeError(
            f"{label} differs\n"
            f"expected SHA-256: {digest(expected)}\n"
            f"actual SHA-256:   {digest(actual)}\n"
            f"first difference at line {line_number}:\n"
            f"expected: {expected_line}\nactual:   {actual_line}"
        )
    return f"{label}: equal"


def pg_result(name):
    sql = read_sql(f"sql/postgres/{name}.sql")
    return postgres(sql, plain=True, separator="\t").strip()


def ch_result(name, table):
    sql = read_sql(f"sql/clickhouse/{name}.sql").replace("{{TABLE}}", table)
    return clickhouse(sql + " FORMAT TSV").strip()


def mart_result(name):
    query_number = name.split("_")[0]
    sql = read_sql(f"sql/clickhouse/{query_number}_mart.sql")
    return clickhouse(sql + " FORMAT TSV").strip()


def main():
    pg_rows = postgres(PG_CANONICAL, plain=True).strip()
    station_rows = clickhouse(CH_CANONICAL.format(table="trips_by_station")).strip()
    key_rows = clickhouse(CH_CANONICAL.format(table="trips_by_key")).strip()

    checks = [
        same("all rows PG vs CH station order", pg_rows, station_rows),
        same("all rows PG vs CH key order", pg_rows, key_rows),
    ]

    pg_days = postgres("""
        SELECT started_at::date, count(*), sum(duration_sec),
               to_char(sum(fare_amount), 'FM999999999999990.00')
        FROM hw4.trips GROUP BY 1 ORDER BY 1;
    """, plain=True, separator="\t").strip()
    for table in ("trips_by_station", "trips_by_key"):
        ch_days = clickhouse(f"""
            SELECT toDate(started_at), count(), sum(duration_sec),
                   toDecimalString(sum(fare_amount), 2)
            FROM {table} GROUP BY 1 ORDER BY 1 FORMAT TSV
        """).strip()
        checks.append(same(f"per-day totals PG vs {table}", pg_days, ch_days))

    result_text = []
    for query in ("q1_narrow", "q2_wide"):
        expected = pg_result(query)
        station = ch_result(query, "trips_by_station")
        key = ch_result(query, "trips_by_key")
        mart = mart_result(query)
        checks.extend([
            same(f"{query} PG vs CH station order", expected, station),
            same(f"{query} PG vs CH key order", expected, key),
            same(f"{query} PG vs CH mart", expected, mart),
        ])
        result_text.append(f"{query} result:\n{expected}")

    summary = postgres("""
        SELECT count(*), count(DISTINCT trip_key), sum(duration_sec),
               sum(fare_amount), min(started_at), max(started_at)
        FROM hw4.trips;
    """, plain=True, separator="\t").strip()

    evidence = f"""HW4 CORRECTNESS EVIDENCE

PostgreSQL canonical SHA-256: {digest(pg_rows)}
ClickHouse station-order SHA-256: {digest(station_rows)}
ClickHouse key-order SHA-256: {digest(key_rows)}

PostgreSQL summary:
{summary}

Checks:
""" + "\n".join(checks) + "\n\n" + "\n\n".join(result_text) + "\n"
    save_evidence("checks.txt", evidence)

    trip_key_distribution = clickhouse("""
        SELECT cityHash64(trip_key) % 4 AS segment, count()
        FROM trips_by_station GROUP BY segment ORDER BY segment FORMAT TSV
    """).strip()
    station_distribution = clickhouse("""
        SELECT cityHash64(start_station_id) % 4 AS segment, count()
        FROM trips_by_station GROUP BY segment ORDER BY segment FORMAT TSV
    """).strip()
    area_distribution = clickhouse("""
        SELECT cityHash64(multiIf(
                   startsWith(start_station_id, 'JC'), 'Jersey City',
                   startsWith(start_station_id, 'HB'), 'Hoboken',
                   'Other'
               )) % 4 AS segment,
               count()
        FROM trips_by_station GROUP BY segment ORDER BY segment FORMAT TSV
    """).strip()
    save_evidence(
        "mpp_distribution.txt",
        "HW4 SIMULATED FOUR-SEGMENT MPP DISTRIBUTION\n\n"
        "Hash by trip_key (segment, rows):\n"
        f"{trip_key_distribution}\n\n"
        "Hash by start_station_id (segment, rows):\n"
        f"{station_distribution}\n\n"
        "Hash by area (only non-empty segments are returned):\n"
        f"{area_distribution}\n",
    )


if __name__ == "__main__":
    main()
