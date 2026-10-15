#!/usr/bin/env python3
"""Load one typed Citi Bike dataset into PostgreSQL and ClickHouse."""

import csv
from datetime import datetime
from hashlib import sha256
import io
import math
import urllib.request
import zipfile

from common import (
    PROJECT,
    clickhouse,
    clickhouse_insert,
    command,
    postgres,
    read_sql,
    save_evidence,
)


SOURCE = PROJECT / "data" / "JC-202608-citibike-tripdata.csv.zip"
SOURCE_URL = "https://s3.amazonaws.com/tripdata/JC-202608-citibike-tripdata.csv.zip"
SOURCE_SHA256 = "74cbe35022888397e3ad4635188117ba594f80c55dda49f9701e73b5de81f108"
HISTORY_END = datetime(2026, 8, 25)
BATCH_1_END = datetime(2026, 8, 29)
BATCH_SIZE = 20_000


def clean(value):
    value = value.strip()
    return value if value else "UNKNOWN"


def fare_cents(duration_sec, member_casual):
    # Teaching-only fare: $2.50 + $0.20/minute + $1.00 for casual riders.
    return 250 + (duration_sec // 60) * 20 + (100 if member_casual == "casual" else 0)


def ensure_source():
    SOURCE.parent.mkdir(parents=True, exist_ok=True)
    if not SOURCE.exists():
        print(f"downloading {SOURCE_URL}")
        download = SOURCE.with_suffix(".download")
        urllib.request.urlretrieve(SOURCE_URL, download)
        if sha256(download.read_bytes()).hexdigest() != SOURCE_SHA256:
            raise RuntimeError("downloaded source checksum does not match")
        download.replace(SOURCE)

    source_hash = sha256(SOURCE.read_bytes()).hexdigest()
    if source_hash != SOURCE_SHA256:
        raise RuntimeError("source checksum does not match")
    return source_hash


def read_source():
    source_hash = ensure_source()

    result = []
    with zipfile.ZipFile(SOURCE) as archive:
        csv_name = next(
            name for name in archive.namelist()
            if name.endswith(".csv") and "__MACOSX" not in name
        )
        with archive.open(csv_name) as raw_file:
            text_file = io.TextIOWrapper(raw_file, encoding="utf-8-sig")
            for row in csv.DictReader(text_file):
                started = datetime.fromisoformat(row["started_at"])
                ended = datetime.fromisoformat(row["ended_at"])
                duration = math.floor((ended - started).total_seconds())
                if duration < 0:
                    raise ValueError(f"negative duration for ride {row['ride_id']}")

                cents = fare_cents(duration, row["member_casual"])
                result.append((
                    f"citibike_jc:{row['ride_id']}",
                    "citibike_jc",
                    row["ride_id"],
                    row["rideable_type"],
                    started.isoformat(sep=" ", timespec="milliseconds"),
                    ended.isoformat(sep=" ", timespec="milliseconds"),
                    clean(row["start_station_id"]),
                    clean(row["start_station_name"]),
                    clean(row["end_station_id"]),
                    clean(row["end_station_name"]),
                    row["member_casual"],
                    duration,
                    f"{cents // 100}.{cents % 100:02d}",
                ))
    return result, source_hash


def to_csv(rows):
    output = io.StringIO()
    csv.writer(output, lineterminator="\n").writerows(rows)
    return output.getvalue()


def load_postgres(rows):
    columns = (
        "trip_key, source_system, ride_id, rideable_type, started_at, ended_at, "
        "start_station_id, start_station_name, end_station_id, end_station_name, "
        "member_casual, duration_sec, fare_amount"
    )
    copy_sql = f"\\copy hw4.trips ({columns}) FROM STDIN WITH (FORMAT csv)"
    args = [
        "docker", "compose", "exec", "-T", "postgres",
        "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "dwh", "-d", "dwh",
        "-c", copy_sql,
    ]
    command(args, data=to_csv(rows))
    postgres("ANALYZE hw4.trips;")


def load_clickhouse(table, rows):
    for start in range(0, len(rows), BATCH_SIZE):
        clickhouse_insert(table, to_csv(rows[start:start + BATCH_SIZE]))


def stage_check(stage):
    source_groups = clickhouse("""
        SELECT
            toDate(started_at),
            start_station_id,
            member_casual,
            count(),
            countIf(rideable_type = 'electric_bike'),
            sum(toUInt64(duration_sec)),
            toDecimalString(sum(fare_amount), 2),
            toDecimalString(round(avg(duration_sec), 1), 1)
        FROM trips_by_station
        GROUP BY toDate(started_at), start_station_id, member_casual
        ORDER BY toDate(started_at), start_station_id, member_casual
        FORMAT TSV
    """).strip()
    mart_groups = clickhouse("""
        SELECT
            calendar_date,
            station_id,
            member_casual,
            sum(m.trips),
            sum(m.electric_trips),
            sum(m.duration_sum),
            toDecimalString(sum(m.fare_sum), 2),
            toDecimalString(round(sum(m.duration_sum) / sum(m.trips), 1), 1)
        FROM daily_station_mart AS m
        GROUP BY calendar_date, station_id, member_casual
        ORDER BY calendar_date, station_id, member_casual
        FORMAT TSV
    """).strip()
    if source_groups != mart_groups:
        raise RuntimeError(f"mart groups differ after {stage}")

    totals = clickhouse("""
        SELECT count(), countIf(rideable_type = 'electric_bike'),
               sum(duration_sec), toDecimalString(sum(fare_amount), 2)
        FROM trips_by_station
        FORMAT TSV
    """).strip()
    group_count = len(source_groups.splitlines())
    group_hash = sha256(source_groups.encode()).hexdigest()
    return f"equal; groups={group_count}; totals={totals}; SHA-256={group_hash}"


def main():
    rows, source_hash = read_source()
    history = [row for row in rows if datetime.fromisoformat(row[4]) < HISTORY_END]
    batch_1 = [
        row for row in rows
        if HISTORY_END <= datetime.fromisoformat(row[4]) < BATCH_1_END
    ]
    batch_2 = [row for row in rows if datetime.fromisoformat(row[4]) >= BATCH_1_END]

    postgres(read_sql("sql/postgres/00_schema.sql"))
    clickhouse(read_sql("sql/clickhouse/00_schema.sql"))

    load_postgres(rows)
    load_clickhouse("trips_by_key", rows)
    load_clickhouse("trips_by_station", history)

    clickhouse(read_sql("sql/clickhouse/10_mart_mv.sql"))
    empty_mart = clickhouse("SELECT count() FROM daily_station_mart FORMAT TSV").strip()
    if empty_mart != "0":
        raise RuntimeError("new materialized view unexpectedly included history")

    clickhouse("""
        INSERT INTO daily_station_mart
        SELECT
            toDate(started_at),
            start_station_id,
            member_casual,
            count(),
            countIf(rideable_type = 'electric_bike'),
            sum(toUInt64(duration_sec)),
            sum(fare_amount)
        FROM trips_by_station
        WHERE started_at < toDateTime64('2026-08-25 00:00:00', 3, 'America/New_York')
        GROUP BY toDate(started_at), start_station_id, member_casual
    """)
    history_check = stage_check("history backfill")

    load_clickhouse("trips_by_station", batch_1)
    batch_1_check = stage_check("batch 1")

    load_clickhouse("trips_by_station", batch_2)
    batch_2_check = stage_check("batch 2")

    pg_version = postgres("SELECT version();", plain=True).strip()
    ch_version = clickhouse("SELECT version() FORMAT TSV").strip()
    final_tables = clickhouse("""
        SELECT _table, count(), uniqExact(trip_key), sum(duration_sec),
               toDecimalString(sum(fare_amount), 2)
        FROM merge('dwh', '^trips_by_(station|key)$')
        GROUP BY _table
        ORDER BY _table
        FORMAT TSV
    """).strip()
    parts = clickhouse("""
        SELECT table, count(), sum(rows), sum(bytes_on_disk)
        FROM system.parts
        WHERE active AND database = 'dwh'
        GROUP BY table
        ORDER BY table
        FORMAT TSV
    """).strip()

    evidence = f"""HW4 LOAD AND INCREMENTAL MART EVIDENCE

PostgreSQL: {pg_version}
ClickHouse: {ch_version}
Source URL: {SOURCE_URL}
Source SHA-256: {source_hash}
Local cache: {SOURCE.relative_to(PROJECT)}
Fare rule: $2.50 + $0.20 per completed minute + $1.00 for casual riders

Total source rows: {len(rows)}
History rows before 2026-08-25: {len(history)}
Batch 1 rows, 2026-08-25 to 2026-08-29: {len(batch_1)}
Batch 2 rows, 2026-08-29 onward: {len(batch_2)}

Mart rows immediately after MV creation: {empty_mart}
Full-grain check after controlled backfill: {history_check}
Full-grain check after batch 1: {batch_1_check}
Full-grain check after batch 2: {batch_2_check}

Final ClickHouse source tables:
{final_tables}

Active parts:
{parts}
"""
    save_evidence("load_and_mart.txt", evidence)


if __name__ == "__main__":
    main()
