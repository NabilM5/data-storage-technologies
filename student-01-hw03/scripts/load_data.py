#!/usr/bin/env python3
"""Load the HW2 August source file into the simpler HW3 PostgreSQL model."""

from pathlib import Path
import zipfile

from common import PROJECT, psql, command, write_evidence


ZIP_FILE = PROJECT.parent / "student-01-hw02" / "data" / "JC-202608-citibike-tripdata.csv.zip"
COLUMNS = (
    "ride_id, rideable_type, started_at, ended_at, start_station_name, "
    "start_station_id, end_station_name, end_station_id, start_lat, "
    "start_lng, end_lat, end_lng, member_casual"
)


def main():
    if not ZIP_FILE.exists():
        raise SystemExit(f"missing HW2 source file: {ZIP_FILE}")

    psql("TRUNCATE mart.daily_station_trips, dds.fact_trip, raw.trip_import;")

    with zipfile.ZipFile(ZIP_FILE) as archive:
        csv_name = next(
            name for name in archive.namelist()
            if name.endswith(".csv") and "__MACOSX" not in name
        )
        csv_bytes = archive.read(csv_name)

    copy_sql = f"\\copy raw.trip_import ({COLUMNS}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    args = ["docker", "compose", "exec", "-T", "postgres", "psql", "-X",
            "-v", "ON_ERROR_STOP=1", "-U", "dwh", "-d", "dwh", "-c", copy_sql]
    command(args, data=csv_bytes)

    transform_sql = """
INSERT INTO dds.fact_trip (
    trip_key, source_system, ride_id, start_station_id, start_station_name,
    end_station_id, rideable_type, member_casual, started_at, ended_at,
    duration_sec
)
SELECT
    'citibike_jc:' || ride_id,
    'citibike_jc',
    ride_id,
    start_station_id,
    start_station_name,
    end_station_id,
    rideable_type,
    member_casual,
    started_at::timestamp,
    ended_at::timestamp,
    floor(extract(epoch FROM (ended_at::timestamp - started_at::timestamp)))::integer
FROM raw.trip_import;

INSERT INTO mart.daily_station_trips
SELECT
    started_at::date::text || '|citibike_jc|' || coalesce(start_station_id, 'UNKNOWN') || '|' || member_casual,
    started_at::date,
    extract(isodow FROM started_at) IN (6, 7),
    'citibike_jc',
    coalesce(start_station_id, 'UNKNOWN'),
    coalesce(max(start_station_name), 'Unknown'),
    CASE
        WHEN start_station_id LIKE 'JC%' THEN 'Jersey City'
        WHEN start_station_id LIKE 'HB%' THEN 'Hoboken'
        ELSE 'Other'
    END,
    member_casual,
    count(*)::integer,
    count(*) FILTER (WHERE rideable_type = 'electric_bike')::integer,
    sum(duration_sec)::bigint,
    round(sum(duration_sec)::numeric / count(*), 1),
    'hw03-load',
    now()
FROM dds.fact_trip
GROUP BY started_at::date, extract(isodow FROM started_at),
         start_station_id, member_casual;

ANALYZE dds.fact_trip;
ANALYZE mart.daily_station_trips;
"""
    psql(transform_sql)

    checks = psql("""
SELECT version();
SELECT 'fact rows and dates' AS check_name, count(*)::text,
       min(started_at)::text, max(started_at)::text
FROM dds.fact_trip;
SELECT 'control slice' AS check_name, count(*)::text,
       sum(duration_sec)::text
FROM dds.fact_trip
WHERE started_at >= '2026-08-03' AND started_at < '2026-08-10'
  AND start_station_id IN ('JC115', 'HB609', 'JC109', 'HB107', 'JC009', 'HB103');
SELECT 'control mart' AS check_name, count(*)::text,
       sum(trips)::text, sum(total_duration_sec)::text,
       md5(coalesce(string_agg(
           row_key || '|' || trips || '|' || electric_trips || '|' || total_duration_sec,
           ',' ORDER BY row_key), ''))
FROM mart.daily_station_trips
WHERE calendar_date BETWEEN '2026-08-03' AND '2026-08-09'
  AND station_id IN ('JC115', 'HB609', 'JC109', 'HB107', 'JC009', 'HB103');
""").stdout
    write_evidence("load.txt", checks)


if __name__ == "__main__":
    main()
