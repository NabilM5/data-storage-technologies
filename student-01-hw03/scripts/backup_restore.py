#!/usr/bin/env python3
"""Create a custom-format dump, restore it, and compare both databases."""

from common import PROJECT, command, psql, write_evidence


VERIFY_SQL = r"""
SELECT 'schemas', count(*)::text
FROM information_schema.schemata
WHERE schema_name IN ('raw', 'dds', 'mart', 'hw3');
SELECT 'tables', count(*)::text
FROM pg_tables
WHERE schemaname IN ('raw', 'dds', 'mart', 'hw3');
SELECT 'indexes', count(*)::text
FROM pg_indexes
WHERE schemaname IN ('dds', 'mart', 'hw3');
SELECT 'constraints', count(*)::text
FROM pg_constraint c
JOIN pg_namespace n ON n.oid = c.connamespace
WHERE n.nspname IN ('dds', 'mart', 'hw3');
SELECT 'fact', count(*)::text, count(trip_key)::text,
       count(DISTINCT trip_key)::text, min(started_at)::text, max(started_at)::text
FROM dds.fact_trip;
SELECT 'mart', count(*)::text, sum(trips)::text, sum(total_duration_sec)::text
FROM mart.daily_station_trips;
SELECT 'control', count(*)::text, sum(duration_sec)::text
FROM dds.fact_trip
WHERE started_at >= '2026-08-03' AND started_at < '2026-08-10'
  AND start_station_id IN ('JC115', 'HB609', 'JC109', 'HB107', 'JC009', 'HB103');
SELECT 'narrow_result', md5(string_agg(x::text, E'\n' ORDER BY x::text))
FROM (
    SELECT member_casual, count(*)::bigint AS trips,
           count(*) FILTER (WHERE rideable_type = 'electric_bike')::bigint AS electric_trips,
           sum(duration_sec)::bigint AS total_duration_sec,
           round(avg(duration_sec), 1) AS avg_duration_sec
    FROM dds.fact_trip
    WHERE start_station_id = 'JC115'
      AND started_at >= '2026-08-05' AND started_at < '2026-08-06'
    GROUP BY member_casual
) x;
SELECT 'wide_result', md5(string_agg(x::text, E'\n' ORDER BY x::text))
FROM (
    SELECT CASE WHEN start_station_id LIKE 'JC%' THEN 'Jersey City'
                WHEN start_station_id LIKE 'HB%' THEN 'Hoboken' ELSE 'Other' END AS area,
           member_casual, count(*)::bigint AS trips,
           count(*) FILTER (WHERE rideable_type = 'electric_bike')::bigint AS electric_trips,
           sum(duration_sec)::bigint AS total_duration_sec,
           round(avg(duration_sec), 1) AS avg_duration_sec
    FROM dds.fact_trip
    WHERE started_at >= '2026-07-31' AND started_at < '2026-09-01'
    GROUP BY area, member_casual
) x;
"""


def docker_db_tool(*args, check=True):
    return command(["docker", "compose", "exec", "-T", "postgres", *args], check=check)


def main():
    (PROJECT / "backup").mkdir(exist_ok=True)
    version = docker_db_tool("pg_dump", "--version").stdout.strip()
    dump = docker_db_tool(
        "pg_dump", "-U", "dwh", "-d", "dwh", "-Fc", "-f", "/backup/dwh.dump"
    )

    docker_db_tool("dropdb", "-U", "dwh", "--if-exists", "--force", "dwh_restore")
    docker_db_tool("createdb", "-U", "dwh", "-O", "dwh", "dwh_restore")
    restore = docker_db_tool(
        "pg_restore", "-U", "dwh", "-d", "dwh_restore", "--exit-on-error", "/backup/dwh.dump"
    )

    source = psql(VERIFY_SQL, plain=True).stdout
    restored = psql(VERIFY_SQL, database="dwh_restore", plain=True).stdout
    equal = source == restored
    if not equal:
        raise RuntimeError("source and restored verification outputs differ")

    text = (
        "HW3 LOGICAL BACKUP AND RESTORE EVIDENCE\n\n"
        f"Utility: {version}\n"
        "Format: pg_dump custom format (-Fc)\n"
        "Dump command: pg_dump -U dwh -d dwh -Fc -f /backup/dwh.dump\n"
        "Restore command: pg_restore -U dwh -d dwh_restore --exit-on-error /backup/dwh.dump\n"
        f"pg_dump exit code: {dump.returncode}\n"
        f"pg_restore exit code: {restore.returncode}\n\n"
        "Source database checks:\n" + source + "\n"
        "Restored database checks:\n" + restored + "\n"
        f"Exact verification output match: {equal}\n"
    )
    write_evidence("backup_restore.txt", text)


if __name__ == "__main__":
    main()

