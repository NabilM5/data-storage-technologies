#!/usr/bin/env bash
# Runs all diagnostic scenarios through Airflow and records the evidence.
#   bash scripts/run_all.sh | tee evidence/runs.txt
# Needs the stand up and the DAG unpaused. Each run starts from the baseline.
set -uo pipefail
cd "$(dirname "$0")/.."

DAG=hw02_citibike_elt
FROM=2026-08-03
TO=2026-08-09
DWH="docker compose exec -T postgres psql -U dwh -d dwh"
AF_DB="docker compose exec -T postgres psql -U airflow -d airflow -tA"

q() { docker compose exec -T postgres psql -U dwh -d dwh -c "$1"; }

run_scenario() {                       # $1 = scenario name
    local scenario=$1
    echo; echo "################ RUN: scenario=$scenario interval=$FROM..$TO ################"
    docker compose exec -T airflow airflow dags trigger "$DAG" \
        --conf "{\"date_from\": \"$FROM\", \"date_to\": \"$TO\", \"scenario\": \"$scenario\"}" >/dev/null 2>&1
    sleep 5
    local run_id state i=0
    run_id=$($AF_DB -c "select run_id from dag_run where dag_id='$DAG' order by id desc limit 1")
    until [ $i -ge 60 ]; do
        state=$($AF_DB -c "select state from dag_run where dag_id='$DAG' and run_id='$run_id'")
        case "$state" in success|failed) break;; esac
        i=$((i+1)); sleep 5
    done
    echo "run_id=$run_id  state=$state"
    docker compose exec -T postgres psql -U airflow -d airflow -c \
        "select task_id, state, to_char(start_date,'HH24:MI:SS') as started, to_char(end_date,'HH24:MI:SS') as ended
         from task_instance where dag_id='$DAG' and run_id='$run_id' order by start_date nulls last"
    if [ "$state" != "success" ]; then
        echo "--- failing dbt tests (from the dbt_test task log) ---"
        grep -hE "FAIL [0-9]+|ERROR [0-9]+|Failure in test|Got [0-9]+ result" \
            "logs/dag_id=$DAG/run_id=$run_id/task_id=dbt_test/attempt=1.log" 2>/dev/null | head -12
    fi
    echo "--- publication log (last 3) ---"
    q "select publication_id, scenario, rows_published, checksum, to_char(published_at,'HH24:MI:SS') as published_at
       from ops.publications order by publication_id desc limit 3"
}

echo "=== 0. Reference cell from the source file (independent of the DWH) ==="
python3 data/independent_check.py 2026-08-05 JC115

run_scenario baseline
run_scenario baseline
echo "--- repeat comparison: the two publications of the same input ---"
q "select publication_id, run_id, rows_published, checksum from ops.publications order by publication_id desc limit 2"
q "select count(*) as published_rows, sum(trips) as trips, sum(total_duration_sec) as total_duration_sec,
          md5(string_agg(row_key||'|'||trips||'|'||electric_trips||'|'||total_duration_sec, ',' order by row_key)) as checksum_now
   from mart.daily_station_trips"

run_scenario duplicate
echo "--- duplicate: raw copies, staged rows and fact rows for the re-sent ride ---"
q "select 'raw' as layer, count(*) from raw.trips where ride_id='221F05B20357A5C2'
   union all select 'stg', count(*) from stg.stg_trips where ride_id='221F05B20357A5C2'
   union all select 'fact', count(*) from dds.fact_trip where ride_id='221F05B20357A5C2'"
q "select calendar_date, station_id, member_casual, trips, total_duration_sec from mart.daily_station_trips
   where calendar_date='2026-08-05' and station_id='JC115' order by member_casual"

run_scenario late_fix
echo "--- late_fix: both versions in raw, the accepted version in staging ---"
q "select ride_id, source_file, ended_at, to_char(loaded_at,'HH24:MI:SS') as loaded_at from raw.trips
   where ride_id='9EF6A52D8FA471A1' order by loaded_at"
q "select ride_id, started_at, ended_at, duration_sec, source_file from stg.stg_trips where ride_id='9EF6A52D8FA471A1'"
q "select calendar_date, station_id, member_casual, trips, total_duration_sec, avg_duration_sec from mart.daily_station_trips
   where calendar_date='2026-08-05' and station_id='JC115' order by member_casual"
run_scenario late_fix
echo "--- late_fix repeated: same cell again (correction must not double) ---"
q "select calendar_date, station_id, member_casual, trips, total_duration_sec, avg_duration_sec from mart.daily_station_trips
   where calendar_date='2026-08-05' and station_id='JC115' order by member_casual"

run_scenario second_source
echo "--- second_source: same ride_id values from two systems stay separate facts ---"
q "select ride_id, count(*) as facts, string_agg(source_system||'@'||start_station_id, ', ' order by source_system) as sources
   from dds.fact_trip group by ride_id having count(*) > 1 order by ride_id"
q "select source_system, count(*) as trips from dds.fact_trip group by source_system order by 1"
q "select calendar_date, source_system, station_id, member_casual, trips, total_duration_sec from mart.daily_station_trips
   where calendar_date='2026-08-06' and station_id='HB609' order by source_system, member_casual"

run_scenario history_overlap
echo "--- history_overlap: two open versions of JC115 in the dimension; publication must be unchanged ---"
q "select station_key, station_id, station_name, valid_from, valid_to, is_current from dds.dim_station where station_id='JC115' order by valid_from"
q "select count(*) as fact_rows_for_JC115_from_0805, count(distinct trip_key) as distinct_trips from dds.fact_trip
   where start_station_id='JC115' and started_at >= '2026-08-05'"
q "select max(published_at) as last_publication, count(distinct run_id) as runs_in_mart from mart.daily_station_trips"

run_scenario zero_duration
echo "--- zero_duration: the violating row found by the business-rule test; publication unchanged ---"
q "select trip_key, started_at, ended_at, duration_sec from dds.fact_trip where ended_at <= started_at"
q "select max(published_at) as last_publication, count(distinct run_id) as runs_in_mart from mart.daily_station_trips"

run_scenario baseline
echo "--- recovery: clean input published again; compare with the earlier baseline checksum ---"
q "select publication_id, scenario, rows_published, checksum, to_char(published_at,'HH24:MI:SS') as published_at from ops.publications order by publication_id"
echo; echo "=== done ==="
