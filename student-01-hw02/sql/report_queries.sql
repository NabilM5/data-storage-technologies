-- Queries for the report, against the published mart. Run from the project dir:
--   docker compose exec -T postgres psql -U dwh -d dwh -f - < sql/report_queries.sql

\echo '== Q1. Trips per day and station for the week, with member share =='
select calendar_date, station_id, station_name, area,
       sum(trips)                                   as trips,
       sum(trips) filter (where member_casual = 'member') as member_trips,
       round(100.0 * sum(trips) filter (where member_casual = 'member') / sum(trips), 1) as member_pct
from mart.daily_station_trips
where source_system = 'citibike_jc'
group by calendar_date, station_id, station_name, area
order by calendar_date, station_id;

\echo '== Q2. Weekday vs weekend: trips and average duration by area and rider type =='
select area, member_casual,
       case when is_weekend then 'weekend' else 'weekday' end as day_type,
       sum(trips)                                            as trips,
       round(sum(total_duration_sec)::numeric / sum(trips) / 60, 1) as avg_duration_min,
       round(100.0 * sum(electric_trips) / sum(trips), 1)    as electric_pct
from mart.daily_station_trips
where source_system = 'citibike_jc'
group by area, member_casual, is_weekend
order by area, member_casual, day_type;

\echo '== Q3. Control cell: 2026-08-05 x JC115 (compare with data/independent_check.py) =='
select calendar_date, station_id, member_casual, trips, electric_trips, total_duration_sec, avg_duration_sec, source_system
from mart.daily_station_trips
where calendar_date = date '2026-08-05' and station_id = 'JC115'
order by source_system, member_casual;

\echo '== Layers: rows per physical table =='
select 'raw.trips' as tbl, count(*) as rows from raw.trips
union all select 'raw.stations', count(*) from raw.stations
union all select 'stg.stg_trips', count(*) from stg.stg_trips
union all select 'dds.fact_trip', count(*) from dds.fact_trip
union all select 'dds.dim_station', count(*) from dds.dim_station
union all select 'dds.dim_date', count(*) from dds.dim_date
union all select 'mart_candidate.daily_station_trips', count(*) from mart_candidate.daily_station_trips
union all select 'mart.daily_station_trips', count(*) from mart.daily_station_trips;

\echo '== Raw batches =='
select source_system, source_file, count(*) as rows, min(loaded_at) as loaded_at
from raw.trips group by 1, 2
union all
select source_system, source_file, count(*), min(loaded_at) from raw.stations group by 1, 2
order by loaded_at;

\echo '== Fact rows per source system =='
select source_system, count(*) as trips, sum(duration_sec) as total_duration_sec
from dds.fact_trip group by source_system order by source_system;

\echo '== Publication log (what the consumer sees and when it was last refreshed) =='
select publication_id, run_id, scenario, date_from, date_to, rows_published, checksum, published_at
from ops.publications order by publication_id;

\echo '== Published mart: rows, run and timestamp per interval =='
select min(calendar_date) as date_from, max(calendar_date) as date_to, count(*) as rows,
       sum(trips) as trips, sum(total_duration_sec) as total_duration_sec,
       run_id, published_at
from mart.daily_station_trips group by run_id, published_at order by published_at;
