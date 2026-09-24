-- SCD2: exactly one station version must be valid on the trip's start date.
select
    t.trip_key,
    t.start_station_id,
    t.started_date,
    count(s.station_key) as versions_valid_on_start_date
from {{ ref('stg_trips') }} t
left join {{ ref('dim_station') }} s
    on s.station_id = t.start_station_id
   and t.started_date >= s.valid_from
   and t.started_date <  s.valid_to
group by t.trip_key, t.start_station_id, t.started_date
having count(s.station_key) <> 1
