-- One row = one trip; the station version is the one valid on the start date.
select
    t.trip_key,
    t.source_system,
    t.ride_id,
    d.date_key,
    s.station_key,
    t.start_station_id,
    t.end_station_id,
    t.rideable_type,
    t.member_casual,
    t.started_at,
    t.ended_at,
    t.duration_sec
from {{ ref('stg_trips') }} t
join {{ ref('dim_date') }} d
    on d.calendar_date = t.started_date
left join {{ ref('dim_station') }} s
    on s.station_id = t.start_station_id
   and t.started_date >= s.valid_from
   and t.started_date <  s.valid_to
