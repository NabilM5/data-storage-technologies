-- Typed trips, one row per (source_system, ride_id): latest loaded wins.
with typed as (
    select
        source_system,
        ride_id,
        rideable_type,
        started_at::timestamp                      as started_at,
        ended_at::timestamp                        as ended_at,
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,
        start_lat::numeric(9, 6)                   as start_lat,
        start_lng::numeric(9, 6)                   as start_lng,
        end_lat::numeric(9, 6)                     as end_lat,
        end_lng::numeric(9, 6)                     as end_lng,
        member_casual,
        source_file,
        batch_id,
        loaded_at,
        row_number() over (
            partition by source_system, ride_id
            order by loaded_at desc, batch_id desc
        )                                          as version_rank
    from {{ source('raw', 'trips') }}
)
select
    source_system,
    ride_id,
    source_system || ':' || ride_id                          as trip_key,
    rideable_type,
    started_at,
    ended_at,
    started_at::date                                         as started_date,
    -- whole seconds; the source has milliseconds
    floor(extract(epoch from (ended_at - started_at)))::integer as duration_sec,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    source_file,
    batch_id,
    loaded_at
from typed
where version_rank = 1
