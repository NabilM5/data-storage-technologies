-- Typed station versions; an open valid_to becomes 9999-12-31.
with typed as (
    select
        station_id,
        station_name,
        area,
        lat::numeric(9, 6)                                  as lat,
        lng::numeric(9, 6)                                  as lng,
        valid_from::date                                    as valid_from,
        coalesce(valid_to::date, date '9999-12-31')         as valid_to,
        source_system,
        source_file,
        loaded_at,
        row_number() over (
            partition by station_id, valid_from::date
            order by loaded_at desc, batch_id desc
        )                                                   as version_rank
    from {{ source('raw', 'stations') }}
)
select
    station_id,
    station_name,
    area,
    lat,
    lng,
    valid_from,
    valid_to,
    source_system,
    source_file,
    loaded_at
from typed
where version_rank = 1
