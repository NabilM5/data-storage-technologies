-- SCD2 station dimension: one row per version, interval [valid_from, valid_to).
select
    md5(station_id || ':' || valid_from::text)  as station_key,
    station_id,
    station_name,
    area,
    lat,
    lng,
    valid_from,
    valid_to,
    valid_to = date '9999-12-31'                as is_current
from {{ ref('stg_stations') }}
