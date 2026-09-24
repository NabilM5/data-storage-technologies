-- SCD2: versions of one station must not overlap.
select
    a.station_id,
    a.station_key   as version_a,
    a.valid_from    as a_from,
    a.valid_to      as a_to,
    b.station_key   as version_b,
    b.valid_from    as b_from,
    b.valid_to      as b_to
from {{ ref('dim_station') }} a
join {{ ref('dim_station') }} b
    on a.station_id = b.station_id
   and a.station_key < b.station_key
   and a.valid_from < b.valid_to
   and b.valid_from < a.valid_to
