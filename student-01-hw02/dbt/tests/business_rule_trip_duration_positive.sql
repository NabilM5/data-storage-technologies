-- Business rule: a trip must end after it starts.
select trip_key, source_system, ride_id, started_at, ended_at, duration_sec
from {{ ref('fact_trip') }}
where ended_at <= started_at
