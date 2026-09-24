-- Mart candidate for the interval: day x source x station x rider type.
-- avg_duration_sec comes from the sums so it re-aggregates correctly.
select
    d.calendar_date::text || '|' || f.source_system || '|' || s.station_id || '|' || f.member_casual
                                                        as row_key,
    d.calendar_date,
    d.is_weekend,
    f.source_system,
    s.station_id,
    s.station_name,
    s.area,
    f.member_casual,
    count(*)::integer                                   as trips,
    count(*) filter (where f.rideable_type = 'electric_bike')::integer
                                                        as electric_trips,
    sum(f.duration_sec)::bigint                         as total_duration_sec,
    round(sum(f.duration_sec)::numeric / count(*), 1)   as avg_duration_sec
from {{ ref('fact_trip') }} f
join {{ ref('dim_date') }} d
    on d.date_key = f.date_key
join {{ ref('dim_station') }} s
    on s.station_key = f.station_key
where d.calendar_date between '{{ var("date_from") }}'::date and '{{ var("date_to") }}'::date
group by
    d.calendar_date, d.is_weekend, f.source_system,
    s.station_id, s.station_name, s.area, f.member_casual
