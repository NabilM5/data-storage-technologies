-- Mart totals must equal fact totals for the interval.
with mart as (
    select sum(trips) as trips, sum(total_duration_sec) as duration_sec
    from {{ ref('daily_station_trips') }}
),
fact as (
    select count(*) as trips, sum(f.duration_sec) as duration_sec
    from {{ ref('fact_trip') }} f
    join {{ ref('dim_date') }} d on d.date_key = f.date_key
    where d.calendar_date between '{{ var("date_from") }}'::date and '{{ var("date_to") }}'::date
)
select mart.trips as mart_trips, fact.trips as fact_trips,
       mart.duration_sec as mart_duration_sec, fact.duration_sec as fact_duration_sec
from mart, fact
where mart.trips is distinct from fact.trips
   or mart.duration_sec is distinct from fact.duration_sec
