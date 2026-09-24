-- Calendar for the dates present in staging.
with bounds as (
    select min(started_date) as d0, max(started_date) as d1
    from {{ ref('stg_trips') }}
),
days as (
    select generate_series(d0, d1, interval '1 day')::date as calendar_date
    from bounds
)
select
    to_char(calendar_date, 'YYYYMMDD')::integer     as date_key,
    calendar_date,
    extract(isodow from calendar_date)::integer     as iso_weekday,
    to_char(calendar_date, 'Dy')                    as weekday_name,
    extract(isodow from calendar_date) >= 6         as is_weekend,
    to_char(calendar_date, 'YYYY-MM')               as year_month
from days
