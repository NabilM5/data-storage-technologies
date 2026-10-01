SELECT
    member_casual,
    count(*)::bigint AS trips,
    count(*) FILTER (WHERE rideable_type = 'electric_bike')::bigint AS electric_trips,
    sum(duration_sec)::bigint AS total_duration_sec,
    round(avg(duration_sec), 1) AS avg_duration_sec
FROM {{FACT_TABLE}}
WHERE start_station_id = 'JC115'
  AND started_at >= timestamp '2026-08-05 00:00:00'
  AND started_at <  timestamp '2026-08-06 00:00:00'
GROUP BY member_casual
ORDER BY member_casual;

