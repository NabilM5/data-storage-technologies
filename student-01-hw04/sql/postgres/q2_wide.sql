SELECT
    CASE
        WHEN start_station_id LIKE 'JC%' THEN 'Jersey City'
        WHEN start_station_id LIKE 'HB%' THEN 'Hoboken'
        ELSE 'Other'
    END AS area,
    member_casual,
    count(*)::bigint AS trips,
    count(*) FILTER (WHERE rideable_type = 'electric_bike')::bigint AS electric_trips,
    sum(duration_sec)::bigint AS duration_sum,
    to_char(sum(fare_amount), 'FM999999999999990.00') AS fare_sum,
    round(avg(duration_sec), 1) AS avg_duration_sec
FROM hw4.trips
WHERE started_at >= timestamp '2026-07-31 00:00:00'
  AND started_at < timestamp '2026-09-01 00:00:00'
GROUP BY area, member_casual
ORDER BY area, member_casual;
