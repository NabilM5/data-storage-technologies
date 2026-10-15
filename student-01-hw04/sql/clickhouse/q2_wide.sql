SELECT
    multiIf(
        startsWith(start_station_id, 'JC'), 'Jersey City',
        startsWith(start_station_id, 'HB'), 'Hoboken',
        'Other'
    ) AS area,
    member_casual,
    count() AS trips,
    countIf(rideable_type = 'electric_bike') AS electric_trips,
    sum(toUInt64(duration_sec)) AS duration_sum,
    toDecimalString(sum(fare_amount), 2) AS fare_sum,
    toDecimalString(round(avg(duration_sec), 1), 1) AS avg_duration_sec
FROM {{TABLE}}
WHERE started_at >= toDateTime64('2026-07-31 00:00:00', 3, 'America/New_York')
  AND started_at < toDateTime64('2026-09-01 00:00:00', 3, 'America/New_York')
GROUP BY area, member_casual
ORDER BY area, member_casual
