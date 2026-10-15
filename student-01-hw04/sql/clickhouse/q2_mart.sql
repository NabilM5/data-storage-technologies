SELECT
    multiIf(
        startsWith(station_id, 'JC'), 'Jersey City',
        startsWith(station_id, 'HB'), 'Hoboken',
        'Other'
    ) AS area,
    member_casual,
    sum(m.trips) AS trips,
    sum(m.electric_trips) AS electric_trips,
    sum(m.duration_sum) AS duration_sum,
    toDecimalString(sum(m.fare_sum), 2) AS fare_sum,
    toDecimalString(round(sum(m.duration_sum) / sum(m.trips), 1), 1) AS avg_duration_sec
FROM daily_station_mart AS m
WHERE calendar_date >= toDate('2026-07-31')
  AND calendar_date < toDate('2026-09-01')
GROUP BY area, member_casual
ORDER BY area, member_casual
