SELECT
    member_casual,
    sum(m.trips) AS trips,
    sum(m.electric_trips) AS electric_trips,
    sum(m.duration_sum) AS duration_sum,
    toDecimalString(sum(m.fare_sum), 2) AS fare_sum,
    toDecimalString(round(sum(m.duration_sum) / sum(m.trips), 1), 1) AS avg_duration_sec
FROM daily_station_mart AS m
WHERE station_id = 'JC115'
  AND calendar_date = toDate('2026-08-05')
GROUP BY member_casual
ORDER BY member_casual
