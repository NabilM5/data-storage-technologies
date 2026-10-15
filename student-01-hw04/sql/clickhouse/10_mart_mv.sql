DROP VIEW IF EXISTS daily_station_mv;
DROP TABLE IF EXISTS daily_station_mart;

CREATE TABLE daily_station_mart (
    calendar_date Date,
    station_id LowCardinality(String),
    member_casual LowCardinality(String),
    trips UInt64,
    electric_trips UInt64,
    duration_sum UInt64,
    fare_sum Decimal(18, 2)
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(calendar_date)
ORDER BY (calendar_date, station_id, member_casual);

CREATE MATERIALIZED VIEW daily_station_mv
TO daily_station_mart AS
SELECT
    toDate(started_at) AS calendar_date,
    start_station_id AS station_id,
    member_casual,
    count() AS trips,
    countIf(rideable_type = 'electric_bike') AS electric_trips,
    sum(toUInt64(duration_sec)) AS duration_sum,
    sum(fare_amount) AS fare_sum
FROM trips_by_station
GROUP BY calendar_date, station_id, member_casual;

