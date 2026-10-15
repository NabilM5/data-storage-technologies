DROP VIEW IF EXISTS daily_station_mv;
DROP TABLE IF EXISTS daily_station_mart;
DROP TABLE IF EXISTS trips_by_station;
DROP TABLE IF EXISTS trips_by_key;

CREATE TABLE trips_by_station (
    trip_key String,
    source_system LowCardinality(String),
    ride_id String,
    rideable_type LowCardinality(String),
    started_at DateTime64(3, 'America/New_York'),
    ended_at DateTime64(3, 'America/New_York'),
    start_station_id LowCardinality(String),
    start_station_name LowCardinality(String),
    end_station_id LowCardinality(String),
    end_station_name LowCardinality(String),
    member_casual LowCardinality(String),
    duration_sec UInt32,
    fare_amount Decimal(12, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (start_station_id, started_at, trip_key);

CREATE TABLE trips_by_key (
    trip_key String,
    source_system LowCardinality(String),
    ride_id String,
    rideable_type LowCardinality(String),
    started_at DateTime64(3, 'America/New_York'),
    ended_at DateTime64(3, 'America/New_York'),
    start_station_id LowCardinality(String),
    start_station_name LowCardinality(String),
    end_station_id LowCardinality(String),
    end_station_name LowCardinality(String),
    member_casual LowCardinality(String),
    duration_sec UInt32,
    fare_amount Decimal(12, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY trip_key;

