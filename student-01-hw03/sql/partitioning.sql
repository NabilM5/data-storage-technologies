DROP TABLE IF EXISTS hw3.fact_trip_partitioned CASCADE;

CREATE TABLE hw3.fact_trip_partitioned (
    trip_key            text NOT NULL,
    source_system       text NOT NULL,
    ride_id             text NOT NULL,
    start_station_id    text,
    start_station_name  text,
    end_station_id      text,
    rideable_type       text NOT NULL,
    member_casual       text NOT NULL,
    started_at          timestamp NOT NULL,
    ended_at            timestamp NOT NULL,
    duration_sec        integer NOT NULL CHECK (duration_sec >= 0),
    PRIMARY KEY (trip_key, started_at)
) PARTITION BY RANGE (started_at);

CREATE TABLE hw3.fact_trip_p_before_aug
    PARTITION OF hw3.fact_trip_partitioned
    FOR VALUES FROM (MINVALUE) TO ('2026-08-01');

CREATE TABLE hw3.fact_trip_p_aug01_08
    PARTITION OF hw3.fact_trip_partitioned
    FOR VALUES FROM ('2026-08-01') TO ('2026-08-08');

CREATE TABLE hw3.fact_trip_p_aug08_15
    PARTITION OF hw3.fact_trip_partitioned
    FOR VALUES FROM ('2026-08-08') TO ('2026-08-15');

CREATE TABLE hw3.fact_trip_p_aug15_22
    PARTITION OF hw3.fact_trip_partitioned
    FOR VALUES FROM ('2026-08-15') TO ('2026-08-22');

CREATE TABLE hw3.fact_trip_p_aug22_sep01
    PARTITION OF hw3.fact_trip_partitioned
    FOR VALUES FROM ('2026-08-22') TO ('2026-09-01');

INSERT INTO hw3.fact_trip_partitioned
SELECT * FROM dds.fact_trip;

CREATE INDEX idx_partitioned_station_started
    ON hw3.fact_trip_partitioned (start_station_id, started_at);

ANALYZE hw3.fact_trip_partitioned;

