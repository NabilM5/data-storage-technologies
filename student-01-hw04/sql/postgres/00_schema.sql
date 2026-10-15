DROP SCHEMA IF EXISTS hw4 CASCADE;
CREATE SCHEMA hw4;

CREATE TABLE hw4.trips (
    trip_key            text PRIMARY KEY,
    source_system       text NOT NULL,
    ride_id             text NOT NULL,
    rideable_type       text NOT NULL,
    started_at          timestamp NOT NULL,
    ended_at            timestamp NOT NULL,
    start_station_id    text NOT NULL,
    start_station_name  text NOT NULL,
    end_station_id      text NOT NULL,
    end_station_name    text NOT NULL,
    member_casual       text NOT NULL,
    duration_sec        integer NOT NULL CHECK (duration_sec >= 0),
    fare_amount         numeric(12, 2) NOT NULL CHECK (fare_amount >= 0)
);

CREATE INDEX idx_trips_station_started
    ON hw4.trips (start_station_id, started_at);

