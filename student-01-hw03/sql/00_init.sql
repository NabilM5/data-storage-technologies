CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS hw3;

CREATE TABLE IF NOT EXISTS raw.trip_import (
    ride_id             text,
    rideable_type       text,
    started_at          text,
    ended_at            text,
    start_station_name  text,
    start_station_id    text,
    end_station_name    text,
    end_station_id      text,
    start_lat           text,
    start_lng           text,
    end_lat             text,
    end_lng             text,
    member_casual       text
);

CREATE TABLE IF NOT EXISTS dds.fact_trip (
    trip_key            text PRIMARY KEY,
    source_system       text NOT NULL,
    ride_id             text NOT NULL,
    start_station_id    text,
    start_station_name  text,
    end_station_id      text,
    rideable_type       text NOT NULL,
    member_casual       text NOT NULL,
    started_at          timestamp NOT NULL,
    ended_at            timestamp NOT NULL,
    duration_sec        integer NOT NULL CHECK (duration_sec >= 0)
);

CREATE TABLE IF NOT EXISTS mart.daily_station_trips (
    row_key             text PRIMARY KEY,
    calendar_date       date NOT NULL,
    is_weekend          boolean NOT NULL,
    source_system       text NOT NULL,
    station_id          text NOT NULL,
    station_name        text NOT NULL,
    area                text NOT NULL,
    member_casual       text NOT NULL,
    trips               integer NOT NULL,
    electric_trips      integer NOT NULL,
    total_duration_sec  bigint NOT NULL,
    avg_duration_sec    numeric(10, 1) NOT NULL,
    run_id              text NOT NULL,
    published_at        timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS hw3.mvcc_demo (
    id integer PRIMARY KEY,
    note text NOT NULL
);

INSERT INTO hw3.mvcc_demo VALUES (1, 'original')
ON CONFLICT (id) DO NOTHING;

