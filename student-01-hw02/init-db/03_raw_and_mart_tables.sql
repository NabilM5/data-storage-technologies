-- Raw: rows as received (all text) plus batch metadata.
CREATE TABLE raw.trips (
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
    member_casual       text,
    source_system       text        NOT NULL,
    source_file         text        NOT NULL,
    batch_id            uuid        NOT NULL,
    loaded_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE raw.stations (
    station_id      text,
    station_name    text,
    area            text,
    lat             text,
    lng             text,
    valid_from      text,
    valid_to        text,
    source_system   text        NOT NULL,
    source_file     text        NOT NULL,
    batch_id        uuid        NOT NULL,
    loaded_at       timestamptz NOT NULL DEFAULT now()
);

-- Published mart: written only by the publish task, after the tests pass.
CREATE TABLE mart.daily_station_trips (
    row_key             text        PRIMARY KEY,
    calendar_date       date        NOT NULL,
    is_weekend          boolean     NOT NULL,
    source_system       text        NOT NULL,
    station_id          text        NOT NULL,
    station_name        text        NOT NULL,
    area                text        NOT NULL,
    member_casual       text        NOT NULL,
    trips               integer     NOT NULL,
    electric_trips      integer     NOT NULL,
    total_duration_sec  bigint      NOT NULL,
    avg_duration_sec    numeric(10,1) NOT NULL,
    run_id              text        NOT NULL,
    published_at        timestamptz NOT NULL
);

-- One row per successful publish; the checksum covers keys, rows and measures.
CREATE TABLE ops.publications (
    publication_id  serial      PRIMARY KEY,
    run_id          text        NOT NULL,
    scenario        text        NOT NULL,
    date_from       date        NOT NULL,
    date_to         date        NOT NULL,
    rows_published  integer     NOT NULL,
    checksum        text        NOT NULL,
    published_at    timestamptz NOT NULL DEFAULT now()
);
