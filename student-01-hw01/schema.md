# Schema for `lakehouse.student_01.weather`

Types are defined explicitly in `pipeline.py` (`WEATHER_SCHEMA`);
`inferSchema` is not used. Observation unit: one city in one UTC hour, key
`(city, time)`.

| Field | Spark type | Trino type | Nullable | Meaning and constraints |
|---|---|---|---|---|
| `time` | `TIMESTAMP` | `timestamp(6) with time zone` | no | Start of the hour, UTC; source format `2024-01-01T00:00`. Stored as Iceberg `timestamptz`. Unparsable → `null` → rejected (`time_unparsed`). |
| `city` | `STRING` | `varchar` | no | City name from the raw file name; the source itself has only coordinates. |
| `temperature_2m` | `DECIMAL(5,1)` | `decimal(5,1)` | no | Air temperature at 2 m, °C. Valid range -90..+60; negative values are valid. `DECIMAL` keeps `AVG` comparable between Spark and Trino. |
| `relative_humidity_2m` | `INT` | `integer` | yes | Relative humidity, %. Valid range 0..100. `NULL` allowed (ERA5 may have gaps). |
| `precipitation` | `DECIMAL(6,2)` | `decimal(6,2)` | yes | Hourly precipitation, mm. Negative values are rejected. `NULL` allowed. |

`Nullable = no` is enforced by the checks in `pipeline.py` (a `NULL` sends the
row to rejects); in the Iceberg schema all columns are physically optional.

## Partitioning

| Layer | Partitions | Why |
|---|---|---|
| Parquet | `city` | 12 values; the query filters by city. Not by `time`: ~17,500 hours would give many small files. |
| Iceberg | `city, months(time)` | City filter plus period pruning; 24 month partitions for two years. |

No layer is partitioned by a unique identifier.

## Rejected Rows

Path: `s3://datalake/student_01/weather/rejects/` (CSV with header, original
values plus `reject_reason`, first matching rule):

| `reject_reason` | Rule |
|---|---|
| `time_unparsed` | `time IS NULL` after reading with the schema |
| `city_missing` | `city` empty or whitespace |
| `temperature_missing` | `temperature_2m IS NULL` |
| `temperature_out_of_range` | `temperature_2m` outside [-90, 60] |
| `humidity_out_of_range` | `relative_humidity_2m` not `NULL` and outside [0, 100] |
| `precipitation_negative` | `precipitation` not `NULL` and `< 0` |
