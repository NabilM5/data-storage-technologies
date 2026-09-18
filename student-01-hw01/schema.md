# Schema for `lakehouse.student_01.weather`

Types are defined explicitly in `pipeline.py` (`WEATHER_SCHEMA`). `inferSchema`
is not used: CSV does not store types, and automatic inference would make the
schema depend on the contents of a specific export.

Observation unit: **one city in one UTC hour**. The key is the `(city, time)`
pair; uniqueness is checked in `pipeline.py` after the second write.

| Field | Spark SQL type | Trino type | Nullable | Meaning and constraints |
|---|---|---|---|---|
| `time` | `TIMESTAMP` | `timestamp(6) with time zone` | no | Start of the observation hour, UTC. Source format: `2024-01-01T00:00`. Spark `TimestampType` is stored in Iceberg as `timestamptz`, which Trino exposes as `timestamp(6) with time zone`; values are UTC instants in both engines (Spark session time zone is set to UTC). An unparsable value becomes `null` and goes to rejects with reason `time_unparsed`. |
| `city` | `STRING` | `varchar` | no | City name in Latin characters, added during preparation from the raw file name. The source provides only coordinates, so city is our attribute, not a field from the export. |
| `temperature_2m` | `DECIMAL(5,1)` | `decimal(5,1)` | no | Air temperature at 2 m, °C. Validation range: -90..+60. Negative values are valid. `DECIMAL`, not `DOUBLE`, gives the same `AVG` in Spark and Trino without scale differences. |
| `relative_humidity_2m` | `INT` | `integer` | yes | Relative humidity, %. Valid range: 0..100. `NULL` is allowed: individual hours in the ERA5 archive may have no value. |
| `precipitation` | `DECIMAL(6,2)` | `decimal(6,2)` | yes | Hourly precipitation total, mm. Negative values are impossible and rejected. `NULL` is allowed for the same reason. |

The `Nullable` column describes the data contract enforced by the quality
checks in `pipeline.py`: a `NULL` in a "no" field sends the row to the reject
path. Physically, all five columns are `optional` in the Iceberg schema (Spark
writes nullable fields), so the constraint lives in the pipeline, not in the
table definition.

## Partitioning

| Layer | Partitions | Why |
|---|---|---|
| Parquet | `city` | 12 values; queries filter by city. Do not partition by `time`: about 17,500 unique hours would create many small files and metadata overhead, as discussed in the lecture. |
| Iceberg | `city, months(time)` | The same city filtering plus period pruning. `months()` gives 24 time partitions for two years, not 17,500. |

No layer is partitioned by a unique identifier.

## Rejected Rows

Path: `s3://datalake/student_01/weather/rejects/` (CSV with header).
The `reject_reason` column stores the first matching reason:

| `reject_reason` | Rule |
|---|---|
| `time_unparsed` | `time IS NULL` after reading with the schema |
| `city_missing` | `city` is empty or whitespace-only |
| `temperature_missing` | `temperature_2m IS NULL` |
| `temperature_out_of_range` | `temperature_2m` outside [-90, 60] |
| `humidity_out_of_range` | `relative_humidity_2m` is not `NULL` and outside [0, 100] |
| `precipitation_negative` | `precipitation` is not `NULL` and `< 0` |

The `raw = accepted + rejected` reconciliation is performed in `pipeline.py`
and fails with an error if the counts do not match.
