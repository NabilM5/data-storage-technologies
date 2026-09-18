# HW 1. Converting an Open Export Into a Reproducible Lakehouse Table

## 1. Source Passport

| Item | Value |
|---|---|
| Name | Open-Meteo Historical Weather API (ERA5 reanalysis) |
| Official link | https://open-meteo.com/en/docs/historical-weather-api |
| Retrieval date | 2026-09-18 (`download_source.sh`). The S3 prefix `ingestion_date=2026-09-17` is a constant in `pipeline.py`, set on the first attempt and kept so that reruns overwrite the same objects |
| License / terms | CC BY 4.0; free for non-commercial use without a key, rate-limited |
| Source format and size | CSV, 12 files (one per city), 6,282,312 B = 5.99 MiB; each file has a 4-line metadata header and 17,544 data rows |
| Sample boundaries | 12 Russian cities, hourly, 2024-01-01 00:00 … 2025-12-31 23:00 UTC; `temperature_2m`, `relative_humidity_2m`, `precipitation` |
| Observation unit | One city in one UTC hour; key `(city, time)` |
| Expected row count | 12 × 731 days × 24 hours = 210,528 |
| Personal data | None |

**Research question:** which cities have similar daily temperature profiles?
Formulated before transformation; answered by the federated query (section 6
in `queries.sql`) after converting UTC to local time through a reference
table.

**Known limitations:**

- ERA5 is a reanalysis on a grid, not a station reading: "temperature in
  Moscow" is the value of the grid cell nearest to the coordinates.
- The archive is published with a delay of about five days; the chosen period
  is final, so downloads are reproducible.
- Time is in UTC, so without a time-zone reference the profiles of Sochi
  (UTC+3) and Vladivostok (UTC+10) are not comparable.
- `relative_humidity_2m` and `precipitation` may be `NULL` for individual
  hours; in this sample there are none.

## 2. Raw Data Without Hidden Changes

Download command (on the host, from `infra/`):

```bash
bash ../student-01-hw01/download_source.sh
```

Original files are uploaded to S3 as-is at
`s3://raw/student_01/weather/ingestion_date=2026-09-17/source/<city>.csv`.
Listing with sizes (`evidence/object-listing.txt`):

```text
=== s3://raw/student_01/weather/ ===
     8,473,160 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/prepared.csv
       522,561 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Arkhangelsk.csv
       521,902 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Astrakhan.csv
       527,268 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Irkutsk.csv
       523,214 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Kazan.csv
       525,754 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Krasnoyarsk.csv
       521,943 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Moscow.csv
       523,554 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Murmansk.csv
       525,863 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Novosibirsk.csv
       520,668 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Saint_Petersburg.csv
       521,692 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Sochi.csv
       523,654 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Vladivostok.csv
       524,239 B  s3://raw/student_01/weather/ingestion_date=2026-09-17/source/Yekaterinburg.csv
  -- objects: 13; total: 14,755,472 B (14.07 MiB)
```

The 12 `source/*.csv` objects have the same sizes as the downloaded files.
`prepared.csv` is a separate reproducible step (`prepare_csv` in
`pipeline.py`): it skips the Open-Meteo metadata block, adds `city` from the
file name and writes one tabular CSV; the originals are not modified.

## 3. Explicit Schema and DQ

Schema, types and rejection rules: [schema.md](schema.md). The CSV is read
with an explicit schema; `inferSchema` is not used.

| Check | Result (`evidence/spark-result.txt`) |
|---|---|
| Row count is greater than zero | 210,528 rows read = 210,528 rows written by the preparation step |
| Required columns exist | All 5 present |
| `time` parses | 0 rows with `null`; period 2024-01-01 00:00 … 2025-12-31 23:00 UTC |
| `temperature_2m` outside [-90, 60] | 0 rows |
| NULL share in `temperature_2m` | 0.000000 % (also 0 % for humidity and precipitation) |
| Reconciliation `raw = accepted + rejected` | 210,528 = 210,528 + 0 |

Rejected rows: 0. The reject path `s3://datalake/student_01/weather/rejects/`
contains a header-only CSV (74 B), a reproducible empty result; artificial
errors were not added. Rules check meaning, not only types: negative
temperature is valid, negative precipitation is rejected, so a change of
units in the source would fail visibly.

## 4. Parquet and Measurement

Parquet is partitioned by `city` (12 values, the query filters by city; not
by the unique key). Rationale: [schema.md](schema.md#partitioning).

| Metric | Prepared CSV | Parquet | How measured |
|---|---|---|---|
| Size, MiB (bytes / 1024²) | 8.08 | 1.70 | `getContentSummary` over the path (`dir_size_mb`) |
| Row count | 210,528 | 210,528 | `count()` over both sources, checked in code |
| One analytical query time, s | first 0.470; repeats 0.240, 0.185 | first 0.153; repeats 0.082, 0.093 | `time.perf_counter()` around `spark.sql(...).first()`; schema defined before the timer |
| Fields used by the query | 3 of 5 (`city`, `time`, `temperature_2m`) | 3 of 5 | `QUERY` in `pipeline.py`: `count(*)`, `avg(temperature_2m)`, Moscow, June–August 2024 |

Original raw size: 5.99 MiB, smaller than the prepared CSV because
preparation repeats the city name on every row. The first run is listed
separately, then two repeats per format with alternating order; all six runs
returned the same result (2,208 rows, mean 20.10503 °C).

Cache state: not cold. The checks and the Parquet write had already read the
data; the DataFrames are not cached in Spark (`persist` is not used).
Laptop: MacBook Pro, Apple M1, 8 GB RAM; Docker Desktop VM with 8 CPUs and
4.8 GiB shared by Spark (`local[*]`, driver 2 GB), MinIO, PostgreSQL and
Trino.

Conclusion: Parquet was faster in every pair, 3.1× on the first run and
2.0–2.9× on the repeats. The `city` filter reads one of 15 files and the
query reads 3 of 5 columns, while the CSV reader parses all rows and fields.
This is one run on an 8 MB sample, not a general law: the absolute times are
close to Spark's fixed overhead (about 0.08–0.10 s), and the spread between
CSV repeats is of the same order as the differences measured.

## 5. Iceberg

Table `lakehouse.student_01.weather`, partitioned by `(city, months(time))`.

| Write | Split condition | Rows |
|---|---|---|
| 1. Initial load (CTAS) | `time < TIMESTAMP '2025-01-01 00:00:00'` | 105,408 (2024, leap year) |
| 2. Additional batch (INSERT) | `time >= TIMESTAMP '2025-01-01 00:00:00'` | 105,120 (2025) |
| Total | | 210,528 = accepted rows |

The batches are non-overlapping and non-empty; the sum is checked in code,
and the `(city, time)` key is unique after the second write (no duplicates).

SQL (section 4 in `queries.sql`) and output (`evidence/trino-result.txt`):

```sql
SELECT snapshot_id, committed_at, operation,
       CAST(summary['total-records'] AS bigint) AS total_records,
       CAST(summary['added-records'] AS bigint) AS added_records
FROM lakehouse.student_01."weather$snapshots"
ORDER BY committed_at;
```

```text
"1683485453863271234","2026-09-18 16:08:10.799 UTC","overwrite","105408","105408"
"5892673281667954445","2026-09-18 16:08:16.624 UTC","append","210528","105120"
"8611983048728512972","2026-09-18 16:09:03.616 UTC","overwrite","105408","105408"
"6090014615143315104","2026-09-18 16:09:07.483 UTC","append","210528","105120"
"4992429410746976373","2026-09-18 16:22:57.432 UTC","overwrite","105408","105408"
"256367815133047278","2026-09-18 16:23:03.052 UTC","append","210528","105120"
```

How the snapshots differ: the pipeline was run three times, and
`CREATE OR REPLACE` keeps the history, so each run adds the same pair. The
`overwrite` snapshot (CTAS) lists only the 2024 files, `total-records` =
105,408; the `append` snapshot (INSERT) adds 105,120 records of 2025 and
points to both file sets, `total-records` = 210,528. The current snapshot is
`256367815133047278`; the row count does not grow on reruns.

Layer relationship: the catalog in PostgreSQL stores a pointer to the current
metadata file; the metadata file lists snapshots; a snapshot points to
manifests; manifests point to Parquet files in MinIO. Engines read the table
only through this chain, so appended files are not visible until commit.

## 6. Spark and Trino See One Table

Question: number of observations and mean temperature for Moscow,
June–August 2024.

| Engine | Query | Result |
|---|---|---|
| Spark | `spark_answer()` in `pipeline.py` | observations = 2208, mean_temperature = 20.105027 |
| Trino | section 3 in `queries.sql` | observations = 2208, mean_temperature = 20.105027 |

Reconciliation: the results match exactly; the total row count in Trino
(210,528) also matches Spark.

Precision: `temperature_2m` is `DECIMAL(5,1)`. Trino returns
`avg(decimal(p,s))` as `decimal(p,s)`, Spark as `decimal(p+4,s+4)`, so Trino
casts to `decimal(18,6)` and Spark to `decimal(14,2)`; both give
`decimal(18,6)`. `time` is stored as Iceberg `timestamptz` (Trino:
`timestamp(6) with time zone`) and both engines treat it as UTC, so the same
`TIMESTAMP` bounds select the same rows.

## 7. Federated SQL

Reference table `memory.default.city_climate` (`city`, `climate_zone`,
`utc_offset_hours`), 9 rows, section 5 in `queries.sql`.

- **Origin:** official Russian time zones (fixed offsets, no daylight-saving
  time) and a coarse climate label per city, filled manually.
- **Rule:** one row per city; the uniqueness check returned 0 rows.
- **Coverage:** 9 of 12 cities. Astrakhan, Arkhangelsk and Irkutsk are
  intentionally omitted; `LEFT JOIN` puts them in the `Unmatched` group with
  offset 0. Section 7 in `queries.sql`:

```text
before_join = 210528, after_join = 210528, unmatched_rows = 52632, unmatched_cities = 3
```

52,632 = 3 × 17,544: the rows of the three uncovered cities, nothing
multiplied or lost.

- **Catalogs involved:** `lakehouse` (Iceberg, JDBC catalog in PostgreSQL,
  files in MinIO) and `memory` (Trino's built-in non-persistent connector).
- **Where data physically lives:** Parquet files and Iceberg metadata in
  MinIO; the pointer to the current metadata in PostgreSQL; the reference
  table in Trino memory.
- **What Trino does:** resolves the table through the catalog, reads the
  needed Parquet files and columns from MinIO, reads the reference table from
  memory, and runs the join and aggregation in its own engine.
- **Why this is federation:** one query combines sources in different storage
  systems without copying them into one place; each connector accesses its
  own source.

`memory` is not persistent: after a Trino restart, section 5 of `queries.sql`
must be rerun.

**Answer.** Daily amplitude (max − min of the 24 hourly means in local time)
and the warmest and coldest local hour, 2024–2025:

| City | Zone | Amplitude, °C | Warmest hour | Coldest hour |
|---|---|---|---|---|
| Irkutsk | Unmatched | 8.58 | 7 UTC (15 local) | 22 UTC (6 local) |
| Astrakhan | Unmatched | 7.95 | 11 UTC (15 local) | 2 UTC (6 local) |
| Krasnoyarsk | continental | 6.46 | 15 | 5 |
| Sochi | subtropical | 6.06 | 14 | 5 |
| Yekaterinburg | continental | 5.72 | 15 | 5 |
| Moscow | continental | 5.68 | 16 | 5 |
| Novosibirsk | continental | 5.45 | 16 | 6 |
| Kazan | continental | 5.01 | 14 | 4 |
| Saint Petersburg | continental | 4.67 | 15 | 5 |
| Vladivostok | monsoon | 4.46 | 15 | 6 |
| Arkhangelsk | Unmatched | 4.30 | 11 UTC (14 local) | 1 UTC (4 local) |
| Murmansk | subarctic | 3.98 | 15 | 4 |

The shape is the same everywhere: warmest at 14–16 local, coldest at 4–6.
The cities differ in amplitude and form three groups: continental interior
(Irkutsk, Astrakhan, about 8 °C), temperate continental (Krasnoyarsk, Sochi,
Yekaterinburg, Moscow, Novosibirsk, Kazan, 5–6.5 °C) and coastal or polar
(Saint Petersburg, Vladivostok, Arkhangelsk, Murmansk, 4–4.7 °C), where the
sea and the polar day and night flatten the daily cycle. Without the
time-zone reference, Moscow (peak 13 UTC) and Vladivostok (peak 5 UTC) would
look like different profiles. The amplitude is an annual mean over two years
and mixes seasons; a seasonal breakdown would be the next step.

## 8. Architecture Conclusion

The S3 layer separated storage and compute: the original export stays
unchanged in MinIO, and any step can be recalculated without calling the API
again. Parquet fixed the types, columns and partitions, so the query reads
three of five fields and one city; on this sample that gave a 4.75× smaller
footprint and a 2–3× faster query, with Spark overhead dominating at this
size. Iceberg added an atomic, versioned list of files: the second batch
became visible only at commit, both engines read the same 210,528 rows, and
the snapshot history shows which write changed the table. Spark reads, checks
and writes the data; Trino answers SQL and joins the Iceberg table with the
`memory` catalog; both see one table through the shared JDBC catalog. The
weakest point is that everything runs on a single Docker VM with 4.8 GiB
shared by four services, with no fault tolerance, no scheduler and plaintext
educational passwords in the configs; the table also has no maintenance
policy, so reruns accumulate snapshots and `months(time)` produced 288 small
files with nothing to compact them.
