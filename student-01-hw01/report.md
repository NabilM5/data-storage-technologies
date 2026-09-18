# HW 1. Converting an Open Export Into a Reproducible Lakehouse Table

All numbers below are taken from the files in `evidence/`, produced by the
commands in `README.md`: `spark-result.txt` (pipeline run of 2026-09-18,
16:22 UTC), `trino-result.txt`, `object-listing.txt`, `compose-ps.txt`.
Nothing is quoted "by expectation".

## 1. Source Passport

| Item | Value |
|---|---|
| Name | Open-Meteo Historical Weather API (ERA5 reanalysis archive) |
| Official link | https://open-meteo.com/en/docs/historical-weather-api |
| Retrieval date | 2026-09-18, `download_source.sh` run at about 19:03 MSK (16:03 UTC); see the note on `ingestion_date` below |
| License / terms | Data under CC BY 4.0 as stated in the API documentation; the API is free for non-commercial use without a key, rate-limited (hence the 2 s pause between requests) |
| Source format and size | CSV, 12 files, one per city, 6,282,312 bytes = 5.99 MiB in total (520,668–527,268 bytes each). Each file is 2 metadata lines, a blank line, a header, and 17,544 data rows: `wc -l` gives 210,576 = 210,528 + 12 × 4 |
| Sample boundaries | 12 Russian cities (fixed coordinates in `download_source.sh`), hourly, 2024-01-01 00:00 … 2025-12-31 23:00 UTC; variables `temperature_2m`, `relative_humidity_2m`, `precipitation` |
| Observation unit | One city in one UTC hour; key `(city, time)` |
| Expected row count | 12 cities × 731 days × 24 hours = 210,528 (2024 is a leap year) |
| Personal data | None: gridded weather values at public coordinates |

**Note on `ingestion_date`.** The raw prefix in S3 is
`ingestion_date=2026-09-17` because `INGESTION_DATE` in `pipeline.py` was
fixed on the first attempt (2026-09-17) and deliberately kept constant so that
reruns overwrite the same objects instead of creating a second copy of the
source. The files were downloaded again on 2026-09-18; since 2024–2025 lies
well outside the ~5-day ERA5 publication delay, the archive for this period is
final and repeated downloads return the same rows (12 × 17,544).

**Research question:** which cities have similar daily temperature profiles?
It was formulated before transformation. The federated query answers it
(section 6 in `queries.sql`) by converting UTC to local time through a
reference table and comparing the daily amplitude and the warmest/coldest hour
by city.

**Known limitations:**

- ERA5 is a reanalysis, not a station reading: values are model fields on a
  grid with cells of tens of kilometres, so "temperature in Moscow" means the
  grid cell nearest to the requested coordinates (the API echoes the actual
  grid point in the file header, e.g. 55.782074, 37.576374 for Moscow instead
  of the requested 55.75, 37.62).
- The archive is published with a delay of about five days; the chosen period
  is final, so the pipeline is reproducible, but a "last week" sample would not
  be.
- Time is stored in UTC, so without a time-zone reference table the daily
  profiles of Sochi (UTC+3) and Vladivostok (UTC+10) are not comparable. This
  is why the reference table in the federated query has analytical meaning
  rather than being decorative.
- The schema allows `NULL` in `relative_humidity_2m` and `precipitation`
  because ERA5 can have gaps in the recent window; in this finalized sample
  there are none (section 3).
- Three cities (Astrakhan, Arkhangelsk, Irkutsk) are intentionally not covered
  by the reference table to show how the `Unmatched` group works.

## 2. Raw Data Without Hidden Changes

Download command (run on the host from `infra/`):

```bash
bash ../student-01-hw01/download_source.sh
```

Original files are uploaded to S3 without edits at
`s3://raw/student_01/weather/ingestion_date=2026-09-17/source/<city>.csv`.
Object listing with sizes (`evidence/object-listing.txt`, produced by
`list_objects.py`):

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

The 12 `source/*.csv` object sizes are byte-for-byte equal to the local files
written by `curl` (e.g. `Moscow.csv`: 521,943 B on disk and in S3), which is
the verifiable sense in which raw is "unchanged".

The prepared tabular CSV (`prepared.csv`, 8,473,160 B) is a separate
reproducible step (`prepare_csv` in `pipeline.py`): it reads the raw objects,
skips the Open-Meteo metadata block by locating the header row that starts with
`time,` programmatically, matches the required columns by name prefix
(`temperature_2m (°C)` → `temperature_2m`), adds `city` from the file name,
and writes one file. It never modifies the originals.

Why the original is stored before transformation: if the contract or typing
turns out to be wrong, history can be recalculated only from unchanged raw
data. Transforming before loading loses information irreversibly.

## 3. Explicit Schema and DQ

Schema, types, and rejection rules: [schema.md](schema.md). The CSV is read
with an explicit `StructType`; `inferSchema` is not used.

Check results from `evidence/spark-result.txt`:

| Check | Result |
|---|---|
| Row count is greater than zero | 210,528 rows read by Spark; equals the 210,528 data rows written by the preparation step |
| Required columns exist | All 5 present: `time`, `city`, `temperature_2m`, `relative_humidity_2m`, `precipitation` |
| `time` parses (rows with `null`) | 0 of 210,528; period 2024-01-01 00:00 … 2025-12-31 23:00 UTC |
| `temperature_2m` outside [-90, 60] | 0 rows (reason `temperature_out_of_range` never assigned) |
| NULL share in `temperature_2m` | 0.000000 % (also 0.000000 % for `relative_humidity_2m` and `precipitation`) |
| Reconciliation `raw = accepted + rejected` | 210,528 = 210,528 + 0 |

Rejected rows: **0**. The reject path
`s3://datalake/student_01/weather/rejects/` contains `_SUCCESS` and one
74-byte CSV consisting of the header only
(`time,city,temperature_2m,relative_humidity_2m,precipitation,reject_reason`),
i.e. a reproducible empty result. Artificial errors were not added: the
assignment states that a reproducible zero result is sufficient, and the
rules remain active for future exports in which the source changes.

Additionally, 12 distinct cities were found, and the `(city, time)` key was
verified unique (210,528 distinct pairs) after the Iceberg load.

Why check ranges and enumerations, not only types: if the source changes units
or adds a new value, the pipeline should fail visibly instead of silently
accepting garbage. Rules follow the physics of the field: negative temperature
is valid, negative precipitation is not.

## 4. Parquet and Measurement

Partitioning and rationale: [schema.md](schema.md#partitioning). Parquet is
partitioned by `city` (12 values, matches the query filter, no partitioning
by the unique key); Spark wrote 15 snappy files because three city partitions
were split across two input splits.

| Metric | Prepared CSV | Parquet | How measured |
|---|---|---|---|
| Size, MiB (bytes / 1024²) | 8.08 (8,473,160 B) | 1.70 (1,784,050 B, 15 files) | `getContentSummary` over the object / directory path, function `dir_size_mb` |
| Row count | 210,528 | 210,528 | `count()` over both sources, checked in code (script fails on mismatch) |
| One analytical query time, s | first 0.470; repeats 0.240, 0.185 | first 0.153; repeats 0.082, 0.093 | `time.perf_counter()` around `spark.sql(...).first()`; schema and temp views defined before the timer |
| Fields used by the query | 3 of 5 (`city`, `time`, `temperature_2m`) | 3 of 5 | Query text in `pipeline.py` (`QUERY`): `count(*)`, `avg(temperature_2m)` for Moscow, June–August 2024 |

Size ratio CSV / Parquet = 4.75. Original raw size (12 Open-Meteo files before
preparation): 5.99 MiB (6,282,312 B). It is *smaller* than the prepared CSV
because preparation repeats the city name on every one of 210,528 rows
(about +2.2 MB) while removing only four short metadata lines per file.

Measurement order: the first measurement is recorded separately, followed by
two repeats for each format; the order of formats alternates between trials so
warm-up does not always benefit one of them. All six timings are listed above,
and all six returned the same result (2,208 observations, mean 20.10503 °C).

Cache state: measurements are **not cold**. Quality checks and the Parquet
write had already read the data, so the MinIO objects were likely in the OS
page cache and the JVM was JIT-warmed. Neither DataFrame is cached in Spark:
an earlier version of the script called `persist()` on the accepted DataFrame,
which made the "CSV" timings 0.09–0.25 s — the speed of Spark's in-memory
columnar cache, not of CSV parsing — and that comparison was discarded as
invalid. Laptop limits: MacBook Pro, Apple M1 (8 cores), 8 GB RAM, macOS 27.0;
Docker Desktop VM with 8 CPUs and 4.8 GiB, shared by Spark (`local[*]`,
driver 2 GB, storage memory 1,048.8 MiB), MinIO, PostgreSQL and Trino
(2 GB limit).

Conclusion: Parquet was faster in every pair — 3.1× on the first run
(0.470 vs 0.153 s) and 2.0–2.9× on the repeats; medians 0.240 s vs 0.093 s.
The mechanism is visible in the layout: the `city = 'Moscow'` predicate prunes
to one of 15 files (149,290 B), the projection reads 3 of 5 columns, and
row-group statistics on `time` skip most of the remaining pages, whereas the
CSV reader must tokenize all 210,528 lines and all five fields before the
filter is applied. Two qualifications keep this from being a general law:
(1) at 8 MB the absolute times are dominated by fixed Spark overhead — task
scheduling and result collection alone account for roughly 0.08–0.10 s, which
is why the Parquet repeats sit at that floor; (2) the spread between CSV
repeats (0.185–0.240 s) is of the same order as the differences being
measured, so with n = 3 the ratio is an indication of direction, not a precise
estimate. On a dataset this small, a different partitioning or a query that
touches all cities could shrink or invert the gap.

The number of fields in a query is not equal to the bytes actually read:
Parquet skips extra columns and partitions, but on a dataset of about 200k rows
the speedup may not appear or may even be negative. Acceleration is not the
goal of the assignment; any measured slowdown would be recorded as-is.

## 5. Iceberg

Table: `lakehouse.student_01.weather`, partitioned by `(city, months(time))`.

| Write | Split condition | Rows |
|---|---|---|
| 1. Initial load (CTAS) | `time < TIMESTAMP '2025-01-01 00:00:00'` (calendar year 2024) | 105,408 = 12 × 366 × 24 |
| 2. Additional batch (INSERT) | `time >= TIMESTAMP '2025-01-01 00:00:00'` (calendar year 2025) | 105,120 = 12 × 365 × 24 |
| Total | — | 210,528 = accepted rows |

The batches are non-overlapping (different calendar years) and both are
non-empty. Their sum matches the full accepted dataset; this is checked in
code, and the script fails if the check does not match. Uniqueness of the
`(city, time)` pair is checked after the second write: 210,528 distinct pairs
for 210,528 rows, so no duplicates.

SQL (section 4 in `queries.sql`) and its text output from
`evidence/trino-result.txt`:

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

How the snapshots differ. The history contains six snapshots because the
pipeline was run three times (16:08, 16:09 and 16:22 UTC; the last run is the
one in `spark-result.txt`). Each run produces exactly the same pair:

- `CREATE OR REPLACE TABLE … AS SELECT … WHERE time < '2025-01-01'` commits an
  **`overwrite`** snapshot whose manifest lists only 2024 files;
  `total-records` is reset to 105,408. `CREATE OR REPLACE` keeps the metadata
  lineage, which is why earlier snapshots remain visible instead of being
  discarded — the assignment allows a longer history on reruns.
- `INSERT INTO … WHERE time >= '2025-01-01'` commits an **`append`** snapshot
  that adds 105,120 records (2025 files) and points to the union of both file
  sets: `total-records` = 210,528.

For the final run the two snapshots are `4992429410746976373` (overwrite,
16:22:57.432 UTC, 105,408 → 105,408) and `256367815133047278` (append,
16:23:03.052 UTC, 105,408 → 210,528); the latter is the current snapshot that
both engines read in section 6. Rerunning the script does not accumulate
rows: the final count is 210,528 after every run, which is the idempotency
property the script asserts.

The `$files` metadata table of the current snapshot shows 288 data files with
210,528 records and 1,927,648 bytes — one file per `(city, month)` partition
(12 × 24). This is the price of the `months(time)` partition: at this volume
the average file is 6.7 KB, well below the size at which Parquet's columnar
encoding pays off. The layout is justified by the query pattern (city and
period pruning), but in a production table it would call for periodic
compaction (`rewrite_data_files`) or a coarser transform such as
`years(time)`.

Layer relationship: the catalog in PostgreSQL stores a pointer to the current
table metadata file; the metadata file lists snapshots; a snapshot points to
manifests; manifests point to concrete Parquet files in MinIO. The engine reads
the table only through this list, so appended files are not visible until the
commit that publishes them.

Practical meaning of history: if the source changes units (for example, starts
sending values in different units), averages will change abruptly, and the
previous snapshot identifies exactly which write introduced the change and
allows the earlier state to be queried for comparison.

## 6. Spark and Trino See One Table

The same question in both engines: the number of hourly observations and the
mean temperature for Moscow in June–August 2024.

| Engine | Query | Result |
|---|---|---|
| Spark | `spark_answer()` in `pipeline.py` — `avg(CAST(temperature_2m AS decimal(14,2)))` | observations = 2208, mean_temperature = 20.105027 |
| Trino | section 3 in `queries.sql` — `avg(CAST(temperature_2m AS decimal(18,6)))` | observations = 2208, mean_temperature = 20.105027 |

Reconciliation: **both numbers match digit for digit** (2208 rows = 92 days ×
24 hours; 20.105027 °C). The row count in Trino's control query (210,528, 12
cities, 2024-01-01 00:00 … 2025-12-31 23:00 UTC) also matches the Spark
reconciliation after the second write.

Precision alignment: `temperature_2m` is stored as `DECIMAL(5,1)`. The two
engines type `AVG` differently: Trino returns `avg(decimal(p,s))` as
`decimal(p,s)`, while Spark widens it to `decimal(p+4,s+4)`. So Trino uses
`avg(CAST(... AS decimal(18,6)))` and Spark uses
`avg(CAST(... AS decimal(14,2)))`; both produce `decimal(18,6)` and the values
compare digit for digit. With `DOUBLE`, differences in the last digits would
be expected. The same applies to time: Spark's `TimestampType` is stored as
Iceberg `timestamptz`, which Trino exposes as `timestamp(6) with time zone`;
both engines interpret the values as UTC instants (Spark session time zone is
set to UTC), so the `TIMESTAMP '2024-06-01 00:00:00'` bounds select the same
rows.

## 7. Federated SQL

Reference table: `memory.default.city_climate` (`city`, `climate_zone`,
`utc_offset_hours`), 9 rows, section 5 in `queries.sql`.

- **Origin:** official Russian time zones (fixed offsets; Russia has had no
  daylight-saving time since 2014, so a single integer per city is exact for
  the whole 2024–2025 period) and a coarse Köppen-style climate label per city,
  filled manually.
- **Rule:** one row per city, unique `city` key — the check in section 5
  (`GROUP BY city HAVING count(*) > 1`) returned 0 rows.
- **Coverage:** 9 cities out of 12. Astrakhan, Arkhangelsk, and Irkutsk are
  intentionally not covered; `LEFT JOIN` assigns them to the `Unmatched` group
  with `utc_offset_hours = 0`, and rows are not lost. Preservation is checked
  in section 7 of `queries.sql`:

```text
before_join = 210528, after_join = 210528, unmatched_rows = 52632, unmatched_cities = 3
```

52,632 = 3 × 17,544, i.e. exactly the rows of the three uncovered cities: the
join neither multiplied nor dropped anything.

Explanations:

- **Which catalogs are involved:** `lakehouse` (Iceberg connector, JDBC
  catalog in PostgreSQL, files in MinIO) and `memory` (built-in non-persistent
  Trino connector).
- **Where data physically lives:** Parquet data files and Iceberg metadata /
  manifests are in MinIO (S3); the pointer to the current metadata file is in
  PostgreSQL; the reference table lives in Trino process memory.
- **What Trino does during the query:** the coordinator resolves
  `lakehouse.student_01.weather` through the JDBC catalog, reads the current
  metadata and manifests, plans splits over only the needed Parquet files and
  columns (`city`, `time`, `temperature_2m`), reads the 9-row reference table
  from memory, and executes the join and aggregation in its own engine. The
  Iceberg table is not copied into `memory` beforehand.
- **Why this is federation:** one SQL statement combines sources that live in
  different storage systems and formats without first reloading them into a
  common store; each connector is responsible for access to its own source,
  and the engine is responsible for the join.

`memory` is not persistent storage: after restarting Trino, section 5 of
`queries.sql` must be run again.

**Answer to the research question.** The final query of section 6 in
`queries.sql` computes, per city, the daily amplitude (max − min of the 24
hourly mean temperatures in local time) and the warmest and coldest local hour
over 2024–2025 (`evidence/trino-result.txt`):

| City | Zone (reference) | Daily amplitude, °C | Warmest hour | Coldest hour |
|---|---|---|---|---|
| Irkutsk | Unmatched | 8.58 | 7 UTC (= 15 local, UTC+8) | 22 UTC (= 6 local) |
| Astrakhan | Unmatched | 7.95 | 11 UTC (= 15 local, UTC+4) | 2 UTC (= 6 local) |
| Krasnoyarsk | continental | 6.46 | 15 | 5 |
| Sochi | subtropical | 6.06 | 14 | 5 |
| Yekaterinburg | continental | 5.72 | 15 | 5 |
| Moscow | continental | 5.68 | 16 | 5 |
| Novosibirsk | continental | 5.45 | 16 | 6 |
| Kazan | continental | 5.01 | 14 | 4 |
| Saint Petersburg | continental | 4.67 | 15 | 5 |
| Vladivostok | monsoon | 4.46 | 15 | 6 |
| Arkhangelsk | Unmatched | 4.30 | 11 UTC (= 14 local, UTC+3) | 1 UTC (= 4 local) |
| Murmansk | subarctic | 3.98 | 15 | 4 |

For the three `Unmatched` cities the query reports UTC hours (offset 0); the
local equivalents are given in parentheses. The amplitude itself is invariant
under the shift, so it is comparable across all twelve cities.

Two findings follow. First, the *shape* of the profile is the same everywhere:
every city is warmest at 14–16 local time and coldest at 04–06, consistent
with the lag of surface air temperature behind solar noon. Second, cities
differ in *amplitude*, and they cluster into three groups:

1. **Large amplitude (≈ 8–8.6 °C):** Irkutsk and Astrakhan — dry continental
   interiors (Siberian plateau, semi-arid Caspian lowland) where clear skies
   and low humidity let the surface heat and cool strongly.
2. **Medium (≈ 5–6.5 °C):** Krasnoyarsk, Sochi, Yekaterinburg, Moscow,
   Novosibirsk, Kazan — the temperate continental core; Sochi falls here
   despite its Black Sea coast, which a seasonal breakdown would be needed to
   explain.
3. **Small (≈ 4–4.7 °C):** Saint Petersburg, Vladivostok, Arkhangelsk,
   Murmansk — maritime or high-latitude sites where the sea (Baltic, Sea of
   Japan, White Sea, Barents Sea) and, at 65–69° N, the polar day/night
   flatten the diurnal cycle.

The per-hour Spark output in `spark-result.txt` corroborates this for two
cities on opposite sides of the country: Moscow peaks at 13 UTC = 16 local
(10.45 °C) and Vladivostok at 5 UTC = 15 local (9.71 °C); without the
time-zone reference the two curves appear shifted by ten hours and would be
classified as different profiles.

Caveats: the profile is an annual mean over two years, so it mixes seasons —
for Murmansk in particular the diurnal cycle nearly vanishes during polar
night and polar day, and a seasonal breakdown would be the natural next
question; ERA5 values are grid-cell means, not station observations; and the
climate labels are a manual classification used only to annotate the result,
not to compute it.

## 8. Architecture Conclusion

The S3 layer separated storage and compute: the original export remains
unchanged in MinIO with byte-for-byte verifiable sizes, and any step can be
recalculated from raw data without calling the external API again. Parquet
defined the file layout — types, columns, and partitions — so that the
analytical query reads three of five fields and one city, which on this 8 MB
sample gave a 4.75× smaller footprint and a 2–3× faster query, with the
caveat that fixed Spark overhead dominates at this scale. Iceberg added what a
directory of Parquet files does not have: an atomic, versioned list of files,
so the second batch became visible only at commit, the same 210,528 rows are
read by both engines, and the `$snapshots` history shows which write changed
the table. Spark and Trino are needed for different tasks: Spark reads,
validates, and writes the data, while Trino answers interactive SQL and joins
the Iceberg table with the `memory` catalog; both see one table through the
shared JDBC catalog in PostgreSQL. The weakest point of the current solution
is that everything runs on a single Docker VM with 4.8 GiB shared by four
services — there is no fault tolerance, no isolation, and no scheduler, and
the credentials are educational plaintext in `spark-defaults.conf` and
`lakehouse.properties`. Two smaller weaknesses became visible in this work:
the table has no maintenance policy, so every rerun adds two snapshots and the
`months(time)` partitioning produced 288 files averaging 6.7 KB with nothing
to compact or expire them; and the stand's reproducibility depends on the
local Docker image cache — a corrupted layer of the Trino image blocked the
Trino half of the pipeline until the base image was switched to the previous
release (see `README.md`).
