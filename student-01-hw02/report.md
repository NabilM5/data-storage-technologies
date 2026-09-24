# HW 2. Report: Citi Bike daily station trips

Stand, model and commands: [README.md](README.md). The numbers below come from
`evidence/runs.txt` (DAG runs and case queries, produced by
`scripts/run_all.sh`), `evidence/sql_results.txt` (`sql/report_queries.sql` on
the published mart) and `data/independent_check.py`.

## 1. SQL results on the published mart

Control slice: 4,083 trips, 2026-08-03 to 2026-08-09, six start stations.
Expected values were computed from the slice file with pandas before the
pipeline was run.

Q1. Trips per day and station, with the member share.

| Expected | Observed (`sql_results.txt`, Q1) |
|---|---|
| Day totals 406, 698, 577, 692, 555, 618, 537; week 4,083 | The same day totals; 42 rows (7 days x 6 stations) summing to 4,083 |
| `JC115` busiest every day | 128, 213, 165, 204, 157, 184, 144 |
| Member share lower at the weekend | Weekdays 54 to 91 %, Saturday and Sunday 45 to 76 % |

Q2. Duration by area, rider type and day type.

| Area | Rider | Day type | Trips | Avg duration, min | Electric, % |
|---|---|---|---|---|---|
| Hoboken | casual | weekday | 354 | 10.4 | 65.0 |
| Hoboken | casual | weekend | 232 | 16.5 | 60.8 |
| Hoboken | member | weekday | 936 | 6.9 | 62.6 |
| Hoboken | member | weekend | 275 | 9.6 | 64.0 |
| Jersey City | casual | weekday | 298 | 9.5 | 72.5 |
| Jersey City | casual | weekend | 205 | 15.3 | 72.2 |
| Jersey City | member | weekday | 1,340 | 6.7 | 55.2 |
| Jersey City | member | weekend | 443 | 8.1 | 55.3 |

Expected: casual trips longer than member trips, weekend trips longer than
weekday trips. Both hold in both areas. Casual weekend trips are about 1.6
times longer than casual weekday trips. The average is
`sum(total_duration_sec) / sum(trips)`, not a mean of daily means.

Q3. Control cell, 2026-08-05 and `JC115`.

| Rider | Trips | Electric | Total duration, s | Avg, s |
|---|---|---|---|---|
| member | 145 | 64 | 57,115 | 393.9 |
| casual | 20 | 14 | 9,252 | 462.6 |

## 2. Independent control

`data/independent_check.py` reads the original month zip with pandas, not the
slice and not the database, and recomputes the Q3 cell:

```text
source file: JC-202608-citibike-tripdata.csv.zip; day=2026-08-05; start_station_id=JC115
               trips  electric_trips  total_duration_sec  avg_duration_sec
casual            20              14                9252             462.6
member           145              64               57115             393.9
total trips: 165
```

All eight values match the published mart.

The check also found an error in the first version of the model. Trips and
electric counts matched, but durations were 57,190 and 9,262 s, which is 75 s
and 10 s too high. The source timestamps carry milliseconds, and
`extract(epoch ...)::integer` in PostgreSQL rounds the fraction while the
check truncated it. Duration is now `floor(...)` in `stg_trips`, and the two
calculations agree.

## 3. DAG, interval and roles

`hw02_citibike_elt` runs `check_input`, `load_raw`, `dbt_build_candidate`,
`dbt_test`, `publish`. A baseline run takes about 25 s.

Interval. `date_from` and `date_to` are DAG parameters, default
2026-08-03 to 2026-08-09, and are passed to dbt as vars. The mart candidate
and the publication cover exactly this interval. The run date is never used as
a business date. On a daily schedule (`0 6 * * *`) the run of day D would take
`date_from = date_to = D-1`, the previous full day.

Graph and log. The graph shows the five stages and which one failed.
Individual dbt models and tests are in the `dbt_build_candidate` and
`dbt_test` logs, for example `FAIL 1 business_rule_trip_duration_positive` and
`Got 854 results`. The loader log shows the batches it replaced and the
scenario file it loaded.

Roles. PostgreSQL executes every transformation as SQL. dbt describes the
models, their dependencies through `ref()` and `source()`, and the tests.
Airflow orders the stages, passes the interval and decides whether `publish`
may run. This is ELT because the source rows are loaded into `raw` unchanged
first, and all transformations happen inside the database afterwards.

Candidate and published. dbt writes `mart_candidate`. `publish` copies the
interval into `mart` in one transaction: delete the interval, insert from the
candidate, write a row to `ops.publications` with a checksum. When `dbt_test`
fails, `publish` is `upstream_failed` and `mart` keeps the previous
publication. The dds tables are rebuilt, but the consumer does not read them.

Serialization. `max_active_runs=1` plus `pg_advisory_xact_lock(4202)` in
`publish`, so two runs never write the mart at the same time.

## 4. Four diagnostic cases

Each run starts from the baseline files plus at most one scenario file from
`data/scenarios/`. Before every case the previous run had published
successfully. Task states and query output are in `evidence/runs.txt`.

| Case | Changed rows | Forecast | Observation | Explanation | Confirmation |
|---|---|---|---|---|---|
| Duplicate | `duplicate.csv`: ride `221F05B20357A5C2` (2026-08-05, JC115, casual) sent a second time, identical | No extra fact, the cell stays at 20 trips, checksum unchanged | raw 2 rows, stg 1, fact 1; cell 20 / 9,252 s unchanged; publication #4 checksum `32b30791...` equals the baseline | `stg_trips` keeps one row per `(source_system, ride_id)`, latest loaded; `trip_key` is unique in the fact | `unique_fact_trip_trip_key` PASS; raw / stg / fact query in `runs.txt` |
| History overlap | `history_overlap.csv`: a second JC115 version valid from 2026-08-05 while the first is still open | SCD2 tests fail, publication blocked | `dbt_test` failed: `scd2_dim_station_no_overlap` 1 row, `scd2_one_station_version_per_trip` 854, `unique_fact_trip_trip_key` 854, `unique_daily_station_trips_row_key` 10; `publish` upstream_failed; last publication still #7 | Two open intervals for one station make the fact join ambiguous: 1,708 fact rows for 854 trips | `dbt/tests/scd2_*.sql`; `dim_station` shows both versions; `ops.publications` unchanged |
| Late correction | `late_fix.csv`: ride `9EF6A52D8FA471A1` (2026-08-05, JC115, member) resent with `ended_at` +15 min, `started_at` unchanged | Same trip count, member duration +900 s, a rerun gives the same result | duration 209 to 1,109 s; cell 145 trips, 57,115 to 58,015 s, avg 393.9 to 400.1; checksum `97185e30...`; the rerun gave the same cell and checksum (publications #5 and #6) | The business date comes from `started_at`, so the fix lands in the right past day; the newer version replaces the older one | raw holds both versions, stg one; "late_fix repeated" in `runs.txt` |
| Second source | `second_source.csv`: 3 rides from `partner_feed` reusing 3 `citibike_jc` ride ids of 2026-08-06, other station and duration | 3 new facts, nothing merged, the `citibike_jc` rows unchanged | fact 4,083 `citibike_jc` + 3 `partner_feed`; each shared id has two facts, `citibike_jc@JC115` and `partner_feed@HB609`; mart 86 rows, the original 84 plus 2 | Trip identity is `(source_system, ride_id)`, and the mart carries `source_system` | `unique_fact_trip_trip_key` PASS on 4,086 rows; shared-id query in `runs.txt` |

Assumptions for the synthetic inputs: the partner system uses the same
16-character id space, which is the "same local id" case from the assignment.
A station rename is a plausible real event; only its overlapping validity is
the error under test.

## 5. Quality checks, broken input and recovery

Checks in `dbt/models/schema.yml` and `dbt/tests/`: `not_null` and `unique` on
`trip_key`, `station_key`, `date_key` and `row_key`; `relationships` from
`fact_trip.station_key` to `dim_station` and from `fact_trip.date_key` to
`dim_date`; `accepted_values` for `rideable_type`, `member_casual` and `area`;
the business rule `business_rule_trip_duration_positive`, which returns trips
with `ended_at <= started_at`; the SCD2 rules
`scd2_dim_station_no_overlap` and `scd2_one_station_version_per_trip`; and
`mart_reconciles_with_fact`, which compares mart totals with fact totals for
the interval. 34 tests in total, all passing on the baseline.

Broken input: `zero_duration.csv` adds trip `TEST0000ZERODUR1` with
`ended_at = started_at`. `dbt_test` failed on
`business_rule_trip_duration_positive` with `FAIL 1`, and the returned row is
`citibike_jc:TEST0000ZERODUR1, 2026-08-05 00:16:14.867 to 00:16:14.867,
duration 0`. `publish` did not run and `mart` still showed publication #7.
The next baseline run removed the scenario batch, all tests passed, and
publication #8 was written with the baseline checksum `32b30791...`.

## 6. Repeat comparison

The checksum is md5 over all `row_key | trips | electric_trips |
total_duration_sec` of the interval, ordered by key, so it covers keys, rows
and measures rather than `count(*)` alone.

| # | Scenario | Rows | Trips | Total duration, s | Checksum |
|---|---|---|---|---|---|
| 1, 2, 3 | baseline | 84 | 4,083 | 2,113,343 | `32b30791faf47178a5a1f6c8e9299be4` |
| 4 | duplicate | 84 | 4,083 | 2,113,343 | `32b30791...`, the same |
| 5, 6 | late_fix, repeated | 84 | 4,083 | 2,114,243 | `97185e302636dbbf39e8d5feb83b6fcc`, the same for both |
| 7 | second_source | 86 | 4,086 | | `f6a6cebb960bff37760b17b141b4990d` |
| 8 | baseline (recovery) | 84 | 4,083 | 2,113,343 | `32b30791...`, same as 1 to 3 |

Two runs on the same correct input give identical keys, rows and measures
(1 to 3 and 8, and 5 and 6). The loader is idempotent per batch, staging keeps
one version per key, and `publish` replaces the interval instead of appending.

## 7. Limitations

- Key uniqueness does not prove completeness. A missing day or a truncated
  source file passes every test as long as `check_input` finds at least one
  row. A row-count contract with the source would be needed.
- "Latest loaded wins" orders versions by load time, not by a version stamp
  from the source. A late redelivery of an old version would overwrite a newer
  one. The source file has no version field to use instead.
- Timestamps are local New York time without a zone, so the ambiguous hour at
  the autumn DST change would need handling. It does not occur in August.
- The dds layer is rebuilt in full on every run. That is fine for 4,083 rows
  but not for a full month of 111,227 trips, where the fact would have to
  become incremental.
- Two of the four cases use synthetic rows, because the real feed has neither
  station versions nor a second system.
- The stand is a teaching one: `airflow standalone`, one PostgreSQL for both
  the DWH and the Airflow metadata, and local passwords in
  `docker-compose.yml`.

## 8. Screenshots

`evidence/screenshots/` with captions in its README: a successful run, the 34
passing tests, the run blocked by `dbt_test` with `publish` upstream_failed,
its audit log, the run history, the stand health, the DAG code, and the task
instances.
