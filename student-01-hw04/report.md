# HW 4 report: analytical mart in ClickHouse

## 1. Goal and data

I compared PostgreSQL with ClickHouse on the same Citi Bike data. The source is
the August 2026 Jersey City file reused from HW2. It contains 111,227 trips.

One source row is one trip. Its business key is
`source_system || ':' || ride_id`. Full rules for time, missing values,
duplicates and corrections are in the data contract in `README.md`.

The source has no money column. I added a clearly synthetic teaching fare so I
could test exact money types:

```text
$2.50 + $0.20 per completed minute + $1.00 for a casual rider
```

It is calculated in integer cents and stored as `numeric(12,2)` in PostgreSQL
and `Decimal(12,2)` in ClickHouse.

## 2. Hypotheses

1. For a station and one day, the ClickHouse order
   `(start_station_id, started_at, trip_key)` will read less data than the order
   `trip_key`.
2. For a query that uses almost all rows, the two ClickHouse orders will have
   similar work.
3. The daily mart will read less data than either detailed ClickHouse table.
4. PostgreSQL can be faster on the small selective query because its B-tree
   index finds only the needed rows.

## 3. Physical design

PostgreSQL uses a primary key on `trip_key` and a B-tree index on
`(start_station_id, started_at)`.

I loaded the same ClickHouse rows into two monthly partitioned MergeTree tables:

| Table | `ORDER BY` | Reason |
|---|---|---|
| `trips_by_station` | `(start_station_id, started_at, trip_key)` | Matches the station and date filter in Q1 |
| `trips_by_key` | `trip_key` | Alternative order that does not match the analytical filter |

Monthly partitions follow the data lifecycle. The sort key is more important
for finding one station and one day inside the large August partition.

The mart grain is calendar day × start station × rider type. Its target is a
`SummingMergeTree`. It stores trip count, electric-trip count, duration sum and
fare sum. It does not store an average. Average duration is calculated as
`sum(duration_sum) / sum(trips)`.

## 4. Correctness

Correctness was checked before timing. I compared a canonical representation of
every row, per-day totals and every result group.

| Check | Result |
|---|---|
| PostgreSQL rows / distinct keys | 111,227 / 111,227 |
| ClickHouse rows / distinct keys in each table | 111,227 / 111,227 |
| Duration sum | 72,967,687 seconds in all tables |
| Fare sum | 541,610.10 in all tables |
| Earliest / latest start | 2026-07-31 16:27:10.701 / 2026-08-31 23:53:36.692 |
| Full-row SHA-256 in all three copies | `10280428fc41ea33fdf919f18d7fe2d849998aa42108cf050f72c45818b2ac4f` |
| Per-day totals | Equal |
| Q1 and Q2, detailed tables and mart | Every group and metric is equal |

Money was compared as integer cents, so values such as `3.30` and `3.3` cannot
create a false text mismatch. Times keep millisecond precision. PostgreSQL uses
New York wall-clock `timestamp`; ClickHouse uses
`DateTime64(3, 'America/New_York')`.

The complete output is in `evidence/checks.txt`.

## 5. Incremental materialized view

I used this controlled sequence:

1. Load 86,325 history rows before 2026-08-25 into the source table.
2. Create the MV. Its target contains zero rows because an incremental MV only
   processes new inserts.
3. Backfill only the rows before 2026-08-25.
4. Insert 14,356 rows from 2026-08-25 through 2026-08-28.
5. Insert 10,546 rows from 2026-08-29 onward.

| Stage | Trips | Duration sum | Fare sum |
|---|---:|---:|---:|
| After history backfill | 86,325 | 57,043,212 | 421,929.10 |
| After batch 1 | 100,681 | 65,332,722 | 487,533.50 |
| After batch 2 | 111,227 | 72,967,687 | 541,610.10 |

At every stage, every day × station × rider group was compared with a fresh
aggregation of the detailed table. Keys, trip count, electric-trip count,
duration sum, fare sum and derived average were equal.
The final mart has 6,272 physical partial rows. The report query sums these rows
by its logical group, so it is correct before and after background merges.

Repeated delivery is not idempotent in this simple design. If the same batch is
inserted again, the MV adds the measures again. A production loader would need
a processed-batch table or partition replacement. Updating an old source row
also does not reverse the old mart contribution, so corrections require a
controlled rebuild of the affected period.

## 6. Measurement method

Both databases ran locally in Linux containers. Each service was limited to
2 CPUs and 2 GB RAM. PostgreSQL was 16.9 and ClickHouse was 25.8.33.6.
ClickHouse queries used `max_threads=2`. PostgreSQL used its normal optimizer,
the suitable B-tree index and a fresh `ANALYZE`.

For every variant I discarded one warm-up, then recorded five runs. PostgreSQL
time comes from `EXPLAIN (ANALYZE, BUFFERS)`. ClickHouse time, rows, bytes and
memory come from `system.query_log`. No ClickHouse merge was running after the
measurements.

Q1 is selective: station `JC115` on 2026-08-05. Q2 covers the complete source
date range and groups by area and rider type.

## 7. Results

### Q1: selective station and day

| System / structure | Five times, ms | Median | Min–max | Rows read | Bytes read |
|---|---|---:|---:|---:|---:|
| PostgreSQL B-tree | 0.494; 0.415; 0.398; 0.434; 0.392 | 0.415 | 0.392–0.494 | 165 result rows from index scan | 136 cached blocks |
| CH station/time order | 4; 4; 4; 4; 4 | 4 | 4–4 | 16,384 | 377,237 |
| CH trip-key order | 7; 7; 7; 6; 6 | 7 | 6–7 | 111,207 | 2,575,560 |
| CH daily mart | 4; 3; 2; 4; 2 | 3 | 2–4 | 4,846 | 175,906 |

The station/time order read 6.8 times fewer rows and bytes than the trip-key
order. Its plan selected 2 of 13 August granules. The trip-key primary index
could not use the station/date predicate and selected 14 of 14 granules.

The station/time median was 4 ms versus 7 ms for trip-key order, but the small
difference is close to fixed query overhead and millisecond timing variation.
PostgreSQL was fastest because its B-tree bitmap scan found the 165 needed rows.
The mart had the lowest ClickHouse median at 3 ms.

### Q2: wide date-range aggregate

| System / structure | Five times, ms | Median | Min–max | Rows read | Bytes read |
|---|---|---:|---:|---:|---:|
| PostgreSQL | 27.476; 22.451; 21.981; 22.316; 22.311 | 22.316 | 21.981–27.476 | Parallel sequential scan | 2,640 cached blocks |
| CH station/time order | 14; 13; 12; 14; 12 | 13 | 12–14 | 111,227 | 2,561,678 |
| CH trip-key order | 16; 80; 23; 16; 15 | 16 | 15–80 | 111,227 | 2,564,462 |
| CH daily mart | 4; 3; 4; 3; 4 | 4 | 3–4 | 6,272 | 230,325 |

Q2 needs almost every trip, so both detailed ClickHouse tables read all 111,227
rows and about the same number of bytes. The sort order gives no meaningful
advantage here. PostgreSQL correctly chose a parallel sequential scan because
an index is not useful when nearly all rows are needed.

The mart read 17.7 times fewer rows and about 11.1 times fewer bytes than the
station-ordered detailed table. Its 4 ms median was about 3.3 times lower than
that detailed-table median.

The raw times are in `evidence/measurements.csv`; full plans and environment
settings are in `evidence/plans.txt` and
`evidence/benchmark_environment.txt`.

## 8. Conclusions and costs

The hypotheses were supported. The matching ClickHouse sort key greatly reduced
read work for Q1, but it did not guarantee lower time on this small cached data.
For Q2, column storage and the pre-aggregated mart were more useful than the
difference between detailed-table sort orders.

This design has costs:

- Keeping two detailed ClickHouse copies uses about 12 MB instead of about 6 MB.
- Changing a MergeTree sort key later requires rewriting the table.
- The mart uses extra storage and CPU during every insert.
- The simple MV does not protect against repeated delivery or old-row updates.
- Results are from only 111,227 rows, one laptop and mostly cached data. They do
  not prove the same timing relationship at production scale.
- The fare is synthetic and must not be interpreted as real revenue.

## 9. MPP distribution and skew

I simulated hash placement across four segments using the same ClickHouse rows.

| Distribution key | Segment row counts | Meaning |
|---|---|---|
| `trip_key` | 27,677 / 27,909 / 27,758 / 27,883 | Very even; max is only 0.8% above min |
| `start_station_id` | 19,499 / 31,169 / 27,825 / 32,734 | Moderate skew; busiest segment has 68% more rows than the smallest |
| area | 0 / 111,225 / 0 / 2 | Severe skew because the key has few values and two main areas hash to one segment |

Hashing by `trip_key` is a good general distribution for balanced scans and a
large fact-to-fact join on the same key. It does not make a join to stations
local, because the station table uses `station_id`.

The station dimension is small, so I would replicate it to every segment. Then
each segment can join locally without moving the large trips table. If both
sides of a large join used `station_id`, distributing both by that key would
avoid network motion, but the measured station skew would make one segment the
slowest. Distributing by area would be a bad choice: almost all work would land
on one segment.

Partition, sort and distribution keys solve different problems here: month
controls lifecycle, station/time controls skipping inside ClickHouse parts, and
the MPP distribution key controls which node stores each row.

The simulated counts are saved in `evidence/mpp_distribution.txt`.

## 10. Reproducibility

From the project folder, with internet access on the first run:

```bash
bash scripts/run_all.sh --clean
```

All paths are relative. The loader downloads the public source into an ignored
local cache and verifies its SHA-256 checksum. The clean run creates the
schemas, reloads all data, checks correctness, reruns the benchmark and saves
its terminal log. To stop while preserving database volumes:

```bash
docker compose down
```
