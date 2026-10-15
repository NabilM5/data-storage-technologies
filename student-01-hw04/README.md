# HW 4. Analytical mart in ClickHouse

This project loads the same Citi Bike trips into PostgreSQL and ClickHouse,
compares two ClickHouse sort orders, builds an incremental materialized-view
mart, checks every result, and measures one selective and one wide query.

The final measurements and explanations are in [report.md](report.md). Raw
outputs are in `evidence/`.

## 1. Connection with HW2 and HW3

The project reuses the real August 2026 Jersey City / Hoboken Citi Bike ZIP:

```text
https://s3.amazonaws.com/tripdata/JC-202608-citibike-tripdata.csv.zip
```

It contains 111,227 trips. HW2 established the business model, and HW3 used the
same narrow and wide queries in PostgreSQL. HW4 keeps their meaning fixed and
changes the physical organization by adding ClickHouse column storage, two
sort orders, and an incremental mart.

On the first run, the loader downloads the 3.8 MB ZIP into the ignored local
`data/` cache and verifies its SHA-256 checksum. The file is not committed.
Expected checksum:
`74cbe35022888397e3ad4635188117ba594f80c55dda49f9701e73b5de81f108`.
The source is covered by the [Citi Bike Data Use Policy](https://citibikenyc.com/data-sharing-policy).

## 2. Data contract

| Item | Rule |
|---|---|
| Consumer | Station operations planner for Jersey City and Hoboken |
| Business question | How do trip count, electric-bike count, duration and a teaching fare vary by station/date, area and rider type? |
| Fact grain | One row is one Citi Bike trip from one source system |
| Business key | `trip_key = source_system || ':' || ride_id` |
| Mart grain | Calendar day × start station × rider type |
| Time | Source New York wall-clock time with milliseconds; PostgreSQL uses `timestamp`, ClickHouse uses `DateTime64(3, 'America/New_York')` |
| Duration | Whole elapsed seconds rounded down; calculated once in Python and loaded unchanged into both systems |
| Missing station | Empty station IDs and names become `UNKNOWN`; they belong to area `Other` |
| Cancellations | The source has no cancellation status, so all valid non-negative-duration rows are included |
| Duplicates | A duplicate `trip_key` is an error; PostgreSQL enforces it and ClickHouse is checked with `uniqExact` and a full canonical hash |
| Corrections | This is a reproducible snapshot. A clean reload replaces it; changing an old source row does not automatically reverse its previous MV contribution |
| Repeated delivery | Re-inserting a batch into the MV source adds its measures again. Production loading would need a batch log or partition replacement |

### Teaching-only fare

Citi Bike does not provide money. To demonstrate exact money types required by
the rubric, the loader adds a deterministic synthetic fare:

```text
$2.50 + $0.20 for each completed minute + $1.00 for a casual rider
```

The calculation uses integer cents. PostgreSQL stores `numeric(12,2)` and
ClickHouse stores `Decimal(12,2)`. This is not presented as a real Citi Bike
price or revenue.

## 3. Physical design

PostgreSQL has a primary key on `trip_key` and a B-tree index on
`(start_station_id, started_at)`.

ClickHouse stores the same rows twice to test two sort orders:

| Table | MergeTree order | Purpose |
|---|---|---|
| `trips_by_station` | `(start_station_id, started_at, trip_key)` | Matches the selective station + date query |
| `trips_by_key` | `trip_key` | Alternative that is useful for key order but poor for the analytical filter |

Both use monthly partitions because month is the natural data lifecycle. The
source contains a very small July partition and one August partition. The sort
key, not the partition key, is expected to distinguish the one-day station
query inside August.

The mart target is `daily_station_mart`, a `SummingMergeTree`. It stores counts
and sums, not averages. Report averages are always computed as
`sum(duration_sum) / sum(trips)`.

## 4. Environment

| Component | Version / limit |
|---|---|
| PostgreSQL | 16.9 |
| ClickHouse | 25.8.33.6 |
| Python | 3.13 or another Python 3; only the standard library is used |
| Container limit | 2 CPUs and 2 GB RAM per database |
| PostgreSQL host port | 5437 |
| ClickHouse HTTP / native host ports | 8124 / 9001 |

Passwords in Compose are local teaching values, not real-system secrets.

## 5. Paths on another machine

All project paths are relative. The project no longer depends on an existing
HW2 folder. The first run needs internet access to download the public source;
later runs reuse the verified local cache.

Docker data lives in named volumes. Do not copy or commit those volume files.

## 6. Clean run from zero

Prerequisites: Docker Desktop, Python 3 and internet access on the first run.

```bash
cd student-01-hw04

# Removes only this project's old volumes, then runs every step.
bash scripts/run_all.sh --clean
```

The script starts healthy containers, loads the data and two held-out batches,
checks exact equality, performs one warm-up plus five measurements, and writes
all evidence files. Its complete terminal output is saved automatically in
`evidence/clean_run.txt`.

You can also run the steps separately:

```bash
docker compose up -d --wait
python3 scripts/load.py
python3 scripts/check_equal.py
python3 scripts/benchmark.py
```

## 7. Incremental mart sequence

1. Load history before `2026-08-25` into `trips_by_station`.
2. Create the MV and prove its target has zero rows.
3. Backfill only rows before `2026-08-25`.
4. Insert batch 1: `2026-08-25` through `2026-08-28`.
5. Insert batch 2: `2026-08-29` onward.
6. After every stage, compare every day × station × rider group and all metrics
   with a fresh aggregation from the detailed table.

The controlled boundary means history goes through the backfill once, while
held-out rows go through the MV once.

## 8. Stop and preserve data

```bash
docker compose down
```

This keeps named volumes. Add `-v` only for an intentional clean reset.

## 9. Project map

```text
student-01-hw04/
├── docker-compose.yml
├── sql/
│   ├── postgres/             # schema and two PostgreSQL queries
│   └── clickhouse/           # two tables, MV, source and mart queries
├── scripts/
│   ├── common.py
│   ├── load.py
│   ├── check_equal.py
│   ├── benchmark.py
│   └── run_all.sh
├── evidence/
│   └── screenshots/README.md
├── README.md
└── report.md
```

