# HW 3. PostgreSQL administration and optimization

This project reuses the Citi Bike model and data from HW2. It measures two
queries before and after a B-tree index, tests a partitioned copy, proves that
all answers stay equal, restores a logical backup, runs a two-session MVCC and
lock experiment, and completes a Patroni switchover with two PostgreSQL nodes.

The measured results and explanations are in [report.md](report.md). Complete
command output is in `evidence/`.

## 1. Link with HW2 and source

No new business dataset is invented. `scripts/load_data.py` reads the existing
HW2 file:

```text
../student-01-hw02/data/JC-202608-citibike-tripdata.csv.zip
```

It is the August 2026 Citi Bike Jersey City / Hoboken file from
<https://s3.amazonaws.com/tripdata/>. The full month is the measurement set:
111,227 trips from 2026-07-31 16:27:10.701 through
2026-08-31 23:53:36.692. The committed HW2 week remains the independent
control slice: 4,083 trips, 84 mart rows and 2,113,343 seconds. Its checksum is
`32b30791faf47178a5a1f6c8e9299be4`, exactly the HW2 value.

## 2. Result contract

| Item | Contract |
|---|---|
| Consumer | Station operations planner for the Jersey City / Hoboken network |
| Business question | How do trip count, electric-bike count and duration vary by station/date, area and rider type? |
| Fact grain and key | One real Citi Bike trip; `trip_key = source_system:ride_id` |
| Result grain | Narrow: rider type for JC115 on 2026-08-05. Wide: area and rider type for the full source range |
| Measures | Trips, electric trips, total duration in whole seconds, average duration in seconds |
| Time convention | Source timestamps are local New York wall-clock values without an offset; stored as `timestamp without time zone` |
| Physical change | Indexes and partitions only; the business rules and query answers must not change |

The HW3 loader is intentionally smaller than the Airflow/dbt HW2 pipeline. It
keeps the same source, fact grain, business key, measures and published mart,
because this homework studies PostgreSQL physical organization rather than ELT
orchestration.

## 3. Environment

| Component | Version / resource |
|---|---|
| Machine | Apple arm64, 8 GB physical memory |
| Docker allocation seen by the engine | 8 CPUs, 5,157,208,064 bytes RAM |
| Docker / Compose | 29.5.3 / v5.1.4 |
| PostgreSQL | 16.9 |
| Patroni | 4.1.5 |
| etcd | 3.5.34 |
| Host Python | 3.13.1; core scripts use the standard library |

Local connections: standalone PostgreSQL `localhost:5434`; Patroni nodes
`localhost:5435` and `localhost:5436`; Patroni REST APIs
`localhost:8008` and `localhost:8009`. Local training passwords are in
`docker-compose.yml`; they are not production secrets.

## 4. Run from zero

Prerequisites: Docker Desktop and Python 3. The HW2 ZIP must exist at the path
above. If it is missing, rebuild it with `python3 ../student-01-hw02/data/make_slice.py`.

```bash
cd student-01-hw03
docker compose up -d --build
python3 scripts/load_data.py
python3 scripts/benchmark.py
python3 scripts/backup_restore.py
python3 scripts/mvcc_demo.py
python3 scripts/patroni_demo.py
```

Or run all steps:

```bash
bash scripts/run_all.sh
```

The screenshot evidence was captured manually from the live Terminal run and
is listed in `evidence/screenshots/README.md`. The manual two-window version of
the transaction experiment is in `sql/two_sessions_manual.sql`.

## 5. Stop and preserve data

```bash
docker compose down
```

Named volumes keep all databases. Use `docker compose down -v` only when an
intentional clean reset is wanted. The dump itself is generated at
`backup/dwh.dump` and ignored by Git; reproducible commands and restore results
are committed as text.

## 6. Project map

```text
student-01-hw03/
├── docker-compose.yml
├── patroni/                 # image and two node configurations
├── sql/                     # schema, two queries, partitions, manual session test
├── scripts/                 # load, benchmark, restore, MVCC and Patroni checks
├── evidence/                # complete outputs, result CSV and labeled screenshots
├── README.md
└── report.md
```
