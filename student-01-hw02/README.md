# HW 2. Citi Bike daily station trips: DWH model and ELT pipeline

Local ELT stand: CSV to PostgreSQL (`raw`), dbt builds `stg`, `dds` and
`mart_candidate`, dbt tests run, then Airflow publishes to `mart`.
Results are in [report.md](report.md), the analysis in
[notebook/hw02_analysis.ipynb](notebook/hw02_analysis.ipynb).

## 1. Source and control slice

| Item | Value |
|---|---|
| Source | Citi Bike system data, Jersey City / Hoboken monthly file `JC-202608-citibike-tripdata.csv.zip` (August 2026, 111,227 trips, 4 MB), https://s3.amazonaws.com/tripdata/ |
| Terms | Citi Bike Data License Agreement (https://citibikenyc.com/data-sharing-policy). No personal data in the file |
| Fields | `ride_id`, `rideable_type` (classic_bike / electric_bike), `started_at`, `ended_at`, `start_station_name`, `start_station_id`, `end_station_name`, `end_station_id`, `start_lat`, `start_lng`, `end_lat`, `end_lng`, `member_casual` (member / casual) |
| Units, time zone | Timestamps with milliseconds, local New York time, no zone in the file. Duration is derived in seconds, coordinates in decimal degrees |
| Control slice | Trips that started 2026-08-03 to 2026-08-09 (Mon to Sun) at six stations: `JC115`, `HB609`, `JC109`, `HB107`, `JC009`, `HB103`. 4,083 trips. Station reference: all 108 start stations of the month |
| How to get it | `python3 data/make_slice.py` downloads the month file and rebuilds `data/slice/` and `data/scenarios/` |

The files in `data/scenarios/` are synthetic test inputs for the four
diagnostic cases. They are derived from slice rows by the same script and are
marked by their path (`scenarios/...`), so they are never mistaken for source
data.

## 2. Requirement

Consumer: a station operations planner of the Jersey City / Hoboken Citi
Bike network.

Questions. (1) How many trips start at each station per day, and how does
the member / casual mix differ between weekdays and the weekend? (2) How does
the average trip duration differ by area and rider type?

MVP: the table `mart.daily_station_trips` with trips, electric trips,
total and average duration per day, start station and rider type, plus the
timestamp of the last successful publication.

## 3. Model

Granularity. One row of `dds.fact_trip` is one completed trip from one
source system. The key is `(source_system, ride_id)`, written as `trip_key`.
One row of the mart is one day, source system, start station and rider type.

```
                dim_date (date_key)            dim_station (station_key, SCD2)
                        1                                1
                        |                                |
                        N                                N
                          fact_trip (trip_key)
        date_key, station_key, source_system, ride_id, rideable_type,
        member_casual, started_at, ended_at, duration_sec, end_station_id
                                  |
                                  v   group by day, source, station, rider type
                   daily_station_trips (row_key)  ->  mart_candidate  ->  mart
```

| Table | Key | Link | Cardinality |
|---|---|---|---|
| `dds.dim_date` | `date_key` (yyyymmdd) | `fact_trip.date_key` | 1 date : N trips |
| `dds.dim_station` | `station_key` = md5(`station_id`, `valid_from`) | `fact_trip.station_key` | 1 version : N trips; 1 `station_id` : N versions |
| `dds.fact_trip` | `trip_key` = `source_system:ride_id` | | 1 : 1 with the source ride |
| `mart.daily_station_trips` | `row_key` = date, source, station, rider type | aggregates `fact_trip` | N trips : 1 row |

`rideable_type` and `member_casual` have two values each and stay on the fact
as degenerate dimensions. A separate table for them would add a join without
adding information. The station `area` (Jersey City or Hoboken) comes from the
id prefix in the source.

Why a star schema. The consumer asks for counts and sums by day and by
station attributes. A star answers that with one join per dimension and
re-aggregates correctly, because the mart stores sums rather than averages. A
Data Vault or a normalised operational model would be the right choice for
integrating many sources with changing entities, which is not the case here.

History. Station names and coordinates change in the real network, and a
past day's report has to keep the station as it was on that day. `dim_station`
is therefore SCD type 2: each version carries `[valid_from, valid_to)` and the
fact joins the version valid on the trip's start date. Trip attributes are not
versioned. A corrected trip replaces the previous one, which is what the late
correction case relies on.

## 4. Layers and physical tables

| Layer | Schema and table | Built by |
|---|---|---|
| Raw | `raw.trips`, `raw.stations` (all text, append-only, with `source_system`, `source_file`, `batch_id`, `loaded_at`) | `loader/load_raw.py` |
| ODS / staging | `stg.stg_trips`, `stg.stg_stations` (types, latest version per key) | dbt `models/staging` |
| DDS | `dds.dim_date`, `dds.dim_station`, `dds.fact_trip` | dbt `models/dds` |
| Mart candidate | `mart_candidate.daily_station_trips` | dbt `models/marts` |
| Mart (published) | `mart.daily_station_trips` and `ops.publications` | DAG task `publish` |

## 5. Tool versions

| Tool | Version |
|---|---|
| Docker Desktop / Compose | 29.5.3 / v5.1.4 (macOS, Apple M1, 8 GB) |
| PostgreSQL | 16.9 (`postgres:16.9`) |
| Apache Airflow | 3.3.2 (`apache/airflow:3.3.2-python3.12`, `airflow standalone`, LocalExecutor) |
| dbt | dbt-core 1.12.5, dbt-postgres 1.11.0 (venv `/opt/dbt-venv` in the Airflow image) |
| Python (host) | 3.13, pandas 2.3, matplotlib 3.10, psycopg2-binary 2.9 (for the scripts in `data/` and the notebook) |

## 6. Run from zero

```bash
cd student-01-hw02
python3 data/make_slice.py                    # slice, station reference, scenarios
docker compose build                          # Airflow image with dbt
docker compose up -d                          # postgres + airflow, about a minute
docker compose ps
```

Airflow UI: http://127.0.0.1:8080, no login (`SIMPLE_AUTH_MANAGER_ALL_ADMINS`).
PostgreSQL from the host: `localhost:5433`, database `dwh`, user and password
`dwh` / `dwh`. These are the local values from `docker-compose.yml`.

Trigger the DAG `hw02_citibike_elt` from the UI, or from the CLI:

```bash
# baseline: a successful publication of the week
docker compose exec -T airflow airflow dags trigger hw02_citibike_elt \
  --conf '{"date_from": "2026-08-03", "date_to": "2026-08-09", "scenario": "baseline"}'

# one of: duplicate | late_fix | second_source | history_overlap | zero_duration
docker compose exec -T airflow airflow dags trigger hw02_citibike_elt \
  --conf '{"date_from": "2026-08-03", "date_to": "2026-08-09", "scenario": "late_fix"}'
```

Each run is `check_input`, `load_raw`, `dbt_build_candidate`, `dbt_test`,
`publish`. `load_raw` loads the baseline files, removes any scenario batches
and then loads the requested scenario, so the cases stay independent. A failed
test stops the run before `publish` and `mart` keeps its previous publication.

The whole diagnostic sequence, recorded in `evidence/runs.txt`:

```bash
bash scripts/run_all.sh | tee evidence/runs.txt
```

Report queries and the independent control:

```bash
docker compose exec -T postgres psql -U dwh -d dwh -f - < sql/report_queries.sql
python3 data/independent_check.py 2026-08-05 JC115
```

## 7. Analysis notebook

[`notebook/hw02_analysis.ipynb`](notebook/hw02_analysis.ipynb) covers the
model, the publication mechanism and three charts that answer the consumer's
questions. It is committed with its outputs, so it can be read on GitHub
without running anything. It queries the mart when the stand is up and
otherwise recomputes the same table from the committed slice, so it runs
either way. Open it in Jupyter or VS Code and run all cells to reproduce it.

## 8. Directory contents

```text
student-01-hw02/
├── README.md, report.md
├── docker-compose.yml, Dockerfile        # postgres 16.9 + airflow 3.3.2 with dbt
├── init-db/                              # databases, schemas, raw / mart / ops tables
├── loader/load_raw.py                    # CSV to raw, batch = source_system + file
├── dbt/                                  # project, profiles, models, tests, macros
├── dags/hw02_citibike_elt.py             # DAG with date_from / date_to / scenario
├── data/make_slice.py                    # rebuilds the slice and the scenarios
├── data/independent_check.py             # control recount from the month file
├── data/slice/, data/scenarios/          # input, about 830 KB
├── sql/report_queries.sql                # queries used in the report
├── scripts/run_all.sh                    # runs all scenarios into evidence/runs.txt
├── notebook/hw02_analysis.ipynb          # analysis notebook, committed with outputs
└── evidence/
    ├── runs.txt                          # DAG runs, task states, case queries
    ├── sql_results.txt                   # report queries on the published mart
    └── screenshots/                      # captioned screenshots
```

The month zip, Docker volumes and Airflow logs are not committed. The zip is
rebuilt by `data/make_slice.py`.

## 9. Stop

```bash
docker compose down        # add -v to drop the database volume as well
```
