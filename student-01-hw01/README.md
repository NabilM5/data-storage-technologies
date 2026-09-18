# HW 1: Running From a Clean Stand

Source: Open-Meteo Historical Weather API (ERA5 archive), CC BY 4.0.
Table: `lakehouse.student_01.weather`.

Run all commands **from the `infra/` directory** of the unpacked package
(written below as `<package>/infra`).

## 0. Prerequisites

Docker Desktop is running; Python 3 and `curl` are available on the host.

```bash
cd <package>/infra && docker compose config --quiet && docker compose build && docker compose up -d && docker compose ps --all
```

`minio-init` must finish with `Exited (0)`; the other four services stay up.

```bash
cd <package>/infra && docker compose exec -T trino trino --execute "SHOW CATALOGS"
```

The output must include `lakehouse` and `memory`.

Note: the package Dockerfile uses `trinodb/trino:483`. The local copy of that
image had a corrupted JDK layer, so the runs in `evidence/` were made with
`trinodb/trino:482` (same JDK and configuration); the results do not depend
on the Trino release.

## 1. Download the Source

```bash
cd <package>/infra && bash ../student-01-hw01/download_source.sh
```

Downloads 12 CSV files to `infra/data/hw01/raw/` (210,528 hourly rows for
2024–2025) with a 2 s pause between requests (API rate limit).

## 2. Run the Pipeline

`infra/scripts/` is mounted in the Spark container as `/scripts/`:

```bash
cd <package>/infra && mkdir -p scripts/hw01 && cp ../student-01-hw01/pipeline.py ../student-01-hw01/list_objects.py scripts/hw01/ && docker compose exec -T spark spark-submit /scripts/hw01/pipeline.py 2>&1 | tee ../student-01-hw01/evidence/spark-result.txt
```

Steps: raw to S3 unchanged → tabular CSV → explicit schema → quality checks
and rejects → Parquet with measurement → two Iceberg writes → Spark query.
The script is idempotent (Parquet overwrite, `CREATE OR REPLACE TABLE`, one
INSERT) and exits with an error if the final count differs from the accepted
rows.

## 3. Run SQL in Trino

```bash
cd <package>/infra && docker compose exec -T trino trino < ../student-01-hw01/queries.sql 2>&1 | tee ../student-01-hw01/evidence/trino-result.txt
```

DataGrip: `jdbc:trino://localhost:8088/lakehouse/student_01`, user `teacher`,
empty password; run the blocks of `queries.sql` in order. After every Trino
restart rerun section 5 (`memory` is non-persistent).

## 4. Collect Evidence

```bash
cd <package>/infra && docker compose ps --all > ../student-01-hw01/evidence/compose-ps.txt && docker compose exec -T spark python3 /scripts/hw01/list_objects.py > ../student-01-hw01/evidence/object-listing.txt
```

`spark-result.txt` and `trino-result.txt` come from steps 2 and 3. The
numbers in `report.md` are taken from these files.

## Directory Contents

```text
student-01-hw01/
├── README.md            # this file
├── report.md            # source passport, measurements, conclusion
├── schema.md            # fields, types, meaning, nullable, partitioning
├── pipeline.py          # raw -> validation -> parquet -> iceberg
├── queries.sql          # validation, analytical, and federated SQL
├── download_source.sh   # reproducible source download
├── list_objects.py      # MinIO object listing for evidence
└── evidence/
    ├── compose-ps.txt
    ├── object-listing.txt
    ├── spark-result.txt
    └── trino-result.txt
```

The dataset and Docker volumes are not included. Credentials in the code are
the educational values from the package `docker-compose.yml`; there are no
external secrets.

## Stop

```bash
cd <package>/infra && docker compose down
```

Without `-v`: that option deletes the MinIO and PostgreSQL volumes.
