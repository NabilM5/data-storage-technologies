# HW 1: Running From a Clean Stand

Source: Open-Meteo Historical Weather API (ERA5 archive), CC BY 4.0.
Table: `lakehouse.student_01.weather`. The assignment and grading criteria are
in the course materials.

Run all commands **from the `infra/` directory** of the unpacked package. Below,
that path is written as `<package>/infra`.

## 0. Prerequisites

Docker Desktop is running, and Python 3 and `curl` are available on the host.

```bash
cd <package>/infra && docker compose config --quiet && docker compose build && docker compose up -d && docker compose ps --all
```

`minio-init` must finish with `Exited (0)`: it creates the `raw` and `datalake`
buckets. The other four services remain running.

Note on the Trino image: the package `infra/trino/Dockerfile` builds from
`trinodb/trino:483`. On the author's machine the locally cached copy of that
image had a truncated JDK layer that Docker Desktop kept reusing after every
re-pull, so for the runs recorded in `evidence/` the Dockerfile was built from
`trinodb/trino:482` (same JDK 25.0.3+9, identical catalog configuration and
PostgreSQL driver). The results do not depend on the Trino release; on a stand
with a healthy `483` image no change is needed.

Check Trino:

```bash
cd <package>/infra && docker compose exec -T trino trino --execute "SHOW CATALOGS"
```

The output must include `lakehouse` and `memory`.

## 1. Download the Source on the Host

```bash
cd <package>/infra && bash ../student-01-hw01/download_source.sh
```

Downloads 12 CSV files to `infra/data/hw01/raw/` (about 210,528 hourly
observations for 2024-2025) and prints their sizes. There is a 2 second pause
between requests because the free Open-Meteo tier rate-limits requests. Files
are not edited manually.

## 2. Run the Pipeline

The code is in the submission directory. Before running, copy it to the path
visible to the Spark container (`infra/scripts/` is mounted as `/scripts/`):

```bash
cd <package>/infra && mkdir -p scripts/hw01 && cp ../student-01-hw01/pipeline.py ../student-01-hw01/list_objects.py scripts/hw01/ && docker compose exec -T spark spark-submit /scripts/hw01/pipeline.py 2>&1 | tee ../student-01-hw01/evidence/spark-result.txt
```

`pipeline.py` performs: raw to S3 unchanged -> tabular CSV preparation ->
reading with an explicit schema -> quality checks and rejected-row output ->
Parquet with measurements -> two writes to Iceberg -> Spark query for
reconciliation.

The script is **idempotent**: rerunning it gives the same final row count.
Parquet is written in `overwrite` mode, the table is recreated with
`CREATE OR REPLACE TABLE`, and then exactly one second batch is inserted. If the
final result does not match the accepted row count, the script exits with an
error.

## 3. Run SQL in Trino

```bash
cd <package>/infra && docker compose exec -T trino trino < ../student-01-hw01/queries.sql 2>&1 | tee ../student-01-hw01/evidence/trino-result.txt
```

In DataGrip: use connection `jdbc:trino://localhost:8088/lakehouse/student_01`,
user `teacher`, empty password; assign this connection to `queries.sql` and run
the blocks in order. The client choice does not affect the result.

**After every Trino restart**, rerun section 5 (`memory.default.city_climate`):
`memory` is a non-persistent catalog.

## 4. Collect Evidence

```bash
cd <package>/infra && docker compose ps --all > ../student-01-hw01/evidence/compose-ps.txt && docker compose exec -T spark python3 /scripts/hw01/list_objects.py > ../student-01-hw01/evidence/object-listing.txt && ls -l ../student-01-hw01/evidence/
```

`spark-result.txt` and `trino-result.txt` are created by the commands in steps 2
and 3.

## 5. Fill in the Report

All numbers in `report.md` are taken from the files in `evidence/`; the
`SUMMARY FOR report.md` block at the end of the `pipeline.py` output lists the
main ones. If the pipeline is rerun, refresh sections 3–7 of `report.md` from
the new evidence files (timings, snapshot ids and the snapshot count change on
every run; row counts do not).

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

Large data files and Docker volumes are not included in the submission.
Credentials in the commands are educational values from the package
`docker-compose.yml`; there are no external secrets.

## Stop

```bash
cd <package>/infra && docker compose down
```

Without `-v`: that option irreversibly deletes the MinIO and PostgreSQL volumes.
