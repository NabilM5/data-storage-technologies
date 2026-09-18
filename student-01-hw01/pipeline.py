"""HW 1. raw -> validation -> Parquet -> Iceberg (Open-Meteo).

Run from infra/ after download_source.sh:
    docker compose exec -T spark spark-submit /scripts/hw01/pipeline.py
Idempotent: raw and Parquet are overwritten, the table is recreated with
CREATE OR REPLACE and gets exactly two writes.
"""

import io
import os
import time

import boto3
from pyspark.sql import SparkSession, functions as F, types as T

STUDENT = "student_01"
DATASET = "weather"
INGESTION_DATE = "2026-09-17"

ENDPOINT = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "hse2026minio"

LOCAL_RAW_DIR = "/data/hw01/raw"
RAW_BUCKET = "raw"
RAW_PREFIX = f"{STUDENT}/{DATASET}/ingestion_date={INGESTION_DATE}/source"
PREPARED_KEY = f"{STUDENT}/{DATASET}/ingestion_date={INGESTION_DATE}/prepared.csv"
PREPARED_PATH = f"s3a://{RAW_BUCKET}/{PREPARED_KEY}"

PARQUET_PATH = f"s3a://datalake/{STUDENT}/{DATASET}/parquet"
REJECT_PATH = f"s3a://datalake/{STUDENT}/{DATASET}/rejects"
TABLE = f"lakehouse.{STUDENT}.{DATASET}"

# Boundary between two non-overlapping batches: 2024 and 2025.
BOUNDARY = "2025-01-01 00:00:00"

# Explicit types, inferSchema is not used. DECIMAL keeps AVG comparable in Spark and Trino.
WEATHER_SCHEMA = T.StructType([
    T.StructField("time", T.TimestampType()),
    T.StructField("city", T.StringType()),
    T.StructField("temperature_2m", T.DecimalType(5, 1)),
    T.StructField("relative_humidity_2m", T.IntegerType()),
    T.StructField("precipitation", T.DecimalType(6, 2)),
])
COLUMNS = [f.name for f in WEATHER_SCHEMA.fields]
COLUMN_LIST = ", ".join(COLUMNS)

# Physical limits; negative temperature is valid.
TEMP_MIN, TEMP_MAX = -90.0, 60.0

# Same query for CSV and Parquet: 3 of 5 fields, one city.
QUERY = """
    SELECT count(*) AS observations,
           avg(temperature_2m) AS mean_temperature
    FROM {view}
    WHERE city = 'Moscow'
      AND time >= TIMESTAMP '2024-06-01 00:00:00'
      AND time <  TIMESTAMP '2024-09-01 00:00:00'
"""


def s3_client():
    return boto3.client("s3", endpoint_url=ENDPOINT,
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY)


def dir_size_mb(spark, path):
    """Object or directory size in MiB (bytes / 1024**2)."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(path), conf)
    summary = fs.getContentSummary(jvm.org.apache.hadoop.fs.Path(path))
    return summary.getLength() / 1024 / 1024


def upload_raw(s3):
    """Store original Open-Meteo files in S3 as-is, without edits."""
    if not os.path.isdir(LOCAL_RAW_DIR):
        raise SystemExit(f"Directory {LOCAL_RAW_DIR} does not exist. "
                         "Run download_source.sh on the host first.")
    files = sorted(f for f in os.listdir(LOCAL_RAW_DIR) if f.endswith(".csv"))
    if not files:
        raise SystemExit(f"No source CSV files found in {LOCAL_RAW_DIR}.")

    total = 0
    for name in files:
        local = os.path.join(LOCAL_RAW_DIR, name)
        key = f"{RAW_PREFIX}/{name}"
        s3.upload_file(local, RAW_BUCKET, key)
        total += os.path.getsize(local)
    print(f"Raw objects uploaded: {len(files)}; "
          f"total {total / 1024 / 1024:.2f} MiB")
    print(f"Prefix: s3://{RAW_BUCKET}/{RAW_PREFIX}/")
    return files


def prepare_csv(s3, files):
    """Build one tabular CSV from the raw objects; originals are not modified.
    The header row is found by the 'time,' prefix, columns by name prefix."""
    source_fields = ["time", "temperature_2m", "relative_humidity_2m", "precipitation"]
    out = io.StringIO()
    out.write(",".join(COLUMNS) + "\n")
    written = 0

    for name in files:
        city = os.path.splitext(name)[0]
        body = s3.get_object(Bucket=RAW_BUCKET,
                             Key=f"{RAW_PREFIX}/{name}")["Body"].read().decode("utf-8")
        lines = body.splitlines()
        header_idx = next((i for i, line in enumerate(lines)
                           if line.startswith("time,")), None)
        if header_idx is None:
            raise ValueError(f"{name}: data header row 'time,' was not found")

        header = [h.strip() for h in lines[header_idx].split(",")]
        # Position of each required field in the source header.
        idx = {}
        for field in source_fields:
            matches = [i for i, h in enumerate(header) if h.split(" ")[0] == field]
            if len(matches) != 1:
                raise ValueError(f"{name}: field {field} was not found unambiguously "
                                 f"in header {header}")
            idx[field] = matches[0]

        for line in lines[header_idx + 1:]:
            if not line.strip():
                continue
            parts = line.split(",")
            if len(parts) < len(header):
                raise ValueError(f"{name}: short row: {line!r}")
            row = [parts[idx["time"]], city,
                   parts[idx["temperature_2m"]],
                   parts[idx["relative_humidity_2m"]],
                   parts[idx["precipitation"]]]
            out.write(",".join(row) + "\n")
            written += 1

    s3.put_object(Bucket=RAW_BUCKET, Key=PREPARED_KEY,
                  Body=out.getvalue().encode("utf-8"))
    size = s3.head_object(Bucket=RAW_BUCKET, Key=PREPARED_KEY)["ContentLength"]
    print(f"Prepared CSV: {written:,} data rows; "
          f"{size / 1024 / 1024:.2f} MiB; s3://{RAW_BUCKET}/{PREPARED_KEY}")
    return written


def reject_reason_column():
    """First matching rejection reason; NULL = row accepted."""
    temp = F.col("temperature_2m")
    hum = F.col("relative_humidity_2m")
    prec = F.col("precipitation")
    return (
        F.when(F.col("time").isNull(), "time_unparsed")
         .when(F.col("city").isNull() | (F.trim(F.col("city")) == ""), "city_missing")
         .when(temp.isNull(), "temperature_missing")
         .when(~temp.between(TEMP_MIN, TEMP_MAX), "temperature_out_of_range")
         .when(hum.isNotNull() & ~hum.between(0, 100), "humidity_out_of_range")
         .when(prec.isNotNull() & (prec < 0), "precipitation_negative")
         .otherwise(F.lit(None).cast("string"))
    )


def validate(df, expected_rows):
    print("\n== Quality checks ==")

    # 1. Required columns exist.
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Prepared CSV is missing columns: {missing}")
    print(f"1. Required columns are present: {COLUMNS}")

    flagged = df.withColumn("reject_reason", reject_reason_column())
    metrics = flagged.agg(
        F.count("*").alias("rows"),
        # 3. time parsed by the schema; unparsable -> null.
        F.sum(F.when(F.col("time").isNull(), 1).otherwise(0)).alias("time_unparsed"),
        # 5. NULL share.
        F.sum(F.when(F.col("temperature_2m").isNull(), 1).otherwise(0)).alias("temp_null"),
        F.sum(F.when(F.col("relative_humidity_2m").isNull(), 1).otherwise(0)).alias("hum_null"),
        F.sum(F.when(F.col("precipitation").isNull(), 1).otherwise(0)).alias("prec_null"),
        F.countDistinct("city").alias("cities"),
        F.min("time").alias("min_time"),
        F.max("time").alias("max_time"),
        F.sum(F.when(F.col("reject_reason").isNotNull(), 1).otherwise(0)).alias("rejected"),
    ).first()

    # 2. Row count is greater than zero.
    if metrics.rows == 0:
        raise ValueError("Prepared CSV is empty")
    print(f"2. Rows read: {metrics.rows:,} (prepared CSV: {expected_rows:,})")
    if metrics.rows != expected_rows:
        raise ValueError("Spark did not read the same number of rows written by the preparation step")

    print(f"3. time was not parsed in {metrics.time_unparsed} rows")
    print(f"4. temperature_2m outside [{TEMP_MIN}, {TEMP_MAX}] "
          "is accounted for in reject_reason below")
    print(f"5. NULL share: temperature_2m {metrics.temp_null / metrics.rows:.6%}; "
          f"relative_humidity_2m {metrics.hum_null / metrics.rows:.6%}; "
          f"precipitation {metrics.prec_null / metrics.rows:.6%}")
    print(f"   Cities: {metrics.cities}; period: {metrics.min_time} .. {metrics.max_time}")

    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    accepted = flagged.filter(F.col("reject_reason").isNull()).select(*COLUMNS)

    # Rejected rows with original values and reason.
    (rejected.write.mode("overwrite").option("header", True).csv(REJECT_PATH))
    rejected_count = metrics.rejected
    accepted_count = metrics.rows - rejected_count

    if rejected_count:
        print("\nRejection reasons:")
        rejected.groupBy("reject_reason").count().orderBy(F.desc("count")).show(truncate=False)
    else:
        print(f"\nNo rejected rows; path {REJECT_PATH} contains an empty check result")

    # 6. Reconciliation: raw = accepted + rejected.
    if accepted_count + rejected_count != metrics.rows:
        raise ValueError("Reconciliation raw = accepted + rejected did not match")
    print(f"6. Reconciliation: raw {metrics.rows:,} = accepted {accepted_count:,} "
          f"+ rejected {rejected_count:,}")
    return accepted, accepted_count, rejected_count


def to_parquet(spark, accepted, accepted_count):
    print("\n== Parquet ==")
    # Partition by city (12 values, query filter), not by time (~17,500 small files).
    (accepted.write.mode("overwrite").option("compression", "snappy")
     .partitionBy("city").parquet(PARQUET_PATH))

    parquet_df = spark.read.parquet(PARQUET_PATH).select(*COLUMNS)
    src_types = {f.name: f.dataType for f in accepted.schema.fields}
    pq_types = {f.name: f.dataType for f in parquet_df.schema.fields}
    if src_types != pq_types:
        raise ValueError(f"Types changed after writing Parquet: {src_types} != {pq_types}")
    print(f"Names and types of {len(COLUMNS)} fields preserved: CSV = Parquet")

    parquet_count = parquet_df.count()
    if parquet_count != accepted_count:
        raise ValueError(f"Row count changed: accepted={accepted_count}, "
                         f"Parquet={parquet_count}")
    print(f"Rows: accepted = Parquet = {parquet_count:,}")

    csv_mb = dir_size_mb(spark, PREPARED_PATH)
    pq_mb = dir_size_mb(spark, PARQUET_PATH)
    print(f"Prepared CSV size: {csv_mb:.2f} MiB; Parquet: {pq_mb:.2f} MiB")
    if pq_mb > 0:
        print(f"CSV/Parquet ratio: {csv_mb / pq_mb:.2f}")
    return parquet_df, csv_mb, pq_mb


def measure(spark, accepted, parquet_df):
    print("\n== Measurement: the same query over CSV and Parquet ==")
    print(QUERY.format(view="<view>"))
    print("The query uses 3 of 5 fields. The schema is defined before the timer starts; "
          "the timer covers execution (.first()), not DataFrame construction.")
    print("This is not a cold start: checks and Parquet writing have already read the data. "
          "Neither DataFrame is cached in Spark: every CSV run re-reads and parses the object.")

    accepted.createOrReplaceTempView("weather_csv")
    parquet_df.createOrReplaceTempView("weather_parquet")

    results = {}
    for trial in range(1, 4):
        # Alternate the order so warm-up does not always benefit one format.
        formats = ("csv", "parquet") if trial % 2 else ("parquet", "csv")
        for fmt in formats:
            started = time.perf_counter()
            row = spark.sql(QUERY.format(view=f"weather_{fmt}")).first()
            elapsed = time.perf_counter() - started
            if results and row != next(iter(results.values())):
                raise ValueError(f"Query result diverged: {fmt}: {row}")
            results[fmt] = row
            label = "first run" if trial == 1 else f"repeat {trial}"
            print(f"{fmt.upper():8s} {label:14s}: {elapsed:7.3f} s; {row.asDict()}")
    print("CSV = Parquet results in all measurements.")
    print("Time depends on cache, warm-up, and laptop resources; "
          "a format speedup is not guaranteed at this volume.")


def snapshot_ids(spark):
    return {r.snapshot_id for r in
            spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()}


def to_iceberg(spark, parquet_df, accepted_count):
    print("\n== Iceberg ==")
    before = F.col("time") < F.lit(BOUNDARY).cast("timestamp")
    split = parquet_df.agg(
        F.sum(F.when(before, 1).otherwise(0)).alias("batch_1"),
        F.sum(F.when(~before, 1).otherwise(0)).alias("batch_2"),
    ).first()
    if not split.batch_1 or not split.batch_2:
        raise ValueError(f"Both batches must be non-empty: {split.asDict()}")
    if split.batch_1 + split.batch_2 != accepted_count:
        raise ValueError("Batches do not cover the full accepted dataset")
    print(f"Batch 1 (time < {BOUNDARY}): {split.batch_1:,} rows")
    print(f"Batch 2 (time >= {BOUNDARY}): {split.batch_2:,} rows")
    print(f"Batch sum = accepted rows: {accepted_count:,}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS lakehouse.{STUDENT}")
    previous = snapshot_ids(spark) if spark.catalog.tableExists(TABLE) else set()

    print(f"\n-- Write 1: CTAS, time < {BOUNDARY}")
    spark.sql(f"""
        CREATE OR REPLACE TABLE {TABLE}
        USING iceberg
        PARTITIONED BY (city, months(time))
        AS SELECT {COLUMN_LIST} FROM parquet.`{PARQUET_PATH}`
        WHERE time < TIMESTAMP '{BOUNDARY}'
    """)
    first_count = spark.table(TABLE).count()
    if first_count != split.batch_1:
        raise ValueError(f"After write 1: {first_count} != {split.batch_1}")
    after_first = snapshot_ids(spark)
    print(f"Rows: {first_count:,}; new snapshots: {sorted(after_first - previous)}")

    print(f"\n-- Write 2: INSERT, time >= {BOUNDARY}")
    spark.sql(f"""
        INSERT INTO {TABLE}
        SELECT {COLUMN_LIST} FROM parquet.`{PARQUET_PATH}`
        WHERE time >= TIMESTAMP '{BOUNDARY}'
    """)
    final = spark.table(TABLE).agg(
        F.count("*").alias("rows"),
        F.countDistinct("city", "time").alias("unique_keys"),
    ).first()
    after_second = snapshot_ids(spark)
    print(f"Rows: {final.rows:,}; new snapshots: {sorted(after_second - after_first)}")

    # Idempotency: same final count, no duplicates.
    if final.rows != accepted_count:
        raise ValueError(f"Final {final.rows} != accepted {accepted_count}: "
                         "duplicates from repeated inserts are possible")
    if final.unique_keys != final.rows:
        raise ValueError(f"Pair (city, time) is not unique: {final.asDict()}")
    print(f"Reconciliation: Iceberg {final.rows:,} = accepted {accepted_count:,}; "
          "pair (city, time) is unique: no duplicates")

    print("\n-- Snapshot history")
    spark.sql(f"""
        SELECT snapshot_id, committed_at, operation,
               summary['total-records'] AS total_records,
               summary['added-records'] AS added_records
        FROM {TABLE}.snapshots ORDER BY committed_at
    """).show(100, truncate=False)

    print("-- Schema from Iceberg metadata")
    spark.sql(f"DESCRIBE TABLE {TABLE}").show(30, truncate=False)
    return split.batch_1, split.batch_2


def spark_answer(spark):
    print("\n== Spark query to Iceberg (equivalent to the Trino query in queries.sql) ==")
    # decimal(14,2): Spark avg -> decimal(18,6), same scale as Trino's avg(decimal(18,6)).
    spark.sql(f"""
        SELECT count(*) AS observations,
               avg(CAST(temperature_2m AS decimal(14,2))) AS mean_temperature
        FROM {TABLE}
        WHERE city = 'Moscow'
          AND time >= TIMESTAMP '2024-06-01 00:00:00'
          AND time <  TIMESTAMP '2024-09-01 00:00:00'
    """).show(truncate=False)

    print("== Research question: daily temperature profile (UTC), Spark ==")
    spark.sql(f"""
        SELECT city, hour(time) AS hour_utc,
               round(avg(CAST(temperature_2m AS decimal(14,2))), 2) AS mean_temperature
        FROM {TABLE}
        WHERE city IN ('Moscow', 'Vladivostok')
        GROUP BY city, hour(time)
        ORDER BY city, hour_utc
    """).show(48, truncate=False)


def main():
    spark = (SparkSession.builder.appName("hw01-weather")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        s3 = s3_client()
        print("== Step 1: raw data unchanged ==")
        files = upload_raw(s3)

        print("\n== Step 2: prepare tabular CSV from originals ==")
        prepared_rows = prepare_csv(s3, files)

        print("\n== Step 3: read with explicit schema (inferSchema is not used) ==")
        df = (spark.read.schema(WEATHER_SCHEMA)
              .option("header", True)
              .option("enforceSchema", False)   # header must match the schema
              .option("mode", "PERMISSIVE")     # unparsable value -> null -> reject
              .option("nullValue", "")
              .option("timestampFormat", "yyyy-MM-dd'T'HH:mm")
              .csv(PREPARED_PATH))
        df.printSchema()

        accepted, accepted_count, rejected_count = validate(df, prepared_rows)
        # Not cached: the measurement below must read the CSV, not Spark memory.

        parquet_df, csv_mb, pq_mb = to_parquet(spark, accepted, accepted_count)
        measure(spark, accepted, parquet_df)
        batch_1, batch_2 = to_iceberg(spark, parquet_df, accepted_count)
        spark_answer(spark)

        print("\n================ SUMMARY FOR report.md ================")
        print(f"Prepared CSV rows:              {prepared_rows:,}")
        print(f"Accepted / rejected:            {accepted_count:,} / {rejected_count:,}")
        print(f"CSV / Parquet size, MiB:        {csv_mb:.2f} / {pq_mb:.2f}")
        print(f"Batch 1 / batch 2:              {batch_1:,} / {batch_2:,}")
        print(f"Iceberg table:                  {TABLE}")
        print("====================================================")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
