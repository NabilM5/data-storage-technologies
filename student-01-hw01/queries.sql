-- HW 1. Validation, analytical, and federated SQL for Trino.
-- Run from infra/: docker compose exec -T trino trino < ../student-01-hw01/queries.sql
-- Section 5 must be rerun after each Trino restart (memory is non-persistent).

-- 1. What is available through the connection
SHOW CATALOGS;
SHOW SCHEMAS IN lakehouse;
SHOW TABLES IN lakehouse.student_01;
DESCRIBE lakehouse.student_01.weather;

-- 2. Control queries: dataset size and boundaries
SELECT count(*) AS all_observations,
       count(DISTINCT city) AS cities,
       min(time) AS min_time,
       max(time) AS max_time
FROM lakehouse.student_01.weather;

SELECT time, city, temperature_2m, relative_humidity_2m, precipitation
FROM lakehouse.student_01.weather
ORDER BY city, time
LIMIT 10;

-- 3. Engine reconciliation: same query as spark_answer() in pipeline.py.
--    Trino avg(decimal(18,6)) -> decimal(18,6); Spark casts to decimal(14,2) for the same scale.
SELECT count(*) AS observations,
       avg(CAST(temperature_2m AS decimal(18,6))) AS mean_temperature
FROM lakehouse.student_01.weather
WHERE city = 'Moscow'
  AND time >= TIMESTAMP '2024-06-01 00:00:00'
  AND time <  TIMESTAMP '2024-09-01 00:00:00';

-- 4. Iceberg metadata: two data writes = two snapshots (suffixed name in double quotes).
SELECT snapshot_id,
       committed_at,
       operation,
       CAST(summary['total-records'] AS bigint) AS total_records,
       CAST(summary['added-records'] AS bigint) AS added_records
FROM lakehouse.student_01."weather$snapshots"
ORDER BY committed_at;

-- Files that make up the current table version
SELECT count(*) AS data_files,
       sum(record_count) AS records,
       sum(file_size_in_bytes) AS bytes
FROM lakehouse.student_01."weather$files";

-- 5. Reference table in the memory catalog: official Russian time zones and a
--    climate label, filled manually, one row per city. 9 of 12 cities; Astrakhan,
--    Arkhangelsk and Irkutsk are omitted to show that coverage gaps do not lose rows.
DROP TABLE IF EXISTS memory.default.city_climate;
CREATE TABLE memory.default.city_climate AS
SELECT * FROM (VALUES
    ('Moscow',           'continental',  3),
    ('Saint_Petersburg', 'continental',  3),
    ('Novosibirsk',      'continental',  7),
    ('Yekaterinburg',    'continental',  5),
    ('Kazan',            'continental',  3),
    ('Sochi',            'subtropical',  3),
    ('Murmansk',         'subarctic',    3),
    ('Vladivostok',      'monsoon',     10),
    ('Krasnoyarsk',      'continental',  7)
) AS t(city, climate_zone, utc_offset_hours);

SELECT * FROM memory.default.city_climate ORDER BY city;

-- Reference keys are unique: this query must return 0 rows.
SELECT city, count(*) AS key_count
FROM memory.default.city_climate
GROUP BY city
HAVING count(*) > 1;

-- 6. Federated query: "Which cities have similar daily temperature profiles?"
--    lakehouse (Iceberg in MinIO) + memory. The reference table converts UTC to
--    local hour; LEFT JOIN keeps unmatched cities as "Unmatched" with the UTC hour.
SELECT e.city,
       coalesce(g.climate_zone, 'Unmatched') AS climate_zone,
       (hour(e.time) + coalesce(g.utc_offset_hours, 0)) % 24 AS local_hour,
       round(avg(CAST(e.temperature_2m AS decimal(18,6))), 2) AS mean_temperature,
       count(*) AS observations
FROM lakehouse.student_01.weather e
LEFT JOIN memory.default.city_climate g ON e.city = g.city
GROUP BY e.city,
         coalesce(g.climate_zone, 'Unmatched'),
         (hour(e.time) + coalesce(g.utc_offset_hours, 0)) % 24
ORDER BY e.city, local_hour;

WITH profile AS (
    SELECT e.city,
           coalesce(g.climate_zone, 'Unmatched') AS climate_zone,
           (hour(e.time) + coalesce(g.utc_offset_hours, 0)) % 24 AS local_hour,
           avg(CAST(e.temperature_2m AS decimal(18,6))) AS mean_temperature
    FROM lakehouse.student_01.weather e
    LEFT JOIN memory.default.city_climate g ON e.city = g.city
    GROUP BY e.city,
             coalesce(g.climate_zone, 'Unmatched'),
             (hour(e.time) + coalesce(g.utc_offset_hours, 0)) % 24
)
SELECT city,
       climate_zone,
       round(max(mean_temperature) - min(mean_temperature), 2) AS daily_amplitude_c,
       CAST(max_by(local_hour, mean_temperature) AS integer) AS warmest_local_hour,
       CAST(min_by(local_hour, mean_temperature) AS integer) AS coldest_local_hour
FROM profile
GROUP BY city, climate_zone
ORDER BY daily_amplitude_c DESC;

-- 7. The JOIN neither multiplied nor lost rows
WITH observations AS (
    SELECT city, time FROM lakehouse.student_01.weather
)
SELECT (SELECT count(*) FROM observations) AS before_join,
       count(*) AS after_join,
       count_if(g.city IS NULL) AS unmatched_rows,
       count(DISTINCT CASE WHEN g.city IS NULL THEN e.city END) AS unmatched_cities
FROM observations e
LEFT JOIN memory.default.city_climate g ON e.city = g.city;
