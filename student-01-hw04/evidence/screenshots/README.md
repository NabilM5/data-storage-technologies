# Screenshot evidence

First run `bash scripts/run_all.sh --clean`. Capture these screenshots manually
only after that command finishes successfully. Put the check name in the
terminal before each command so the purpose is visible inside the image.

| Suggested file | Check confirmed |
|---|---|
| `01_environment.png` | PostgreSQL and ClickHouse containers are healthy |
| `02_transfer_and_equality.png` | Canonical row hashes and all query-result comparisons match |
| `03_q1_sort_plans.png` | Station/time order reads fewer Q1 granules than trip-key order |
| `04_q2_wide_plans.png` | Wide query reads all relevant parts/granules |
| `05_incremental_mart.png` | Empty MV target, history backfill, batch 1 and batch 2 totals |
| `06_measurements.png` | Five raw times, median, minimum and maximum for every variant |
| `07_mpp_distribution.png` | Four-segment distribution and skew for three candidate keys |
| `08_clean_run.png` | The complete clean run finishes successfully |

Screenshots support the committed text evidence. They do not replace it.

Run each block from `student-01-hw04`. Make the terminal wide, run `clear`, and
use macOS `Command + Shift + 4` after the result appears.

### 01 environment

```bash
clear
echo "CHECK 01: PostgreSQL and ClickHouse are healthy"
docker compose ps
```

### 02 transfer and equality

```bash
clear
echo "CHECK 02: PostgreSQL and ClickHouse data are equal"
sed -n '1,20p' evidence/checks.txt
```

### 03 Q1 query plans

This compact view shows the table, parts and granules for Q1.

```bash
clear
echo "CHECK 03: Q1 ClickHouse parts and granules read"
sed -n '31,123p' evidence/plans.txt | grep -E 'clickhouse /|ReadFromMergeTree|Parts:|Granules:'
```

### 04 Q2 query plans

```bash
clear
echo "CHECK 04: Q2 ClickHouse parts and granules read"
sed -n '158,248p' evidence/plans.txt | grep -E 'clickhouse /|ReadFromMergeTree|Parts:|Granules:'
```

### 05 incremental mart

```bash
clear
echo "CHECK 05: empty mart, history backfill and two new batches"
sed -n '1,18p' evidence/load_and_mart.txt
```

### 06 measurements

```bash
clear
echo "CHECK 06: one warm-up, five measured runs, median reported"
cut -d, -f1-8 evidence/measurements.csv | column -t -s,
```

### 07 MPP distribution

```bash
clear
echo "CHECK 07: simulated four-segment MPP distribution"
cat evidence/mpp_distribution.txt
echo
echo "Conclusion: trip_key gives the most balanced distribution"
```

### 08 clean run

```bash
clear
echo "CHECK 08: complete clean run finished successfully"
grep -E 'Removing only|Source SHA-256|Total source rows|Mart rows immediately|Full-grain check|All checks finished successfully' evidence/clean_run.txt
```

Save the eight PNG files in this folder using the suggested names above.
