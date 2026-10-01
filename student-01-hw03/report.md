# HW 3 report

## 1. Data and method

The consumer, business question, grain, keys, measures, units and time
convention are fixed in the README result contract. I used 111,227 real trips
from the HW2 August source for measurement. No synthetic performance rows were
added. The narrow filter matches 165 rows (0.1483%); the wide filter matches
all 111,227 rows (100%). The narrow result has two rows and the wide result has
five rows.

Before every comparison I ran `ANALYZE`. Each table/query variant had one
discarded warm-up followed by five measured executions under the same settings:
`shared_buffers=128MB`, `work_mem=4MB`,
`max_parallel_workers_per_gather=2`, `jit=on`, and `random_page_cost=4`.
The reported value is PostgreSQL server `Execution Time` from
`EXPLAIN (ANALYZE, BUFFERS)`, not planner cost and not client time.

The query texts are in `sql/narrow.sql` and `sql/wide.sql`. They answer the
same operations-planning question at narrow and broad scopes. The exact
answers are:

| Query | Group | Trips | Electric | Total seconds | Average seconds |
|---|---|---:|---:|---:|---:|
| Narrow | casual | 20 | 14 | 9,252 | 462.6 |
| Narrow | member | 145 | 64 | 57,115 | 393.9 |
| Wide | Hoboken / casual | 15,311 | 9,745 | 14,159,551 | 924.8 |
| Wide | Hoboken / member | 33,287 | 19,831 | 15,819,077 | 475.2 |
| Wide | Jersey City / casual | 15,886 | 10,905 | 16,491,652 | 1,038.1 |
| Wide | Jersey City / member | 46,741 | 25,979 | 26,493,189 | 566.8 |
| Wide | Other / casual | 2 | 2 | 4,218 | 2,109.0 |

## 2. Measurements

| Query / variant | Matching rows | Main plan nodes / partitions | Five times, ms | Median, ms | Answer matched? |
|---|---:|---|---|---:|---|
| Narrow / original | 165 | Seq Scan; 111,062 rows removed | 9.295, 9.510, 9.674, 9.642, 10.191 | 9.642 | Yes |
| Narrow / index | 165 | Bitmap Index Scan + Bitmap Heap Scan | 0.414, 0.398, 0.436, 0.514, 0.386 | 0.414 | Yes |
| Narrow / partitioned + index | 165 | Only `p_aug01_08`; bitmap scans | 0.421, 0.368, 0.461, 0.389, 0.499 | 0.421 | Yes |
| Wide / original | 111,227 | Parallel Seq Scan | 22.969, 20.182, 20.144, 21.028, 20.281 | 20.281 | Yes |
| Wide / index | 111,227 | Parallel Seq Scan; index ignored | 20.894, 20.600, 20.166, 26.367, 22.086 | 20.894 | Yes |
| Wide / partitioned + index | 111,227 | Parallel Append; all five partitions | 18.504, 21.843, 18.172, 17.899, 18.572 | 18.504 | Yes |

The complete plans are in `evidence/optimization.txt`; all numeric rows are in
`evidence/optimization_results.csv`.

## 3. Index conclusion

The tested B-tree is `(start_station_id, started_at)`. Equality on station is
first and the timestamp range is second, matching the narrow `WHERE` clause.
The original narrow plan scanned the whole heap, removed 111,062 rows and hit
2,284 shared buffers. With the index, PostgreSQL found 165 entries, visited 129
heap blocks and hit 133 buffers. The median changed from 9.642 to 0.414 ms.

The wide query selects 100% of the fact. PostgreSQL correctly kept a parallel
sequential scan because reading almost every heap row through an index would
add random access without avoiding work. Its median changed only from 20.281
to 20.894 ms, which is run-to-run variation rather than improvement. I did not
disable sequential scans. The added index is 3,456 kB, took 281.525 ms of
client elapsed time to build, and adds write and maintenance cost. The original
table uses 18 MB of heap and 28 MB total after indexes.

## 4. Partition conclusion

The partition key is `started_at`, because both report filters use time ranges.
This one-month teaching set uses weekly ranges so several partitions contain
data. Every lower bound is included and upper bound excluded:

| Partition | Bound | Rows |
|---|---|---:|
| `p_before_aug` | MINVALUE to 2026-08-01 | 20 |
| `p_aug01_08` | 2026-08-01 to 2026-08-08 | 23,924 |
| `p_aug08_15` | 2026-08-08 to 2026-08-15 | 24,810 |
| `p_aug15_22` | 2026-08-15 to 2026-08-22 | 27,040 |
| `p_aug22_sep01` | 2026-08-22 to 2026-09-01 | 35,433 |

The narrow plan names only `fact_trip_p_aug01_08`, proving partition pruning.
The wide range covers every row, so its `Parallel Append` correctly names all
five partitions. The partitioned primary key is `(trip_key, started_at)`
because a PostgreSQL unique constraint on a partitioned table must include the
partition key. The load independently checks that all 111,227 `trip_key`
values are unique, so trip identity has not changed. The same
`(start_station_id, started_at)` index exists on each partition; therefore the
comparison is explicitly “partitioned + index,” not partitioning alone.

The partitioned wide median was 18.504 ms in this run, but five small warm runs
are not enough to claim a stable broad-query speed-up. Its defensible benefit
here is pruning for bounded dates; its price is more DDL, per-partition indexes
and operational complexity.

## 5. Correctness checks

For every row in the measurement table I compared the new result and saved
baseline in both directions with `EXCEPT ALL`. Every difference count is zero.
`EXCEPT ALL` checks keys, measure values and duplicate multiplicity, unlike a
simple row count.

The independent HW2 control slice also matches:

- fact rows: 4,083;
- mart rows: 84;
- duration: 2,113,343 seconds;
- checksum: `32b30791faf47178a5a1f6c8e9299be4`.

This confirms that physical changes did not alter the business answer.

## 6. Logical backup and restore

I used `pg_dump` 16.9 custom format (`-Fc`) and restored into a separate
database named `dwh_restore`. Both commands returned exit code 0. Source and
restore each have four expected schemas, 12 tables, 16 indexes, 16 constraints,
111,227 populated and unique fact keys, 6,272 mart rows, and the same date
range. The control count/duration and MD5 values of both query answers match
exactly. Full commands and output are in `evidence/backup_restore.txt`.

## 7. Patroni replication and switchover

The local cluster has two PostgreSQL 16.9 nodes managed by Patroni 4.1.5 and
one etcd 3.5.34 DCS. etcd stores cluster state and the leader lock, not trip
rows. Before the test, both Patroni and SQL confirmed `patroni1` as writable
leader (`pg_is_in_recovery() = false`) and `patroni2` as streaming replica
(`true`, lag 0). In the final manual run, the replica received marker
`manual_before_switchover_20261001160019` on timeline 8.

A planned `patronictl switchover` made `patroni2` leader and `patroni1` a
streaming replica on timeline 9. The new leader contained the old marker,
accepted `manual_after_switchover_20261001160019`, and both markers reached the
current replica with lag 0. A direct insert on that replica failed with
`cannot execute INSERT in a read-only transaction`; the marker count on the
leader stayed zero. After the role change, the replicated mart still had 4,083
fact rows, 84 mart rows and 2,113,343 seconds.

Switchover is planned and chooses a healthy caught-up candidate. Failover is an
emergency promotion when the leader is unavailable; asynchronous lag can make
its RPO non-zero. Clients must reconnect after either role change. This laptop
stand demonstrates role management and streaming, not protection from losing
the laptop. Replication also repeats accidental deletes, so it cannot replace
an independent backup. RTO and RPO were not production-tested here.

## 8. Two sessions, MVCC, locks and WAL

Both sessions used `Read Committed`. Session A updated one row and kept the
transaction open. Session B first read `original`, because uncommitted row
versions are invisible. After A committed, B's next statement read
`changed by session A`, because Read Committed takes a new snapshot for each
statement. Both transactions ended.

In the lock experiment, B tried to update the same row. `pg_stat_activity`
showed B waiting on `Lock / transactionid`, and `pg_blocking_pids()` returned
A's PID. B was deliberately canceled by a 2.5-second statement timeout; this
kept the automated demonstration finite. The manual screenshot used a
120-second timeout so the wait could be inspected before it ended. `pg_locks`
with `granted = false` is another way to inspect a wait.

WAL is written before changed data pages. PostgreSQL replays it after a crash,
and physical replicas receive and replay the same WAL stream. A logical dump
recreates selected database objects and rows and is useful across a separate
database. A physical base backup plus a continuous WAL archive copies the
cluster files and can support point-in-time recovery. They solve different
recovery problems.

## 9. Evidence map

| Evidence | File |
|---|---|
| Load, date range and HW2 checksum | `evidence/load.txt` |
| Plans, buffers, repetitions and equality | `evidence/optimization.txt` |
| Compact measurement table | `evidence/optimization_results.csv` |
| Dump, restore and exact checks | `evidence/backup_restore.txt` |
| MVCC visibility and row-lock wait | `evidence/two_sessions.txt` |
| Roles, markers, switchover and rejected write | `evidence/patroni.txt` |
| Fourteen manual screenshots | `evidence/screenshots/` |
