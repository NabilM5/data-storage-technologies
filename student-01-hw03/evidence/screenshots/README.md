# Screenshot evidence

| File | Check confirmed |
|---|---|
| `01_data_control.png` | Full fact count/date range and the HW2 control slice count, duration and checksum |
| `02_plan_before_index.png` | Narrow query uses `Seq Scan` and removes 111,062 rows; the dropped index is restored by `ROLLBACK` |
| `03_plan_after_index.png` | Composite B-tree definition and `Bitmap Index Scan` / `Bitmap Heap Scan` plan |
| `04_partition_pruning.png` | Five populated partitions; the narrow plan reads only `fact_trip_p_aug01_08` |
| `05_measurements_and_equality.png` | Five measured times, medians and zero `EXCEPT ALL` difference rows for all six variants |
| `06_backup_restore.png` | `pg_dump` / `pg_restore` exit codes and identical source/restore verification |
| `07_patroni_roles_before.png` | Healthy containers, `patroni1` leader, `patroni2` streaming replica, lag 0 |
| `08_marker_replicated_before_switchover.png` | Before-marker written on the leader and read on the replica |
| `09_patroni_roles_after_switchover.png` | Successful planned switchover; `patroni2` leader and `patroni1` streaming replica |
| `10_markers_after_switchover.png` | Both manual markers visible on the current replica with lag 0 |
| `11_replica_write_rejected.png` | Replica rejects `INSERT`; rejected marker count on the leader is zero |
| `12_mvcc_before_commit.png` | Session A uses Read Committed and sees its own uncommitted row version |
| `13_mvcc_after_commit.png` | Side-by-side sessions: B sees `original` before A commits and the new value afterward |
| `14_same_row_lock_wait.png` | Same-row update waits on `Lock / transactionid`; blocker PID and query are identified |

The manual Patroni markers are:

- `manual_before_switchover_20261001160019`
- `manual_after_switchover_20261001160019`
