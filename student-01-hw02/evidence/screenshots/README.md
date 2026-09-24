# Screenshots

What each screenshot confirms. The same facts are in `../runs.txt` and
`../sql_results.txt` as text.

| File | What it shows | Conclusion |
|---|---|---|
| `01_dag_run_success.png` | A baseline run of `hw02_citibike_elt`, all five tasks green, 25 s | The pipeline runs end to end and publishes for the given interval |
| `02_dbt_test_34_passed.png` | `dbt_test` log: `Done. PASS=34 WARN=0 ERROR=0 SKIP=0 TOTAL=34` | All quality checks pass on clean input, so publication is allowed |
| `03_dag_run_blocked_by_test.png` | The `zero_duration` run: `dbt_test` Failed, `publish` Upstream Failed | A failing check stops the run before publication |
| `04_blocked_run_audit_log.png` | Audit log of the same run, `dbt_test` running then failed | The failure is recorded per task with its time |
| `05_dag_runs_list.png` | 15 runs with Success and Failed states | Scenarios were run repeatedly; failures are visible in the history |
| `06_airflow_home_health.png` | Scheduler, DagProcessor, Triggerer, MetaDatabase healthy | The stand is fully up |
| `07_dag_code_in_ui.png` | DAG source in the UI: stage list, scenarios, `dbt_cmd` | The interval is passed to dbt as vars, not taken from the run date |
| `08_task_instances_recent.png` | Task instances of the last runs | The `publish` task is `Upstream Failed` exactly in the failed runs |
| `09_task_instances_history.png` | Older task instances | Earlier scenario runs, same pattern |
| `10_mart_result.png` | `select ... from mart.daily_station_trips where calendar_date = '2026-08-05' and station_id = 'JC115'`: member 145 trips / 57,115 s, casual 20 trips / 9,252 s | The published mart answers the consumer's question, matching `data/independent_check.py` exactly |
| `11_publication_kept.png` | `select ... from ops.publications order by publication_id desc limit 5`: the latest row is `#11, baseline, checksum 32b30791...` | The failed `zero_duration` run added no row; the consumer still sees the last correct baseline |
