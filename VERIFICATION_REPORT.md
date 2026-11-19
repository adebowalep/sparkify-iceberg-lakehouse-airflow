# Verification report

Environment: Udacity Workspace (Airflow 3, Glue 5.0, Iceberg, Athena), us-east-1. Evidence: `docs/evidence_log.md`, `docs/athena_results/*.csv`, `docs/sql_corrections.md`. All numbers come from real Airflow and Athena output.

## Static checks (local, Airflow 3.0.6)
- No `#### YOUR CODE HERE` markers remain; all six edited files parse; all five DAGs import without errors.
- `raw`, `transactions`, `analytics`: asset-scheduled, `max_active_runs=1`, `max_active_tasks=2` (also visible on the Airflow Details page).
- No connections or `Variable.get` at DAG-parse time; bucket resolved through Jinja `var.value.s3_bucket`; connection IDs are constants at the top of each DAG; `setup/` files unchanged.
- `analytics/glue_script.py` has no append/insert operations; it uses `createOrReplace()`.

## Runtime results
| Requirement | Status | Evidence |
|---|---|---|
| One manual `run_pipeline` trigger starts raw -> transactions -> analytics | PASS | All three intervals, plus reruns: every downstream run is `asset_triggered`, all tasks `success` |
| Correct DAG ids, asset scheduling, concurrency limits | PASS | DagBag + Airflow UI |
| Asset events carry `data_interval` and `tables` | PASS | Completion events show 2 items; `raw_ingestion_pending` shows 1 |
| Raw: dynamic S3 discovery, one mapped Glue job per table | PASS | `ingest` mapped over `logs`/`songs`; no table names in raw/dag.py or raw/glue_script.py except the required validation SQL |
| Raw: new source folder needs no DAG change | PASS | Temporary folder `zz_schema_test` was picked up (3 mapped ingests, table created, 3 rows); removed afterwards |
| Raw: schema evolution | PASS | v2 file added column `note`; old rows NULL, new rows populated; test table dropped afterwards |
| Raw: partition-level overwrite, no double counting | PASS | 01-01 = 2618 before/after rerun and after later intervals; 01-02 = 2592; 01-03 = 2560 |
| Transactions: dependency order, dedup, MERGE, PK checks | PASS | Graph/gantt order; 6 PK checks + version-resolution check green on every run; tables existed from interval 1, so later intervals used the MERGE branch |
| Analytics: full replacement, reproducible | PASS | Counts identical across reruns (before fix: 85/331/3134/1795 for interval 1 rerun; final 97/776/8446/3965 with all intervals) |
| Failed task blocks downstream | PASS (observed) | Expired-credential failures: `validate_raw`/`notify_complete` = `upstream_failed`, no event, transactions/analytics did not start |
| Athena can query every table | PASS | raw.logs, six transactions tables, four analytics tables queried |
| Schemas match the data dictionary | PASS with notes | All column names match; raw.logs uses the source JSON keys in lower case (`userid`, `firstname`...), which the provided SQL requires; the three timestamp columns became `timestamp` after the SQL fix (verified for `users`; the full-schema export was taken before the fix) |
| SQL check operators | PASS | raw x2, transactions x7 |

## Provided-SQL correction (verified defect)
Three provided templates treated millisecond Unix times as seconds (years 50,000+). Minimal fix documented in `docs/sql_corrections.md`; counts unchanged, dates now 2018-11.

## Notes
- AWS temporary credentials in Airflow connections expired during testing (operational, not code). A `raw` run started by hand (`manual`) fails by design; start runs only from `run_pipeline`.
- The Airflow web UI restarted once and lost its history; pipeline data in S3/Glue was unaffected.
- `index.py` (Workspace zip helper from the starter) is not a DAG and is excluded from the submission ZIP.
