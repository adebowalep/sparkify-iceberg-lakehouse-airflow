# Execution evidence (real Athena output, Workspace run)

## After interval 2026-01-01 (first run) — baseline before rerun
| Query | Result |
|---|---|
| `raw.logs` rows where data_interval = 2026-01-01 | 2618 |
| `analytics.user_facts` | 85 |
| `analytics.session_facts` | 331 |
| `analytics.song_facts` | 3134 |
| `analytics.artist_facts` | 1795 |

Airflow: run_pipeline -> raw -> transactions -> analytics all `success`, each started by an Asset event (run type `asset_triggered`).

## Rerun of 2026-01-01 (no manual cleanup) — Airflow run_pipeline 11:45:35
Chain: raw 11:45:41 -> transactions 11:50:10 -> analytics 11:59:31, all `success`, all `asset_triggered`.

| Query | Before rerun | After rerun | Match |
|---|---|---|---|
| `raw.logs` rows, interval 2026-01-01 | 2618 | 2618 | yes |
| `analytics.user_facts` | 85 | 85 | yes |
| `analytics.session_facts` | 331 | 331 | yes |
| `analytics.song_facts` | 3134 | 3134 | yes |
| `analytics.artist_facts` | 1795 | 1795 | yes |

Result: raw partition overwrite and analytics full replacement are idempotent for interval 2026-01-01.


## Interval 2026-01-02 — Airflow
run_pipeline 12:23:22 -> raw 12:23:35 -> transactions 12:28:17 -> analytics 12:36:16, all `success`, all asset-triggered (0 failed tasks/runs). Athena counts pending.


## Interval 2026-01-03 — Airflow
run_pipeline 15:06:51 -> raw 15:07:09 -> transactions 15:13:10 -> analytics 15:22:34, all `success`, asset-triggered. Asset event on `raw_ingestion_pending` carries `data_interval`; `raw_ingestion_complete` / `transactions_complete` events carry 2 items (`data_interval`, `tables`).

## Athena results after all three intervals (CSV copies in docs/athena_results/)
| `raw.logs` data_interval | rows |
|---|---|
| 2026-01-01 | 2618 (unchanged from first run and from rerun) |
| 2026-01-02 | 2592 |
| 2026-01-03 | 2560 |

| transactions table | rows | | analytics table | rows |
|---|---|---|---|---|
| users | 97 | | user_facts | 97 |
| artists | 3965 | | artist_facts | 3965 |
| songs | 8446 | | song_facts | 8446 |
| song_versions | 8566 | | session_facts | 776 |
| user_levels | 106 | | | |
| events | 6820 | | | |

Consistency: user_facts = users (97), song_facts = songs (8446), artist_facts = artists (3965), as the fact SQL left-joins from those tables.
Schema spot-checks (DESCRIBE): `analytics.user_facts` (10 columns) and `transactions.events` (12 columns, partitioned by `event_date`) match the data dictionary exactly.

## Rebuild after SQL timestamp fix + optional extra-credit tests (in progress)
- transactions.users/user_levels/events dropped, fixed SQL deployed (see docs/sql_corrections.md).
- Run 2026-01-01 (16:17:54): raw 16:18:14 -> transactions 16:23:40 -> analytics 16:30:37, all success.
- New source folder: `data_intervals/2026-01-01/zz_schema_test/` uploaded; `ingest` mapped over 3 tables (logs, songs, zz_schema_test) with no DAG change; `raw.zz_schema_test` has 3 rows (alpha, beta, gamma).
- Pending: upload v2 for 2026-01-02 and run it (schema evolution), run 2026-01-03, cleanup, final count/date checks.
- Schema evolution: v2 file (adds `note`) in `data_intervals/2026-01-02/zz_schema_test/`; run 2026-01-02 (17:04:46) raw 17:05:05 -> transactions 17:13:20 -> analytics 17:20:19, all success. `DESCRIBE raw.zz_schema_test` shows new column `note`; 6 rows, `note` NULL for interval 2026-01-01 rows, populated for 2026-01-02 rows (docs/athena_results/zz_schema_test_after_interval2.csv). PASS.
- Interval 2026-01-03 attempt 1 (run_pipeline 17:38:23): raw run 17:39:02 FAILED — both mapped `ingest` tasks (logs, songs) failed after ~2 min; `validate_raw` and `notify_complete` ended `upstream_failed`, so no `raw_ingestion_complete` event was emitted and `transactions`/`analytics` did not start (failure correctly blocks downstream). Root cause: pending (see task logs).
- Root cause of the 2026-01-03 attempt-1 failure: `ExpiredTokenException` (temporary AWS session token in the Airflow `aws_default` connection expired) while polling the Glue job state (GetJobRun). Not a code defect. Fix: refresh credentials in Airflow connections, then clear the failed run. A manual trigger of the `raw` DAG (run type `manual`, 17:52:49) failed immediately in `capture_landing_keys` because `raw` is meant to start only from the `raw_ingestion_pending` asset event.

## FINAL state after SQL fix + full rebuild of all three intervals (run_pipeline 2026-01-03 at 18:16:17; raw 18:16:27 -> transactions 18:21:04 -> analytics 18:34:17, all success, `asset_triggered`)
| Check | Result |
|---|---|
| raw.logs rows per interval | 2618 / 2592 / 2560 (identical to the pre-fix build) |
| transactions rows | users 97, artists 3965, songs 8446, song_versions 8566, user_levels 106, events 6820 (identical to pre-fix build) |
| analytics rows | user_facts 97, session_facts 776, song_facts 8446, artist_facts 3965 (identical) |
| date sanity (was year 50xxx) | users.registration 2018-10-19..2018-11-01; user_levels.changed_at 2018-11-01..2018-11-29; events.ts 2018-11-01..2018-11-30; events.event_date 2018-11-01..2018-11-30 |
| `DESCRIBE transactions.users` | registration, last_seen_at are now `timestamp` |
| `SHOW TABLES IN raw` | logs, songs only (test table dropped) |

Operational incidents (not code defects): AWS temporary credentials in the Airflow connections expired twice (ExpiredTokenException, then InvalidAccessKeyId after a bad paste); runs failed, validation/notify tasks ended `upstream_failed`, no completion event was emitted and downstream DAGs did not start. Manually triggering `raw` (run type `manual`) fails by design because its templates read the triggering asset event; only `run_pipeline` starts the chain.
