# Engineering notes: correctness review and decisions

This review was done **after** the AWS runs documented in `evidence_log.md`. Everything here was verified with the local test
suite (`tests/`, real Glue scripts + real SQL on synthetic data, local Spark 3.5.4 / Iceberg 1.7.1). Nothing here has been re-run on AWS.

## 1. Findings

| # | Finding | Status | Evidence |
|---|---|---|---|
| 1 | Replaying/loading an older interval after a newer one overwrote newer `users` attributes and `last_seen_at` (unconditional `MERGE ... UPDATE SET *`) | **Confirmed, fixed** | `tests/test_replay_regression.py` (failed before the fix, passes after) |
| 2 | `users.sql` tie-break depended on input row order when two events share a timestamp | **Confirmed, fixed** | same file |
| 3 | `events.sql` attached a play to any song with the same title (the artist join never filtered), including for unknown artists | **Confirmed, fixed** | `tests/test_event_resolution.py` |
| 4 | Records removed from a corrected raw interval stay in `transactions` | **Confirmed, documented as a limitation** (see 3) | `tests/test_source_corrections.py` |
| 5 | Loading a later interval before an earlier one leaves an extra `initial` row in `user_levels` | **Confirmed, documented** (current level unaffected) | strict-xfail test in `test_replay_regression.py` |
| 6 | `inlet_events[asset][-1]` is the newest event ever, not the one that started the run, so queued runs could pick a different interval than `triggering_asset_events` (used elsewhere in the same DAG) | **Demonstrated by Airflow 3.0.6 source, fixed.** Not observed on AWS: runs were always triggered one at a time | `tests/test_orchestration.py` |
| 7 | `validate_*` checks would not catch NULL keys or unit errors such as the milliseconds bug | **Added Glue-side checks** | `tests/test_merge_semantics_and_quality.py` |

Investigated, not a defect: duplicate event accumulation on rerun (events are keyed by a deterministic hash; tested), idempotent
reruns of an unchanged interval (tested), timestamp conversion (tested against known epoch values).

## 2. Update semantics per transactions table
One MERGE strategy for all tables would be wrong, so each table declares its own in `TABLES` (`transactions/dag.py`).

| Table | Nature | Rule on a matching key |
|---|---|---|
| `users` | mutable attributes with an event time (`last_seen_at`) | update **only if incoming `last_seen_at` >= stored** (`version_column`). `>=` so a corrected re-delivery of the same instant still applies. |
| `artists`, `songs` | mutable descriptive attributes, no timestamp | update, but an incoming NULL never erases a known value (`keep_existing_when_null`). No ordering rule is invented. |
| `song_versions` | immutable (key = hash of song_id + duration) | plain update (no-op) |
| `user_levels` | history, key (user_id, changed_at) | plain update; matching rows describe the same transition |
| `events` | immutable facts keyed by a deterministic hash | plain update, deliberately: a replay may legitimately re-resolve `version_id`/`song_id`/`artist_id` |

Within one batch the winner is deterministic: newest version first, then the other column values.

## 3. Source corrections and deletions: decision
Option B: the transactions layer is **insert/update only**. Corrected and new records are applied; records that disappear from a
corrected interval are not deleted. Reasons: the assignment specifies upserts with `MERGE INTO`; `events`, `songs` etc. carry no
source-interval column (the data dictionary fixes their schema), so a deletion scoped to one interval is not possible without changing
the model; an unscoped `WHEN NOT MATCHED BY SOURCE THEN DELETE` would remove other intervals' data. If deletions ever matter, the
smallest safe design is to add a `source_interval` column and delete `WHERE source_interval = X AND key NOT IN (staging)`.

## 4. Event resolution rules (`events.sql`)
1. A song candidate must match the played title **and** the artist name exactly.
2. Among candidates, a version within 0.01 s of the played length is required; the closest wins, then the lowest `version_id`, then the lowest `song_id`.
3. No candidate: `song_id`, `artist_id`, `version_id` are NULL. Artist and title match but no version in tolerance: `artist_id` is set, `song_id` and `version_id` are NULL (as before).
4. Events are resolved against the catalog loaded **so far**; an event whose song arrives in a later interval stays unresolved until its interval is replayed.

## 5. Data quality
Added to `transactions/glue_script.py`, so they run before anything is written and are testable locally:
- **NULL in a key column**: such rows never match in a MERGE and would be re-inserted on every run.
- **Timestamps/dates in the future**: the signature of a unit error (the original milliseconds-as-seconds bug produced year 50,000+).

Existing Athena checks are unchanged (raw rows exist, primary keys unique, unresolved `version_id` ratio < 30%). The 30% limit comes from the
assignment brief; it exists because plays can reference songs that are not in the catalog. Deliberately **not** added: record-count change
thresholds (arbitrary for this data) and more Athena SQL checks (cannot be verified without an AWS run).

## 6. What is verified where
| Level | What |
|---|---|
| Verified on AWS (earlier implementation, commit `a432963`) | end-to-end runs of 3 intervals, reruns, schema evolution, new source folder, failure propagation, Athena counts and schemas |
| Verified locally (pytest, synthetic data) | all behaviour in sections 1-5 |
| Verified by GitHub Actions | not yet (workflow added, no run recorded) |
| Not verified on AWS | everything changed after `a432963`: events resolution, MERGE conditions, staging checks, DAG interval selection. Expect `transactions.events` resolution counts and downstream `song_facts`/`artist_facts` to differ from the recorded numbers |

## 7. Local test environment vs AWS
Local runs use a Hadoop Iceberg catalog on the local filesystem instead of the AWS Glue Data Catalog on S3, a stubbed `awsglue` module, and
Spark in local mode. Athena (Trino) SQL and the Airflow scheduler are not exercised. Glue-specific behaviour (job bookmarks, IAM, S3 IO) is not covered.

## 8. Licensing and publication
`LICENSE.txt` is Udacity's licence for its educational content; it grants rights under CC BY-NC-ND 4.0 (**NoDerivs**) only for content "specifically
marked" as subject to it, and excludes some uses from "non-commercial". Several files here derive from Udacity starter material (CloudFormation
template, `setup/` DAGs, SQL templates, DAG/Glue skeletons) and this repository publishes completed versions of them. Whether that redistribution
is permitted is **not established**. Options: keep the repository private; publish only original material (tests, docs, the Glue/DAG logic written
by the author) and link to Udacity for the starter files; or ask Udacity support for written confirmation. Unmodified starter SQL (`docs/original_sql/`)
is kept locally and git-ignored, and the course datasets are not included.
