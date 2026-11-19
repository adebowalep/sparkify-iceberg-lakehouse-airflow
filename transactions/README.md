### `transactions` DAG

**Trigger:** Asset `raw_ingestion_complete`.
**Emits:** Asset `transactions_complete` with `{data_interval, tables}`.

For each of `users, artists, songs, song_versions, user_levels, events`: render the supplied Jinja SQL -> upload to `s3://<bucket>/artifacts/transactions/sql/<run_id>/<table>.sql` -> Glue job runs it, de-duplicates on the primary key and `MERGE INTO`s the Iceberg table (creating it on first run; `events` is partitioned by `event_date`).

Promotion order: `artists -> songs -> song_versions`, `users -> user_levels`, and `events` after `users, songs, artists, song_versions`.

`validate_transactions` runs Athena SQL checks: primary-key uniqueness for every table and an unresolved-`version_id` ratio below 30% for `events`.
