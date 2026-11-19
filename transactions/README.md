### `transactions` DAG

**Trigger:** Asset `raw_ingestion_complete`.
**Emits:** Asset `transactions_complete` with `{data_interval, tables}`.

For each of `users, artists, songs, song_versions, user_levels, events`: render the supplied Jinja SQL -> upload to `s3://<bucket>/artifacts/transactions/sql/<run_id>/<table>.sql` -> Glue job runs it, de-duplicates on the primary key and `MERGE INTO`s the Iceberg table (creating it on first run; `events` is partitioned by `event_date`).

Promotion order: `artists -> songs -> song_versions`, `users -> user_levels`, and `events` after `users, songs, artists, song_versions`.

`validate_transactions` runs Athena SQL checks: primary-key uniqueness for every table and an unresolved-`version_id` ratio below 30% for `events`.

**Update rules per table** (declared in `TABLES`): `users` updates only if the incoming `last_seen_at` is not older than the stored one (so replaying an older interval cannot overwrite newer state); `artists` and `songs` never replace a known value with NULL; the other tables use a plain upsert. The Glue job rejects staged data with NULL keys or timestamps in the future. `events.sql` resolves a play to a song only when the **title and the artist name** match.

Limitation: the layer is insert/update only; records removed from a corrected interval are not deleted. See `docs/engineering_notes.md`.
