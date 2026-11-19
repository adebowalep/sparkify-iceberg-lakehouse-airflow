### `analytics` DAG

**Trigger:** Asset `transactions_complete`.

For each SQL file in `analytics/sql/` (`user_facts`, `session_facts`, `song_facts`, `artist_facts`): render -> upload to `s3://<bucket>/artifacts/analytics/sql/<run_id>/<table>.sql` -> Glue job executes it and **fully replaces** `iceberg.analytics.<table>` (`createOrReplace`), so every run is a fresh snapshot of `transactions` and re-running an interval never accumulates duplicates.
