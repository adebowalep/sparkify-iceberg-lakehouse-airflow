### `raw` DAG

**Trigger:** Asset `raw_ingestion_pending` (emitted by `run_pipeline`, carries `data_interval`).
**Emits:** Asset `raw_ingestion_complete` with `{data_interval, tables}`.

1. `capture_landing_keys` – lists the table folders under `data_intervals/<data_interval>/` in S3 (schema-driven; no table names in code).
2. `ingest` – one mapped Glue job per discovered table; writes `iceberg.raw.<table>` partitioned by `data_interval`. First run creates the table; later runs overwrite only that interval's partition (idempotent) and evolve the schema if new columns appear.
3. `validate_raw` – Athena SQL checks that `raw.logs` and `raw.songs` contain rows for the interval.
4. `notify_complete` – emits `raw_ingestion_complete`.
