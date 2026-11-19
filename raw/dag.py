from pathlib import Path
from airflow.sdk import DAG, Asset, task, Param, TaskGroup

# Import the GlueJob Operator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

# Import the SQLCheckOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator

# Import the S3Hook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ── Config ─────────────────────────────────────────────────────────────────────
S3_BUCKET = "{{ var.value.s3_bucket }}"

# Create a variable named LANDING_PREFIX
# Assign the variable to a string representing 
# the top level folder where data intervals are stored
LANDING_PREFIX = "data_intervals"

# Create a variable named WAREHOUSE_LOCATION 
# Assign the variable to an S3 uri pointing to 
# the location where the iceberg warehouse should be stored
#  (format s3://bucket/location)
WAREHOUSE_LOCATION = f"s3://{S3_BUCKET}/iceberg-warehouse/"

# Create a variable named ICEBERG_SCRIPT that points to the absolute path
# for the glue_script within the raw directory
ICEBERG_SCRIPT = (Path(__file__).resolve().parent / "glue_script.py").as_posix()

# Create variables for the connection ids here
AWS_CONN_ID    = "aws_default"
ATHENA_CONN_ID = "athena_default"
GLUE_ROLE_NAME = "dev-lakehouse-glue-role"

# Keep this line unchanged
REGION = "us-east-1"

# Initialize an Asset that is emitted by the `run_pipeline` dag.
RAW_INGESTION_PENDING = Asset("raw_ingestion_pending")

# Initialize an Asset to represent completion of raw ingestion
RAW_INGESTION_COMPLETE = Asset("raw_ingestion_complete")

def single_interval(events, asset_name):
    """data_interval carried by the asset events that triggered THIS run.

    `inlet_events[asset]` returns every event the asset has ever had, so `[-1]` is the newest event overall,
    which is not necessarily the one that started this run (queued runs, rapid triggers).
    `triggering_asset_events` is scoped to the current run, so it always names the right interval.
    If one run was started by events for different intervals, refuse instead of silently skipping one."""
    intervals = {event.extra["data_interval"] for event in events}
    if not intervals:
        raise ValueError(
            f"No '{asset_name}' asset event triggered this run. Start the pipeline from run_pipeline; "
            f"do not trigger this DAG by hand.")
    if len(intervals) > 1:
        raise ValueError(
            f"This run was triggered by events for several intervals {sorted(intervals)}. "
            f"Trigger run_pipeline for one interval at a time (wait for run_pipeline to finish).")
    return intervals.pop()


# Initialize a DAG 
# - Set the id to "raw"
# - Schedule the dag to trigger when the run_pipeline dag completes
# - Set maximum active runs to 1
# - Set maximum active tasks to 2
with DAG(
    dag_id="raw",
    schedule=[RAW_INGESTION_PENDING],
    max_active_runs=1,
    max_active_tasks=2,
    doc_md=(Path(__file__).resolve().parent / "README.md").read_text(),
    tags=["lakehouse", "raw"],
) as dag:

    # Define an airflow python task named `capture_landing_keys`
    # - Set the task decorator argument `inlets` to the Asset that triggers
    #   the raw dag
    # - Include the following parameters in the function signature:
    #   - `s3_bucket` a templated string argument. Renders the s3_bucket name at runtime
    #   - `inlet_events` (context variable used for accessing that tasks's inlet events)
    @task(inlets=[RAW_INGESTION_PENDING])
    def capture_landing_keys(s3_bucket, *, triggering_asset_events) -> list[str]:

        # Pull the data interval value from the metadata of the Asset event(s) that triggered this run
        data_interval = single_interval(
            triggering_asset_events[RAW_INGESTION_PENDING], RAW_INGESTION_PENDING.name)

        # Initialize an S3Hook (connection opened at task runtime only)
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        # Using the `LANDING_PREFIX` variable and the data_interval
        # pulled from the Asset, define the S3 prefix
        # for the data_interval folder (format: <landing>/<data_interval>/)
        interval_prefix = f"{LANDING_PREFIX}/{data_interval}/"

        # List the table "folders" stored in the data_interval (schema-driven
        # discovery: nothing about the source tables is hardcoded).
        # The returned list is pushed to XCom as the task's return value.
        table_prefixes = hook.list_prefixes(
            bucket_name=s3_bucket, prefix=interval_prefix, delimiter="/"
        ) or []

        if not table_prefixes:
            raise ValueError(
                f"No source tables found under s3://{s3_bucket}/{interval_prefix}"
            )

        return table_prefixes

    # Keep this line unchanged
    table_keys = capture_landing_keys(S3_BUCKET)

    # Fill in the partial configurations for the GlueJobOperator
    submit_glue_jobs = GlueJobOperator.partial(
        # Set the task_id to `ingest`
        task_id="ingest",

        # Set the aws_conn_id
        aws_conn_id=AWS_CONN_ID,

        # Set the script_location to the absolute path
        # for the raw glue script
        script_location=ICEBERG_SCRIPT,

        # Set the s3_bucket to your templated S3_BUCKET variable
        s3_bucket=S3_BUCKET,

        # Set the iam_role_name to the role defined in this file
        iam_role_name=GLUE_ROLE_NAME,

        # Set the reion_name to the region defined in this file
        region_name=REGION,

        # Set the attribute that ensures downstream tasks
        # wait for this task to complete
        wait_for_completion=True,

        # Set the attribute that ensures the glue script is replaced
        # each time this task is executed
        replace_script_file=True,

        # Keep this line unchanged
        map_index_template="{{ task.script_args['--table'] }}",

        # Set the attribute that ensures glue logs flow into the airflow UI
        verbose=True,
        create_job_kwargs={
            "GlueVersion": "5.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
            "DefaultArguments": {
                # Set the datalake format to "iceberg"
                "--datalake-formats": "iceberg",
                "--conf": (
                    # Register Iceberg's SQL extensions with Spark — adds support for
                    # MERGE INTO, CALL procedures, and other Iceberg-specific syntax.
                    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                    # Define a Spark catalog named 'iceberg' backed by Iceberg's SparkCatalog
                    # implementation. Tables addressed as iceberg.<db>.<table> route through this.
                    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                    # Use AWS Glue Data Catalog as the metadata store for this catalog —
                    # databases and tables are persisted to Glue, visible to Athena.
                    "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                    # Use Iceberg's native S3 file IO for reading and writing table data —
                    # avoids HDFS-style overhead and integrates with Iceberg's S3 optimizations.
                    "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                    # Root S3 location where this catalog stores Iceberg metadata and data files.
                    # New tables created via this catalog land under <warehouse>/<db>/<table>/.
                    f"--conf spark.sql.catalog.iceberg.warehouse={WAREHOUSE_LOCATION} "
                    # Only overwrite partitions that the incoming dataframe touches; leave
                    # other partitions intact. Required for partition-level upserts/replaces.
                    "--conf spark.sql.sources.partitionOverwriteMode=dynamic"
                ),
            },
        }
    ).expand_kwargs(
        table_keys.map(lambda key: {
            "job_name": f"ingest_raw_{key.strip('/').split('/')[-1]}",
            "script_args": {
                "--table": key.strip("/").split("/")[-1],
                "--landing_path": f"s3://{S3_BUCKET}/{key}",
                "--data_interval": "{{ triggering_asset_events.for_asset(name='raw_ingestion_pending')[-1].extra['data_interval'] }}"
            }
        })
    )

    with TaskGroup("validate_raw") as validate_raw:

        # Pass the following SQL query to an SQLCheckOperator
        # Use an athena connection id
        SQLCheckOperator(
            task_id="check_raw_logs",
            conn_id=ATHENA_CONN_ID,
            sql="""
                SELECT COUNT(*) FROM raw.logs
                WHERE data_interval = '{{ triggering_asset_events.for_asset(name='raw_ingestion_pending')[-1].extra['data_interval'] }}'
            """,
        )

        # Pass the following SQL query to an SQLCheckOperator
        # Use an athena connection id
        SQLCheckOperator(
            task_id="check_raw_songs",
            conn_id=ATHENA_CONN_ID,
            sql="""
                SELECT COUNT(*) FROM raw.songs
                WHERE data_interval = '{{ triggering_asset_events.for_asset(name='raw_ingestion_pending')[-1].extra['data_interval'] }}'
            """,
        )

    # Define a task that sets `outlets` to the 
    # Asset used to trigger the transactions dag
    @task(inlets=[RAW_INGESTION_PENDING], outlets=[RAW_INGESTION_COMPLETE])
    def notify_complete(tables, *, triggering_asset_events, outlet_events) -> None:

        # Add data_interval (and the tables that were ingested)
        # to the outlet's metadata
        data_interval = single_interval(
            triggering_asset_events[RAW_INGESTION_PENDING], RAW_INGESTION_PENDING.name)
        outlet_events[RAW_INGESTION_COMPLETE].extra = {
            "data_interval": data_interval,
            "tables": [key.strip("/").split("/")[-1] for key in tables],
        }

    # Set task dependencies
    # The glue job, validations, and the notification task
    # should run in sequential order
    submit_glue_jobs >> validate_raw >> notify_complete(table_keys)