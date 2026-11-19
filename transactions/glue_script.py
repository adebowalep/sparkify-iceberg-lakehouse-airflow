import sys
import json
import boto3
import uuid
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql import Window


# ── Pure helpers (kept in this file because Glue receives a single script) ──────────────
def dedupe(df, keys, version_column=None):
    """One row per key. With a version column the newest row wins; remaining ties are broken by the
    other column values so the result never depends on row order."""
    if not version_column:
        return df.dropDuplicates(keys)
    others = sorted(c for c in df.columns if c not in keys and c != version_column)
    order = [F.col(version_column).desc_nulls_last()] + [F.col(c).asc_nulls_last() for c in others]
    ranked = df.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))
    return ranked.filter("_rn = 1").drop("_rn")


def build_merge_sql(target, staging, keys, columns, version_column=None, keep_existing_when_null=False):
    """MERGE INTO statement.
    - version_column: a matched row is updated only if the incoming row is at least as recent
      (>=, so a corrected re-delivery of the same instant still applies).
    - keep_existing_when_null: matched rows keep the stored value when the incoming value is NULL."""
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    matched = "WHEN MATCHED"
    if version_column:
        matched += f" AND (t.{version_column} IS NULL OR s.{version_column} >= t.{version_column})"
    if keep_existing_when_null:
        assignments = ", ".join(f"t.{c} = COALESCE(s.{c}, t.{c})" for c in columns if c not in keys)
        matched += f" THEN UPDATE SET {assignments}"
    else:
        matched += " THEN UPDATE SET *"
    return f"""
        MERGE INTO {target} t
        USING {staging} s
        ON {on_clause}
        {matched}
        WHEN NOT MATCHED THEN INSERT *
    """


def validate_staging(df, keys, table_name):
    """Fail the job (before anything is written) on data that would silently corrupt the table:
    NULL keys never match in a MERGE, so they would be re-inserted on every run, and timestamps in
    the future are the signature of a unit error (e.g. milliseconds read as seconds)."""
    null_keys = df.filter(" OR ".join(f"{k} IS NULL" for k in keys)).count()
    if null_keys:
        raise RuntimeError(f"{table_name}: {null_keys} staged row(s) have a NULL in key columns {keys}")
    for field in df.schema.fields:
        type_name = field.dataType.simpleString()
        if type_name == "timestamp":
            future = df.filter(F.col(field.name) > F.current_timestamp()).count()
        elif type_name == "date":
            future = df.filter(F.col(field.name) > F.current_date()).count()
        else:
            continue
        if future:
            raise RuntimeError(f"{table_name}.{field.name}: {future} staged value(s) are in the future "
                               f"(check timestamp units)")


# Use getResolvedOptions to collect arguments passed to the script
# Isolate the "config" argument
args = getResolvedOptions(sys.argv, ["config"])
raw_config = args["config"]
# Defensive: Glue also receives a `--conf` (Spark settings) argument, and argparse
# accepts `--conf` as an abbreviation of `--config`. Prefer the exact token.
if "--config" in sys.argv:
    raw_config = sys.argv[sys.argv.index("--config") + 1]

# Use json.loads to convert the config argument 
# to a python dictionary
config = json.loads(raw_config)

table_name = config['table']
sql_s3_path = config['sql']
upsert_keys = config['upsert_keys']
partition_keys = config.get('partition_keys', [])
version_column = config.get('version_column')                          # optional: timestamp-aware update
keep_existing_when_null = config.get('keep_existing_when_null', False)  # optional: never erase with NULL

# Initialize a spark context
sc = SparkContext()

# Wrap the spark context in a GlueContext
glueContext = GlueContext(sc)

# Store the glue context's spark_session to the variable `spark`
spark = glueContext.spark_session


# Unique view for parallel safety
unique_stg = f"stg_{table_name}_{str(uuid.uuid4())[:8]}"

# Using boto3, read the file stored at the bucket an key defined above
# Store the file's string content as the variable `sql_query`
sql_bucket, sql_key = sql_s3_path.replace("s3://", "", 1).split("/", 1)
sql_query = (
    boto3.client("s3")
    .get_object(Bucket=sql_bucket, Key=sql_key)["Body"]
    .read()
    .decode("utf-8")
)

# Use spark to execute the query.
# Store the output to the variable `stg_df`
stg_df = spark.sql(sql_query)

# Drop duplicates on upsert keys (deterministically: newest version first when a version column exists)
stg_df = dedupe(stg_df, upsert_keys, version_column)

# Refuse staged data that would corrupt the table (NULL keys, timestamps in the future)
validate_staging(stg_df, upsert_keys, table_name)

if partition_keys:
    # Sort the dataframe within partitions. Apply this change inplace
    stg_df = stg_df.sortWithinPartitions(*partition_keys)

# Create staging table Temporary View using 
# the `unique_stg` variable defined above
stg_df.createOrReplaceTempView(unique_stg)

target_table = f"iceberg.transactions.{table_name}"

# Check if the production table exists
if not spark.catalog.tableExists(target_table):

    # If it doesn't exist, write the staging table
    # to the production table using iceberg
    writer = (
        spark.table(unique_stg)
        .writeTo(target_table)
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
    )
    
    # If the table has partition keys (defined in the dag.py)
    # configure the table to partitioned by those columns
    if partition_keys:
        writer = writer.partitionedBy(*[F.col(k) for k in partition_keys])
    
    # Create the table
    writer.create()
    print(f"Created Iceberg table {target_table}")
else:

    target_df = spark.table(target_table)
    new_cols = set(stg_df.columns) - set(target_df.columns)
    
    if new_cols:

        for col in new_cols:
            # Iceberg-compatible SQL type (e.g., DECIMAL(10,2))
            col_type = stg_df.schema[col].dataType.simpleString()
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {col} {col_type}")


    # Update records if the record exists in the production table (and is not older than the stored one,
    # see build_merge_sql), insert if it does not.
    spark.sql(build_merge_sql(target_table, unique_stg, upsert_keys, stg_df.columns,
                              version_column, keep_existing_when_null))

# Verify: the table must hold exactly one row per upsert key.
prod_df  = spark.table(target_table)
total    = prod_df.count()
distinct = prod_df.select(*upsert_keys).distinct().count()
print(f"[{table_name}] rows={total:,} distinct_keys={distinct:,}")
if total != distinct:
    raise RuntimeError(f"{target_table} has duplicate rows for keys {upsert_keys}")

print(f"Completed Iceberg merge for {table_name}")
