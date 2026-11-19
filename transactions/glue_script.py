import sys
import json
import boto3
import uuid
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

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

# Drop duplicates on upsert keys
stg_df = stg_df.dropDuplicates(upsert_keys)

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


    # Inclusion of partition keys in 'on_clause' enables Partition Pruning
    on_clause = " AND ".join([f"t.{k} = s.{k}" for k in upsert_keys])
    
    # Using spark, merge the staging table's data
    # into the production table.
    # Update records if the record exists in the production table
    # Insert if the record doesn't exist in the production table
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {unique_stg} s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# Verify: the table must hold exactly one row per upsert key.
prod_df  = spark.table(target_table)
total    = prod_df.count()
distinct = prod_df.select(*upsert_keys).distinct().count()
print(f"[{table_name}] rows={total:,} distinct_keys={distinct:,}")
if total != distinct:
    raise RuntimeError(f"{target_table} has duplicate rows for keys {upsert_keys}")

print(f"Completed Iceberg merge for {table_name}")
