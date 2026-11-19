import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# ── Read job arguments ────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", 
    "table", 
    "landing_path",
    "data_interval",
])

# ── Configure spark ───────────────────────────────────────────────────────────

# Initialize a spark context
sc = SparkContext()

# Wrap the spark context in a GlueContext
glueContext = GlueContext(sc)

# Store the glue context's spark_session to the variable `spark`
spark = glueContext.spark_session

# Init a glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args) 

# ── Set up locations ────────────────────────────────────────────────────────────

table = args["table"]
landing_path = args["landing_path"]

ICEBERG_TABLE = f"iceberg.raw.{table}"

# ── Read landing files ──────────────────────────────────────────────────────────

data_interval = args["data_interval"]

# Spark infers schema from JSON natively — no header/inferSchema options needed.
# Default reader expects newline-delimited JSON (one object per line); for files
# containing a single object or pretty-printed JSON, add .option("multiLine", "true").
df = spark.read.json(landing_path)

if df.rdd.isEmpty():
    print(f"No files at {landing_path} — skipping.")
    job.commit()
    sys.exit(0)

# Add data_interval column to the dataframe
df = df.withColumn("data_interval", F.lit(data_interval))

# ── Write to Iceberg ────────────────────────────────────────────────────────────

# Check if the table exists
if not spark.catalog.tableExists(ICEBERG_TABLE):

    # If the table doesn't exists create the table and write 
    # the raw data to it using iceberg
    (
        df
        .writeTo(ICEBERG_TABLE)
        # Set "data_interval" as the partition column
        .partitionedBy(F.col("data_interval"))
        # Use Iceberg as the table format
        .using("iceberg")
        # Set Parquet as the storage format for data files
        .tableProperty("write.format.default", "parquet")
        # Snappy compression: high write speed, high read speed, moderate storage costs
        .tableProperty("write.parquet.compression-codec", "snappy")
        .create()
    )
    print(f"[{table}] Created Iceberg table: {ICEBERG_TABLE}")
else:
    # Schema evolution: add columns that are new in this interval's source files
    # and align existing columns (names/types) with the table, so a drifting
    # source never breaks ingestion.
    target_schema = {f.name.lower(): f for f in spark.table(ICEBERG_TABLE).schema.fields}
    for field in df.schema.fields:
        if field.name.lower() not in target_schema:
            spark.sql(
                f"ALTER TABLE {ICEBERG_TABLE} ADD COLUMN `{field.name}` {field.dataType.simpleString()}"
            )
            print(f"[{table}] Added new column {field.name} ({field.dataType.simpleString()})")
    target_fields = spark.table(ICEBERG_TABLE).schema.fields
    df_cols = {c.lower(): c for c in df.columns}
    df = df.select([
        (F.col(f"`{df_cols[f.name.lower()]}`").cast(f.dataType)
         if f.name.lower() in df_cols else F.lit(None).cast(f.dataType)).alias(f.name)
        for f in target_fields
    ])

    # If the table exists, overwrite the partitions
    # found in the raw data
    df.writeTo(ICEBERG_TABLE).overwritePartitions()
    print(f"[{table}] Overwrote partition data_interval={data_interval}")

count = spark.sql(f"""
    SELECT COUNT(*) FROM {ICEBERG_TABLE}
    WHERE data_interval = '{data_interval}'
""").collect()[0][0]
print(f"[{table}] {count:,} rows in partition data_interval={data_interval}")
print(f"[{table}] Ingestion finished: table={ICEBERG_TABLE} data_interval={data_interval} rows={count}")

job.commit()
