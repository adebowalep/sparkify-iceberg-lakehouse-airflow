import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

try:  # optional: a bundled JDK 17 (the project's dev venv); CI installs a real JDK instead
    import jdk4py

    os.environ.setdefault("JAVA_HOME", str(jdk4py.JAVA_HOME))
except ImportError:
    pass

from pipeline_harness import Pipeline, reset_catalog  # noqa: E402

# Versions used by AWS Glue 5.0 (Spark 3.5.4, Iceberg 1.7.x)
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    warehouse = tempfile.mkdtemp(prefix="iceberg-wh-")
    session = (
        SparkSession.builder.master("local[1]").appName("sparkify-tests")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")  # local stand-in for the Glue catalog
        .config("spark.sql.catalog.iceberg.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def pipeline(spark, tmp_path):
    reset_catalog(spark)
    return Pipeline(spark, tmp_path)
