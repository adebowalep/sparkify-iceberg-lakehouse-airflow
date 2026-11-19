"""Local harness that runs the *real* Glue scripts and SQL templates against synthetic data.

What is real:   raw/glue_script.py, transactions/glue_script.py, analytics/glue_script.py,
                every SQL template under transactions/sql and analytics/sql, Spark SQL, Iceberg MERGE.
What is faked:  the AWS Glue runtime (awsglue module), S3 (SQL text is served from memory),
                and the Iceberg catalog (a local Hadoop catalog instead of the AWS Glue Data Catalog).

These tests therefore verify the transformation logic, not the AWS deployment.
"""
from __future__ import annotations

import ast
import io
import json
import runpy
import sys
import types
from pathlib import Path
from unittest import mock

from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[1]
BUCKET = "test-bucket"


# ───────────────────────────── configuration helpers ─────────────────────────────
def load_tables_config() -> list[dict]:
    """Read the TABLES list from transactions/dag.py without importing Airflow."""
    tree = ast.parse((ROOT / "transactions" / "dag.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "TABLES" for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError("TABLES not found in transactions/dag.py")


class _TI:
    """Stand-in for Airflow's task instance in the Jinja templates."""

    def __init__(self, interval: str):
        self._interval = interval

    def xcom_pull(self, task_ids=None):
        return {"data_interval": self._interval}


def render_sql(path: Path, interval: str | None = None) -> str:
    return Environment().from_string(path.read_text()).render(ti=_TI(interval or "unused"))


# ─────────────────────────────── Glue runtime fakes ───────────────────────────────
def _fake_awsglue_modules(spark):
    def get_resolved_options(argv, names):
        out = {}
        for name in names:
            flag = f"--{name}"
            if flag not in argv:
                raise RuntimeError(f"missing argument {flag}")
            out[name] = argv[argv.index(flag) + 1]
        return out

    class GlueContext:
        def __init__(self, sc):
            self.spark_session = spark

    class Job:
        def __init__(self, ctx):
            pass

        def init(self, name, args=None):
            pass

        def commit(self):
            pass

    awsglue = types.ModuleType("awsglue")
    utils = types.ModuleType("awsglue.utils")
    utils.getResolvedOptions = get_resolved_options
    context = types.ModuleType("awsglue.context")
    context.GlueContext = GlueContext
    job = types.ModuleType("awsglue.job")
    job.Job = Job
    return {"awsglue": awsglue, "awsglue.utils": utils, "awsglue.context": context, "awsglue.job": job}


class _FakeS3:
    def __init__(self, objects: dict):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)].encode("utf-8"))}


def run_script(spark, script: Path, argv: list[str], s3_objects: dict | None = None) -> None:
    """Execute a Glue script file as __main__ with the Glue runtime replaced by local fakes."""
    s3 = _FakeS3(s3_objects or {})
    with mock.patch.dict(sys.modules, _fake_awsglue_modules(spark)), \
            mock.patch.object(sys, "argv", [str(script)] + argv), \
            mock.patch("boto3.client", lambda *_a, **_k: s3), \
            mock.patch("pyspark.context.SparkContext", lambda *_a, **_k: spark.sparkContext):
        try:
            runpy.run_path(str(script), run_name="__main__")
        except SystemExit as exc:  # the raw script exits 0 when a landing folder is empty
            if exc.code not in (0, None):
                raise


# ─────────────────────────────────── the pipeline ───────────────────────────────────
class Pipeline:
    """Drives raw -> transactions -> analytics for synthetic intervals."""

    def __init__(self, spark, workdir: Path):
        self.spark = spark
        self.workdir = workdir
        self.tables = load_tables_config()

    # --- raw ---
    def ingest_raw(self, table: str, interval: str, records: list[dict]) -> None:
        landing = self.workdir / "landing" / interval / table
        landing.mkdir(parents=True, exist_ok=True)
        for old in landing.glob("*.json"):
            old.unlink()
        (landing / "part-0.json").write_text("\n".join(json.dumps(r) for r in records) + "\n")
        run_script(
            self.spark, ROOT / "raw" / "glue_script.py",
            ["--JOB_NAME", f"ingest_{table}", "--table", table,
             "--landing_path", landing.as_uri(), "--data_interval", interval],
        )

    # --- transactions ---
    def promote(self, interval: str, only: list[str] | None = None, sql_override: dict | None = None) -> None:
        for entry in self.tables:
            name = entry["table"]
            if only and name not in only:
                continue
            sql_path = ROOT / "transactions" / "sql" / f"{name}.sql"
            sql_text = (sql_override or {}).get(name) or render_sql(sql_path, interval)
            key = f"artifacts/transactions/sql/test/{name}.sql"
            payload = {"sql": f"s3://{BUCKET}/{key}"}
            payload.update({k: v for k, v in entry.items() if k != "depends_on"})
            run_script(
                self.spark, ROOT / "transactions" / "glue_script.py",
                ["--config", json.dumps(payload)], {(BUCKET, key): sql_text},
            )

    # --- analytics ---
    def refresh_analytics(self) -> None:
        for sql_path in sorted((ROOT / "analytics" / "sql").glob("*.sql")):
            key = f"artifacts/analytics/sql/test/{sql_path.name}"
            payload = {"sql": f"s3://{BUCKET}/{key}", "table": sql_path.stem}
            run_script(
                self.spark, ROOT / "analytics" / "glue_script.py",
                ["--config", json.dumps(payload)], {(BUCKET, key): render_sql(sql_path)},
            )

    # --- convenience ---
    def load_interval(self, interval: str, logs: list[dict], songs: list[dict]) -> None:
        """Ingest raw data for one interval and promote it through the transactions layer."""
        self.ingest_raw("logs", interval, logs)
        self.ingest_raw("songs", interval, songs)
        self.promote(interval)

    def rows(self, query: str) -> list[dict]:
        return [r.asDict() for r in self.spark.sql(query).collect()]


def reset_catalog(spark) -> None:
    for ns in ("raw", "transactions", "analytics"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{ns}")
        for row in spark.sql(f"SHOW TABLES IN iceberg.{ns}").collect():
            spark.sql(f"DROP TABLE IF EXISTS iceberg.{ns}.{row.tableName} PURGE")
