"""Airflow structure and asset-wiring checks. Need only Airflow (no AWS, no Spark)."""
import importlib.util
import sys
from types import SimpleNamespace
from unittest import mock

import pytest

from pipeline_harness import ROOT, load_tables_config

DAG_FILES = {"raw": "raw/dag.py", "transactions": "transactions/dag.py", "analytics": "analytics/dag.py",
             "run_pipeline": "setup/run_pipeline.py", "setup_s3_data": "setup/setup_s3_data.py"}


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models.dagbag import DagBag

    # Hooks must not be created while the DAG files are parsed (connections are resolved at task runtime).
    with mock.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.__init__", side_effect=AssertionError("S3Hook created at parse time")), \
            mock.patch("airflow.providers.amazon.aws.hooks.glue.GlueJobHook.__init__", side_effect=AssertionError("GlueJobHook created at parse time")), \
            mock.patch("airflow.sdk.Variable.get", side_effect=AssertionError("Variable.get at parse time")):
        bag = DagBag(dag_folder=str(ROOT), include_examples=False)
    return bag


def import_module(name, relpath):
    spec = importlib.util.spec_from_file_location(name, ROOT / relpath)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def asset_names(dag):
    return sorted(a.name for _, a in dag.timetable.asset_condition.iter_assets())


def test_all_project_dags_import_cleanly(dagbag):
    own = {path: err for path, err in dagbag.import_errors.items() if any(path.endswith(f) for f in DAG_FILES.values())}
    assert own == {}
    assert set(DAG_FILES) <= set(dagbag.dags)


@pytest.mark.parametrize("dag_id,upstream_asset", [("raw", "raw_ingestion_pending"), ("transactions", "raw_ingestion_complete"),
                                                    ("analytics", "transactions_complete")])
def test_each_layer_is_scheduled_on_its_upstream_asset_with_required_limits(dagbag, dag_id, upstream_asset):
    dag = dagbag.dags[dag_id]
    assert type(dag.timetable).__name__ == "AssetTriggeredTimetable"
    assert asset_names(dag) == [upstream_asset]
    assert dag.max_active_runs == 1 and dag.max_active_tasks == 2


def test_assets_are_emitted_by_the_producing_dag(dagbag):
    def outlets(dag_id, task_id):
        return [o.name for o in dagbag.dags[dag_id].get_task(task_id).outlets]
    assert outlets("run_pipeline", "emit_ready") == ["raw_ingestion_pending"]
    assert outlets("raw", "notify_complete") == ["raw_ingestion_complete"]
    assert outlets("transactions", "notify_complete") == ["transactions_complete"]


def test_raw_graph_discovers_then_ingests_then_validates_then_notifies(dagbag):
    dag = dagbag.dags["raw"]
    assert dag.get_task("capture_landing_keys").downstream_task_ids == {"ingest", "notify_complete"}
    assert "validate_raw.check_raw_logs" in dag.get_task("ingest").downstream_task_ids
    assert dag.get_task("notify_complete").upstream_task_ids >= {"validate_raw.check_raw_logs", "validate_raw.check_raw_songs"}
    ingest = dag.get_task("ingest")
    assert type(ingest).__name__ == "MappedOperator", "one Glue job per discovered table (dynamic task mapping)"


def test_transactions_promotion_order_follows_declared_dependencies(dagbag):
    dag = dagbag.dags["transactions"]
    tables = load_tables_config()
    order = [t["table"] for t in tables]
    for t in tables:
        promote = dag.get_task(f"promote_{t['table']}")
        assert f"{t['table']}_upload_sql" in promote.upstream_task_ids
        for dep in t["depends_on"]:
            assert f"promote_{dep}" in promote.upstream_task_ids
            assert order.index(dep) < order.index(t["table"])
    # the spec'd ordering
    deps = {t["table"]: set(t["depends_on"]) for t in tables}
    assert deps["songs"] == {"artists"} and deps["song_versions"] == {"songs"} and deps["user_levels"] == {"users"}
    assert deps["events"] == {"users", "songs", "artists", "song_versions"}


def test_transactions_completion_is_gated_by_every_validation(dagbag):
    dag = dagbag.dags["transactions"]
    checks = {t.task_id for t in dag.tasks if t.task_id.startswith("validate_transactions.")}
    assert checks == {f"validate_transactions.check_pk_{t['table']}" for t in load_tables_config()} | {
        "validate_transactions.check_version_resolution"}
    assert dag.get_task("notify_complete").upstream_task_ids >= checks
    for task_id in checks:
        assert dag.get_task(task_id).trigger_rule == "none_failed_min_one_success"
        assert {f"promote_{t['table']}" for t in load_tables_config()} <= dag.get_task(task_id).upstream_task_ids


def test_analytics_replaces_four_tables_and_each_upload_precedes_its_job(dagbag):
    dag = dagbag.dags["analytics"]
    for name in ("user_facts", "session_facts", "song_facts", "artist_facts"):
        assert dag.get_task(f"promote_{name}").upstream_task_ids == {f"{name}_upload_sql"}


# ─────────────── which interval does a run process? (run-scoped events) ───────────────
def event(interval):
    return SimpleNamespace(extra={"data_interval": interval})


class ByAsset:
    """Minimal stand-in for Airflow's asset-keyed accessors (Asset objects are not hashable)."""

    def __init__(self):
        self.items = {}

    def __getitem__(self, asset):
        return self.items.setdefault(asset.name, [])

    def __setitem__(self, asset, value):
        self.items[asset.name] = value


def accessor(asset, *intervals):
    acc = ByAsset()
    acc[asset] = [event(i) for i in intervals]
    return acc


class Outlets(ByAsset):
    def __getitem__(self, asset):
        return self.items.setdefault(asset.name, SimpleNamespace(extra=None))


def test_interval_is_taken_from_the_events_that_triggered_the_run_not_the_newest_event(dagbag):
    raw = import_module("raw_dag_under_test", "raw/dag.py")
    asset = raw.RAW_INGESTION_PENDING
    fake_hook = mock.MagicMock()
    fake_hook.list_prefixes.return_value = ["data_intervals/2026-01-02/logs/"]
    with mock.patch.object(raw, "S3Hook", return_value=fake_hook):
        # this (queued) run was triggered by the 01-02 event, even if a newer 01-03 event already exists
        out = raw.capture_landing_keys.function("bucket", triggering_asset_events=accessor(asset, "2026-01-02"))
    assert out == ["data_intervals/2026-01-02/logs/"]
    assert fake_hook.list_prefixes.call_args.kwargs["prefix"] == "data_intervals/2026-01-02/"


def test_run_started_by_several_intervals_is_refused_instead_of_skipping_one(dagbag):
    raw = import_module("raw_dag_under_test2", "raw/dag.py")
    asset = raw.RAW_INGESTION_PENDING
    with pytest.raises(ValueError, match="several intervals"):
        raw.capture_landing_keys.function("bucket", triggering_asset_events=accessor(asset, "2026-01-02", "2026-01-03"))


def test_same_interval_delivered_twice_is_accepted(dagbag):
    raw = import_module("raw_dag_under_test3", "raw/dag.py")
    assert raw.single_interval([event("2026-01-02"), event("2026-01-02")], "x") == "2026-01-02"


def test_run_without_a_triggering_event_fails_with_a_clear_message(dagbag):
    raw = import_module("raw_dag_under_test4", "raw/dag.py")
    with pytest.raises(ValueError, match="run_pipeline"):
        raw.capture_landing_keys.function("bucket", triggering_asset_events=accessor(raw.RAW_INGESTION_PENDING))


def test_completion_event_carries_interval_and_tables(dagbag):
    raw = import_module("raw_dag_under_test5", "raw/dag.py")
    outlet = Outlets()
    raw.notify_complete.function(["data_intervals/2026-01-02/logs/", "data_intervals/2026-01-02/songs/"],
                                 triggering_asset_events=accessor(raw.RAW_INGESTION_PENDING, "2026-01-02"),
                                 outlet_events=outlet)
    assert outlet[raw.RAW_INGESTION_COMPLETE].extra == {"data_interval": "2026-01-02", "tables": ["logs", "songs"]}


def test_transactions_reads_its_own_triggering_event(dagbag):
    tx = import_module("transactions_dag_under_test", "transactions/dag.py")
    got = tx.metadata.function(triggering_asset_events=accessor(tx.RAW_INGESTION_COMPLETE, "2026-01-02"))
    assert got == {"data_interval": "2026-01-02"}
    with pytest.raises(ValueError, match="several intervals"):
        tx.metadata.function(triggering_asset_events=accessor(tx.RAW_INGESTION_COMPLETE, "2026-01-01", "2026-01-02"))
