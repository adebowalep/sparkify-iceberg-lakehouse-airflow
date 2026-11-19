"""Per-table update semantics of the transactions MERGE and the staging-data checks."""
import pytest

from data import ms, play, song
from pipeline_harness import reset_catalog

A, B, C = "2026-01-01", "2026-01-02", "2026-01-03"
LOGS = [play(100, ms(1))]


def artist_row(pipeline):
    return pipeline.rows("SELECT artist_name, location, latitude FROM iceberg.transactions.artists WHERE artist_id = 'AR1'")[0]


def test_incoming_null_does_not_erase_known_artist_and_song_attributes(pipeline):
    pipeline.load_interval(A, LOGS, [song("S1", "Song One", "AR1", "Artist A", 200.0, year=2010, location="Paris, FR")])
    # A later interval repeats the same artist/song but with unknown location, latitude and year (0 = unknown)
    pipeline.load_interval(B, [play(100, ms(2), session=2)],
                           [song("S1", "Song One", "AR1", "Artist A", 200.0, year=0, location=None, latitude=None)])
    assert artist_row(pipeline) == {"artist_name": "Artist A", "location": "Paris, FR", "latitude": 48.8}
    assert pipeline.rows("SELECT year FROM iceberg.transactions.songs")[0]["year"] == 2010


def test_known_catalog_values_are_still_corrected_by_new_non_null_values(pipeline):
    pipeline.load_interval(A, LOGS, [song("S1", "Song One", "AR1", "Artist A", 200.0, location="Paris, FR")])
    pipeline.load_interval(B, [play(100, ms(2), session=2)],
                           [song("S1", "Song One", "AR1", "Artist A", 200.0, location="Lyon, FR")])
    assert artist_row(pipeline)["location"] == "Lyon, FR"


def test_conflicting_catalog_rows_resolve_independently_of_input_order(pipeline, spark):
    rows = [song("S1", "Song One", "AR1", "Artist A", 200.0, year=2011, location="Paris, FR"),
            song("S1", "Song One (Remastered)", "AR1", "Artist A Band", 200.0, year=2011, location="Rome, IT")]
    results = []
    for ordering in (rows, rows[::-1]):
        reset_catalog(spark)
        pipeline.load_interval(A, LOGS, ordering)
        results.append((artist_row(pipeline), pipeline.rows("SELECT title FROM iceberg.transactions.songs")))
    assert results[0] == results[1]


def _users_sql(first_expr="'Ana'", user_expr="CAST(100 AS BIGINT)", registration="TIMESTAMP '2018-11-01 08:00:00'",
               last_seen="TIMESTAMP '2018-11-01 12:00:00'"):
    return (f"SELECT {user_expr} AS user_id, {first_expr} AS first_name, 'Lee' AS last_name, 'F' AS gender, "
            f"'Paris' AS location, {registration} AS registration, {last_seen} AS last_seen_at")


def test_null_primary_key_is_rejected_before_anything_is_written(pipeline):
    with pytest.raises(RuntimeError, match="NULL in key columns"):
        pipeline.promote(A, only=["users"], sql_override={"users": _users_sql(user_expr="CAST(NULL AS BIGINT)")})
    assert not pipeline.spark.catalog.tableExists("iceberg.transactions.users")


def test_timestamps_in_the_future_are_rejected(pipeline):
    """The original provided SQL read millisecond epochs as seconds (year ~50,000); that must not pass."""
    buggy = (
        "SELECT CAST(userid AS BIGINT) AS user_id, firstname AS first_name, lastname AS last_name, gender, location, "
        "from_unixtime(registration) AS registration, from_unixtime(ts) AS event_ts FROM iceberg.raw.logs"
    )
    pipeline.ingest_raw("logs", A, LOGS)
    buggy_sql = (f"SELECT user_id, first_name, last_name, gender, location, "
                 f"CAST(registration AS TIMESTAMP) AS registration, CAST(event_ts AS TIMESTAMP) AS last_seen_at FROM ({buggy})")
    with pytest.raises(RuntimeError, match="in the future"):
        pipeline.promote(A, only=["users"], sql_override={"users": buggy_sql})
