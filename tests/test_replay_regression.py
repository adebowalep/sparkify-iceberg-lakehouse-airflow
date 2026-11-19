"""Replaying an older interval after newer ones must not regress current state (late / out-of-order data)."""
import pytest

from data import ms, page, play, song
from pipeline_harness import reset_catalog

A, B, C = "2026-01-01", "2026-01-02", "2026-01-03"
SONGS = [song("S1", "Song One", "AR1", "Artist A", 200.0)]

LOGS = {
    A: [play(100, ms(1), session=1, location="Paris, FR", last="Lee"),
        page(100, ms(1, 13), "Home", session=1, item=1, location="Paris, FR", last="Lee")],
    B: [page(100, ms(2), "Submit Upgrade", session=2, level="paid", location="Paris, FR", last="Lee"),
        play(100, ms(2, 1), session=2, item=1, level="paid", location="Paris, FR", last="Lee")],
    C: [play(100, ms(3), session=3, level="paid", location="London, UK", last="Lee-Smith")],
}

USER_SQL = ("SELECT user_id, first_name, last_name, location, CAST(last_seen_at AS STRING) last_seen_at, "
            "CAST(registration AS STRING) registration FROM iceberg.transactions.users ORDER BY user_id")


def load_all(pipeline):
    for interval in (A, B, C):
        pipeline.load_interval(interval, LOGS[interval], SONGS)


def snapshot(pipeline):
    pipeline.refresh_analytics()
    return {
        "users": pipeline.rows(USER_SQL),
        "levels": pipeline.rows("SELECT user_id, CAST(changed_at AS STRING) changed_at, new_level, source "
                                "FROM iceberg.transactions.user_levels ORDER BY changed_at"),
        "events": pipeline.rows("SELECT event_id, CAST(ts AS STRING) ts, session_id, version_id, song_id, artist_id, level "
                                "FROM iceberg.transactions.events ORDER BY event_id"),
        "user_facts": pipeline.rows("SELECT * FROM iceberg.analytics.user_facts ORDER BY user_id"),
        "session_facts": pipeline.rows("SELECT * FROM iceberg.analytics.session_facts ORDER BY session_id"),
    }


def test_latest_state_after_in_order_load(pipeline):
    load_all(pipeline)
    user = pipeline.rows(USER_SQL)[0]
    assert user["location"] == "London, UK" and user["last_name"] == "Lee-Smith"
    assert user["last_seen_at"].startswith("2018-11-03 12:00:00")


@pytest.mark.parametrize("replayed", [A, B])
def test_replaying_older_interval_does_not_regress_current_state(pipeline, replayed):
    load_all(pipeline)
    before = snapshot(pipeline)

    pipeline.load_interval(replayed, LOGS[replayed], SONGS)   # late replay of an older interval
    after = snapshot(pipeline)

    assert after["users"] == before["users"], "older interval overwrote newer user attributes / last_seen_at"
    assert after["levels"] == before["levels"], "subscription history changed"
    assert after["events"] == before["events"], "events duplicated or changed"
    assert after["user_facts"] == before["user_facts"]
    assert after["session_facts"] == before["session_facts"]


def test_out_of_order_first_load_converges_to_in_order_state(pipeline, spark):
    for interval in (C, A, B):                        # C arrives first, then older data
        pipeline.load_interval(interval, LOGS[interval], SONGS)
    out_of_order = pipeline.rows(USER_SQL)

    reset_catalog(spark)
    load_all(pipeline)
    assert pipeline.rows(USER_SQL) == out_of_order


def _tie_logs(order):
    r1 = play(300, ms(1), session=5, item=0, location="Paris, FR", first="Zed")
    r2 = play(300, ms(1), session=6, item=0, location="Rome, IT", first="Zed")   # same user, same ts
    return [r1, r2] if order == "r1r2" else [r2, r1]


def test_identical_timestamps_resolve_deterministically_within_a_batch(pipeline, spark):
    results = []
    for order in ("r1r2", "r2r1"):
        reset_catalog(spark)
        pipeline.load_interval(A, _tie_logs(order), SONGS)
        results.append(pipeline.rows(USER_SQL))
    assert results[0] == results[1], "same input in a different row order produced a different user state"


def test_same_timestamp_correction_in_later_run_is_applied(pipeline):
    """A replay carrying the same event instant but corrected values replaces the stored values."""
    pipeline.load_interval(A, [play(400, ms(1), location="Paris, FR")], SONGS)
    pipeline.load_interval(A, [play(400, ms(1), location="Lyon, FR")], SONGS)
    assert pipeline.rows(USER_SQL)[0]["location"] == "Lyon, FR"


@pytest.mark.xfail(strict=True, reason=(
    "KNOWN LIMITATION: the 'initial' level row is derived from the raw intervals loaded so far, so loading a later "
    "interval before an earlier one leaves an extra 'initial' row (stale seed). Current levels are unaffected."))
def test_out_of_order_first_load_user_levels_match_in_order_load(pipeline, spark):
    """The 'initial' level row is derived from the raw intervals loaded so far."""
    q = ("SELECT user_id, CAST(changed_at AS STRING) changed_at, new_level, source "
         "FROM iceberg.transactions.user_levels ORDER BY changed_at, source")
    load_all(pipeline)
    in_order = pipeline.rows(q)
    reset_catalog(spark)
    for interval in (C, A, B):
        pipeline.load_interval(interval, LOGS[interval], SONGS)
    assert pipeline.rows(q) == in_order
