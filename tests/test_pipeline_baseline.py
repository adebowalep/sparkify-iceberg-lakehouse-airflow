"""Baseline behaviour of the existing pipeline on synthetic data (must keep passing after any change)."""
from data import ms, page, play, song

A, B, C = "2026-01-01", "2026-01-02", "2026-01-03"


def test_three_intervals_flow_through_all_layers(pipeline):
    songs = [song("S1", "Song One", "AR1", "Artist A", 200.0)]
    pipeline.load_interval(A, [play(100, ms(1)), page(100, ms(1, 13), "Home", item=1)], songs)
    pipeline.load_interval(B, [page(100, ms(2), "Submit Upgrade", session=2, level="paid"),
                               play(100, ms(2, 1), session=2, item=1, level="paid")], songs)
    pipeline.load_interval(C, [play(100, ms(3), session=3, location="London, UK"),
                               play(200, ms(3, 1), session=4, first="Bo", last="Kay", location="Rome, IT")], songs)
    pipeline.refresh_analytics()

    counts = {r["t"]: r["n"] for r in pipeline.rows(
        "SELECT 'users' t, COUNT(*) n FROM iceberg.transactions.users "
        "UNION ALL SELECT 'events', COUNT(*) FROM iceberg.transactions.events "
        "UNION ALL SELECT 'songs', COUNT(*) FROM iceberg.transactions.songs "
        "UNION ALL SELECT 'user_facts', COUNT(*) FROM iceberg.analytics.user_facts")}
    assert counts == {"users": 2, "events": 4, "songs": 1, "user_facts": 2}


def test_same_interval_twice_is_idempotent(pipeline):
    songs = [song("S1", "Song One", "AR1", "Artist A", 200.0)]
    logs = [play(100, ms(1)), play(100, ms(1, 1), item=1)]
    pipeline.load_interval(A, logs, songs)
    pipeline.refresh_analytics()
    before = pipeline.rows("SELECT * FROM iceberg.transactions.events ORDER BY event_id")
    facts_before = pipeline.rows("SELECT * FROM iceberg.analytics.user_facts")

    pipeline.load_interval(A, logs, songs)
    pipeline.refresh_analytics()
    assert pipeline.rows("SELECT * FROM iceberg.transactions.events ORDER BY event_id") == before
    assert pipeline.rows("SELECT * FROM iceberg.analytics.user_facts") == facts_before


def test_timestamps_are_converted_from_milliseconds(pipeline):
    songs = [song("S1", "Song One", "AR1", "Artist A", 200.0)]
    pipeline.load_interval(A, [play(100, ms(1, 12))], songs)
    ev = pipeline.rows("SELECT CAST(ts AS STRING) ts, CAST(event_date AS STRING) d FROM iceberg.transactions.events")[0]
    assert ev["d"] == "2018-11-01" and ev["ts"].startswith("2018-11-01 12:00:00")
    user = pipeline.rows("SELECT CAST(registration AS STRING) r, CAST(last_seen_at AS STRING) l "
                         "FROM iceberg.transactions.users")[0]
    assert user["r"].startswith("2018-11-01 08:00:00") and user["l"].startswith("2018-11-01 12:00:00")
