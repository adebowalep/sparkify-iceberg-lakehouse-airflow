"""What happens when an already-loaded interval is corrected at the source.

Supported behaviour (the transactions layer is insert/update only, as the original assignment specifies
`MERGE INTO` upserts): corrected and new records are applied; records REMOVED from a corrected interval are
NOT deleted from the transactions layer. `events` carries no source-interval column (the data dictionary fixes
its schema), so a safely scoped delete is not possible without changing the model. See README "Known limitations".
"""
from data import ms, play, song

A = "2026-01-01"
SONGS = [song("S1", "Song One", "AR1", "Artist A", 200.0)]


def events(pipeline):
    return {r["session_id"]: r for r in pipeline.rows(
        "SELECT session_id, level, location FROM iceberg.transactions.events ORDER BY session_id")}


def initial(pipeline):
    logs = [play(100, ms(1, 1), session=1, level="free", location="Paris, FR"),
            play(100, ms(1, 2), session=2, level="free", location="Paris, FR"),
            play(100, ms(1, 3), session=3, level="free", location="Paris, FR")]
    pipeline.load_interval(A, logs, SONGS)
    return logs


def test_unchanged_interval_replay_changes_nothing(pipeline):
    logs = initial(pipeline)
    before = events(pipeline)
    pipeline.load_interval(A, logs, SONGS)
    assert events(pipeline) == before


def test_corrected_record_is_updated_in_place(pipeline):
    initial(pipeline)
    corrected = [play(100, ms(1, 1), session=1, level="paid", location="Lyon, FR"),
                 play(100, ms(1, 2), session=2), play(100, ms(1, 3), session=3)]
    pipeline.load_interval(A, corrected, SONGS)
    got = events(pipeline)
    assert len(got) == 3 and got[1]["level"] == "paid" and got[1]["location"] == "Lyon, FR"


def test_new_record_in_a_reloaded_interval_is_inserted(pipeline):
    logs = initial(pipeline)
    pipeline.load_interval(A, logs + [play(100, ms(1, 4), session=4)], SONGS)
    assert set(events(pipeline)) == {1, 2, 3, 4}


def test_removed_source_record_is_retained_in_transactions_documented_limitation(pipeline):
    logs = initial(pipeline)
    pipeline.load_interval(A, [logs[0], logs[2]], SONGS)          # session 2 removed from the corrected source
    raw_sessions = {r["sessionId"] for r in pipeline.rows(
        "SELECT sessionId FROM iceberg.raw.logs WHERE data_interval = '2026-01-01'")}
    assert raw_sessions == {1, 3}, "raw layer replaces the partition, so the removed record is gone there"
    assert set(events(pipeline)) == {1, 2, 3}, "documented limitation: transactions is insert/update only"


def test_other_intervals_are_untouched_by_a_correction(pipeline):
    initial(pipeline)
    pipeline.load_interval("2026-01-02", [play(200, ms(2), session=9)], SONGS)
    pipeline.load_interval(A, [play(100, ms(1, 1), session=1, level="paid")], SONGS)
    assert {1, 2, 3, 9} <= set(events(pipeline))
