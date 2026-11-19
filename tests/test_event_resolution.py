"""Resolving a play event to (artist, song, version) must respect the artist's identity."""
import pytest

from data import ms, play, song
from pipeline_harness import reset_catalog

A = "2026-01-01"


def resolve(pipeline, plays, songs, interval=A):
    pipeline.load_interval(interval, plays, songs)
    rows = pipeline.rows(
        "SELECT session_id, song_id, artist_id, version_id FROM iceberg.transactions.events ORDER BY session_id")
    return {r["session_id"]: r for r in rows}


def p(session, artist, title, length):
    return play(100 + session, ms(1, session), session=session, artist=artist, song=title, length=length)


STAY_A = song("SA", "Stay", "ARA", "Artist A", 210.0)
STAY_B = song("SB", "Stay", "ARB", "Artist B", 210.0)


def test_same_title_by_two_artists_resolves_to_the_playing_artist(pipeline):
    got = resolve(pipeline, [p(1, "Artist A", "Stay", 210.0), p(2, "Artist B", "Stay", 210.0)], [STAY_A, STAY_B])
    assert (got[1]["song_id"], got[1]["artist_id"]) == ("SA", "ARA")
    assert (got[2]["song_id"], got[2]["artist_id"]) == ("SB", "ARB")


def test_same_title_resolution_feeds_correct_song_and_artist_facts(pipeline):
    resolve(pipeline, [p(1, "Artist A", "Stay", 210.0), p(2, "Artist B", "Stay", 210.0), p(3, "Artist B", "Stay", 210.0)],
            [STAY_A, STAY_B])
    pipeline.refresh_analytics()
    plays = {r["song_id"]: r["total_plays"] for r in pipeline.rows("SELECT song_id, total_plays FROM iceberg.analytics.song_facts")}
    assert plays == {"SA": 1, "SB": 2}
    artists = {r["artist_id"]: r["total_plays"] for r in pipeline.rows("SELECT artist_id, total_plays FROM iceberg.analytics.artist_facts")}
    assert artists == {"ARA": 1, "ARB": 2}


def test_unknown_artist_with_a_known_title_stays_unresolved(pipeline):
    got = resolve(pipeline, [p(1, "Nobody", "Stay", 210.0)], [STAY_A, STAY_B])
    assert got[1]["song_id"] is None and got[1]["artist_id"] is None and got[1]["version_id"] is None


def test_song_missing_from_catalog_stays_unresolved(pipeline):
    got = resolve(pipeline, [p(1, "Artist A", "Unreleased", 100.0)], [STAY_A])
    assert got[1]["song_id"] is None and got[1]["artist_id"] is None and got[1]["version_id"] is None


def test_known_song_but_unmatched_duration_keeps_artist_but_no_version(pipeline):
    """Documented rule: artist is resolved from (title, artist name); song_id/version_id need a version within 0.01s."""
    got = resolve(pipeline, [p(1, "Artist A", "Stay", 333.0)], [STAY_A])
    assert got[1]["artist_id"] == "ARA" and got[1]["song_id"] is None and got[1]["version_id"] is None


def test_one_song_with_several_versions_picks_the_matching_duration(pipeline):
    versions = [song("SA", "Stay", "ARA", "Artist A", 210.0), song("SA", "Stay", "ARA", "Artist A", 180.0),
                song("SA", "Stay", "ARA", "Artist A", 180.004)]
    got = resolve(pipeline, [p(1, "Artist A", "Stay", 210.0), p(2, "Artist A", "Stay", 180.0), p(3, "Artist A", "Stay", 180.004)],
                  versions)
    assert len({got[1]["version_id"], got[2]["version_id"], got[3]["version_id"]}) == 3, "versions must be distinct"
    assert {g["song_id"] for g in got.values()} == {"SA"}


def test_similar_durations_prefer_the_closest_version(pipeline):
    versions = [song("SA", "Stay", "ARA", "Artist A", 180.000), song("SA", "Stay", "ARA", "Artist A", 180.006)]
    got = resolve(pipeline, [p(1, "Artist A", "Stay", 180.005)], versions)
    expected = pipeline.rows("SELECT version_id FROM iceberg.transactions.song_versions WHERE duration > 180.003")[0]["version_id"]
    assert got[1]["version_id"] == expected


def test_equally_plausible_matches_resolve_deterministically(pipeline, spark):
    """Two song_ids with the same title, artist and duration: the choice must not depend on load order."""
    twins = [song("SB1", "Stay", "ARB", "Artist B", 210.0), song("SB2", "Stay", "ARB", "Artist B", 210.0)]
    outcomes = []
    for ordering in (twins, twins[::-1]):
        reset_catalog(spark)
        outcomes.append(resolve(pipeline, [p(1, "Artist B", "Stay", 210.0)], ordering)[1])
    assert outcomes[0] == outcomes[1]
