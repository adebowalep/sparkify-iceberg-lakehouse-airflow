"""Synthetic source records (same shape as the Sparkify JSON logs and song files)."""
from __future__ import annotations

from datetime import datetime, timezone


def ms(day: int, hour: int = 12, minute: int = 0) -> int:
    """Epoch milliseconds for 2018-11-<day> <hour>:<minute> UTC (the real data is Nov 2018)."""
    return int(datetime(2018, 11, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


REGISTRATION = ms(1, 8)


def play(user: int, ts: int, *, artist="Artist A", song="Song One", length=200.0, session=1, item=0,
         level="free", location="Paris, FR", first="Ana", last="Lee", gender="F") -> dict:
    """A NextSong log record."""
    return {
        "artist": artist, "auth": "Logged In", "firstName": first, "gender": gender, "itemInSession": item,
        "lastName": last, "length": length, "level": level, "location": location, "method": "PUT",
        "page": "NextSong", "registration": REGISTRATION, "sessionId": session, "song": song, "status": 200,
        "ts": ts, "userAgent": "UA/1.0", "userId": str(user),
    }


def page(user: int, ts: int, page_name: str, *, session=1, item=0, level="free", location="Paris, FR",
         first="Ana", last="Lee", gender="F") -> dict:
    """A non-playback log record (Home, Submit Upgrade, ...). Song fields are null as in the real data."""
    rec = play(user, ts, session=session, item=item, level=level, location=location, first=first, last=last,
               gender=gender)
    rec.update({"page": page_name, "artist": None, "song": None, "length": None})
    return rec


def song(song_id: str, title: str, artist_id: str, artist_name: str, duration: float, *, year=2010,
         location="Paris, FR", latitude=48.8, longitude=2.3) -> dict:
    return {
        "num_songs": 1, "artist_id": artist_id, "artist_latitude": latitude, "artist_longitude": longitude,
        "artist_location": location, "artist_name": artist_name, "song_id": song_id, "title": title,
        "duration": duration, "year": year,
    }
