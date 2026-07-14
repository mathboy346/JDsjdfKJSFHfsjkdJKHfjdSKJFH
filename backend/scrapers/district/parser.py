"""Parse District's __NEXT_DATA__ payload (as returned by district-proxy) into
flat row records, mirroring the shape backend/scrapers/parser.py produces for
BMS so downstream ingestion code can stay parallel.

Confirmed live 2026-07-14: a single (movie, city) fetch returns sessions for
multiple upcoming dates in one shot (sessionDates typically covers today
plus the next couple of days) — worth using for the advance pipeline too,
not just daily, since it's the same request either way.
"""

from datetime import datetime


def _extract_movie_sessions(raw: dict) -> dict | None:
    try:
        server_state = raw["props"]["pageProps"]["data"]["serverState"]
    except (KeyError, TypeError):
        return None
    movie_sessions = server_state.get("movieSessions") or {}
    if not movie_sessions:
        return None
    # There's exactly one query per page fetch; grab the (only) entry.
    return next(iter(movie_sessions.values()), None)


def _format_time(show_time_iso: str) -> str:
    """'2026-07-14T09:50' -> '09:50 AM'. District's showTime is local time,
    not UTC (unlike what the old district_tracking-main scraper assumed)."""
    try:
        dt = datetime.strptime(show_time_iso, "%Y-%m-%dT%H:%M")
        return dt.strftime("%I:%M %p").lstrip("0") or dt.strftime("%I:%M %p")
    except ValueError:
        return show_time_iso


def parse_payload(raw: dict, wanted_date_codes: set[str]) -> list[dict]:
    """wanted_date_codes: e.g. {'20260714'} for daily, {'20260715','20260716'}
    for advance — filters the multi-date response down to what this shard
    call is responsible for."""
    out: list[dict] = []
    obj = _extract_movie_sessions(raw)
    if not obj:
        return out

    movie_meta = (obj.get("meta") or {}).get("movie") or {}
    district_movie_id = movie_meta.get("content_id")
    movie_name = movie_meta.get("name", "Unknown")
    primary_language = movie_meta.get("primary_language", "")
    runtime_minutes = movie_meta.get("duration")

    for cinema_entry in obj.get("arrangedSessions") or []:
        cinema = cinema_entry.get("data") or {}
        venue_name = cinema.get("name", "Unknown")
        # chainKey is the real cinema brand (e.g. "Cinepolis", "Suresh
        # Production"); clientId is the booking-tech backend a venue routes
        # through (e.g. "ticketnew") and is NOT a chain — confirmed live by a
        # genuine Cinepolis venue carrying clientId "ticketnew". Prefer
        # chainKey; fall back to clientId only when a venue has no chainKey
        # at all (independent single-screens with no bigger brand).
        chain = cinema.get("chainKey") or cinema.get("clientId") or "Unknown"
        district_venue_id = cinema.get("id")
        city = cinema.get("city", "Unknown")
        state = cinema.get("state", "Unknown")

        for sess in cinema_entry.get("sessions") or []:
            show_time_iso = sess.get("showTime", "")
            date_code = show_time_iso[:10].replace("-", "") if show_time_iso else ""
            if wanted_date_codes and date_code not in wanted_date_codes:
                continue

            total = sold = gross = 0
            for area in sess.get("areas") or []:
                seats = int(area.get("sTotal") or 0)
                avail = int(area.get("sAvail") or 0)
                price = float(area.get("price") or 0)
                total += seats
                sold += seats - avail
                gross += (seats - avail) * price

            out.append({
                "movie": movie_name,
                "district_movie_id": district_movie_id,
                "language": primary_language,
                "runtime_minutes": runtime_minutes,
                "venue": venue_name,
                "district_venue_id": district_venue_id,
                "chain": chain,
                "client_id": cinema.get("clientId", ""),
                "city": city,
                "state": state,
                "time": _format_time(show_time_iso),
                "audi": sess.get("audi") or "",
                "session_id": str(sess.get("sid", "")),
                "totalSeats": total,
                "sold": sold,
                "available": total - sold,
                "gross": round(gross, 2),
                "date": date_code,
                "source": "District",
            })
    return out


def dedupe_rows(rows: list[dict]) -> list[dict]:
    """Deduplicate by (venue, time, session_id, audi) — same key shape as
    BMS's parser.dedupe_rows."""
    seen, out = set(), []
    for r in rows:
        key = (r["venue"], r["time"], r["session_id"], r["audi"])
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out
