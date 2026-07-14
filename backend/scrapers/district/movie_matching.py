"""Resolve each District row's district_movie_id -> variant_key at ingest
time, so district_show_log rows (and the aggregate tables built from them)
carry the same identity BMS rows do.

Looks up the movie_matches table (built by bh's scripts/match_district.py,
a periodic batch job) for a confirmed BMS match. Movies with no match yet
(brand new, not yet batch-matched, or genuinely District-exclusive) get a
synthesized variant_key: "{title} [District | {language}]" -- "District" as
the format placeholder satisfies normalizer.py:parse_variant_key()'s
two-part [format | language] bracket shape, using the real language we
already know from District's own movie metadata.

Known limitation: if match_district.py later finds a real BMS match for a
movie that was already synthesized here, rows ingested before vs. after
that match land under two different variant_keys rather than merging
automatically -- a reconciliation pass is real future work, not solved here.
"""

import asyncpg

from backend.config import get_settings
from backend.scrapers.show_log import _asyncpg_dsn, _asyncpg_ssl


def synthesize_variant_key(title: str, language: str) -> str:
    lang = (language or "Unknown").strip() or "Unknown"
    return f"{title} [District | {lang}]"


async def resolve_variant_keys(rows: list[dict]) -> list[dict]:
    district_movie_ids = {
        str(r["district_movie_id"]) for r in rows if r.get("district_movie_id")
    }
    if not district_movie_ids:
        for r in rows:
            r["variant_key"] = synthesize_variant_key(r.get("movie", "Unknown"), r.get("language", ""))
        return rows

    settings = get_settings()
    conn = await asyncpg.connect(
        _asyncpg_dsn(settings.database_url), ssl=_asyncpg_ssl(settings.database_url)
    )
    try:
        matched = await conn.fetch(
            """
            SELECT district_movie_id, variant_key FROM movie_matches
            WHERE district_movie_id = ANY($1::varchar[]) AND variant_key IS NOT NULL
            """,
            list(district_movie_ids),
        )
    finally:
        await conn.close()

    matches = {m["district_movie_id"]: m["variant_key"] for m in matched}

    for r in rows:
        mid = str(r.get("district_movie_id") or "")
        r["variant_key"] = matches.get(mid) or synthesize_variant_key(
            r.get("movie", "Unknown"), r.get("language", "")
        )
    return rows
