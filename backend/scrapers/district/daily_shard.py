"""Daily District shard scraper. Mirrors the shape of
backend/scrapers/sharded/daily_shard.py (BMS) but shards by movie rather than
venue, since a single District page fetch is (movie, city) -> every cinema
and every showtime in that city at once, not (venue, date) -> one venue's
full day.

Flow per movie assigned to this shard:
  1. Fetch the movie against one of its SEO-linked cities to learn its
     language and confirm it's real (movies can vanish from the listing
     between discovery and scrape).
  2. Expand to the full target city set for that language (discovery.py).
  3. Fetch every remaining target city, parse, collect rows for today's date.
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from backend.scrapers.district import client, discovery
from backend.scrapers.district.parser import dedupe_rows, parse_payload

IST = timezone(timedelta(hours=5, minutes=30))
SHARD_COUNT = int(os.environ.get("DISTRICT_SHARD_COUNT", "8"))


def shard_id() -> int:
    return int(os.environ.get("SHARD_ID", "1"))


def daily_date_code() -> str:
    return datetime.now(IST).strftime("%Y%m%d")


def output_path(mode: str, date_code: str, sid: int) -> str:
    base = os.environ.get(
        "DISTRICT_SHARD_OUTPUT_DIR",
        os.path.join("/tmp/district_shard_out", mode, date_code),
    )
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, f"detailed{sid}.json")


def _my_movies(movies: dict[str, set[str]], sid: int, shard_count: int) -> dict[str, set[str]]:
    ids = sorted(movies.keys(), key=int)
    mine = ids[sid - 1 :: shard_count]
    return {mid: movies[mid] for mid in mine}


def scrape_movie(mid: str, seo_cities: set[str], city_catalog: dict, date_codes: set[str]) -> list[dict]:
    rows: list[dict] = []
    cities_left = set(seo_cities) or {"delhi-ncr"}  # every movie gets at least one probe city
    probe_city = next(iter(cities_left))

    raw = client.fetch_with_retry(mid, probe_city)
    if raw is None:
        return rows
    rows.extend(parse_payload(raw, date_codes))

    language = rows[0]["language"] if rows else ""

    targets = discovery.target_cities_for_movie(seo_cities, language, city_catalog)
    targets.discard(probe_city)

    for city in targets:
        raw = client.fetch_with_retry(mid, city)
        if raw is None:
            continue
        rows.extend(parse_payload(raw, date_codes))
        time.sleep(0.3)

    return rows


def main() -> int:
    sid = shard_id()
    date_code = daily_date_code()

    listing_html = client.fetch_movies_listing_html()
    all_movies = discovery.discover_movies(listing_html)
    my_movies = _my_movies(all_movies, sid, SHARD_COUNT)
    city_catalog = discovery.load_city_catalog()

    print(
        f"DISTRICT DAILY SHARD {sid} | {len(my_movies)}/{len(all_movies)} movies | date={date_code}",
        flush=True,
    )

    all_rows: list[dict] = []
    for i, (mid, seo_cities) in enumerate(my_movies.items(), 1):
        print(f"[{i}/{len(my_movies)}] movie {mid} ({len(seo_cities)} seo cities)", flush=True)
        try:
            rows = scrape_movie(mid, seo_cities, city_catalog, {date_code})
            all_rows.extend(rows)
        except Exception as e:
            print(f"FAIL movie {mid} | {type(e).__name__}: {e}", flush=True)

    deduped = dedupe_rows(all_rows)
    out_path = output_path("daily", date_code, sid)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(deduped, f, ensure_ascii=False)

    print(f"DONE | rows={len(deduped)} | wrote {out_path}", flush=True)
    print(out_path, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
