"""District shard scraper. Mirrors the shape of
backend/scrapers/sharded/daily_shard.py (BMS) but shards by movie rather than
venue, since a single District page fetch is (movie, city) -> every cinema
and every showtime in that city at once, not (venue, date) -> one venue's
full day.

Full-coverage: every movie assigned to this shard is checked against every
known District city (discovery.py:all_target_cities()), fetched
concurrently within the shard (mirroring refresh_venues.py's
ThreadPoolExecutor pattern on the BMS side) since serial fetching against
~830 cities per movie would blow well past the job timeout.

Advance dates cost a full extra fetch each, not "free" from one request:
a page fetch only ever returns ONE day's sessions (whatever
`selectedShowDate` defaults to — today) even though its own metadata lists
several available `sessionDates`. A different date needs an explicit
`fromdate=YYYY-MM-DD` param (client.py's `from_date` arg) — discovered from
the site's own date-tab links, not something inferred from the first
response. DISTRICT_ADVANCE_DAYS defaults to 0 (today only) — District's job
is plugging gaps in BMS's live dashboard numbers, not Advance-page parity,
so T+1/T+2 aren't worth 3x the request volume/cost. Shard count/timeout in
district_daily.yml are sized with this in
mind; revisit both if real runs show it's not enough.
"""

import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from backend.scrapers.district import client, discovery
from backend.scrapers.district.parser import dedupe_rows, parse_payload

IST = timezone(timedelta(hours=5, minutes=30))
SHARD_COUNT = int(os.environ.get("DISTRICT_SHARD_COUNT", "24"))
CITY_WORKERS = int(os.environ.get("DISTRICT_CITY_WORKERS", "10"))
# Live-day only: District's role is filling BMS gaps on the dashboard, not
# Advance-page parity, so T+1/T+2 aren't worth 3x the request volume/cost.
ADVANCE_DAYS = int(os.environ.get("DISTRICT_ADVANCE_DAYS", "0"))


def shard_id() -> int:
    return int(os.environ.get("SHARD_ID", "1"))


_URL_RE = re.compile(r"https?://\S+")


def redact_urls(text: str) -> str:
    """Strip URLs from error text before it is logged or written to the status
    file. HTTP error messages embed the request URL, and for a request routed
    through the proxy Worker that URL is a repository secret: masked in this
    job's own log, but NOT in the combine job (which reads the status file and
    doesn't have the secret in scope) -- in a public repo that would leak it."""
    return _URL_RE.sub("<url>", text)


def _log(msg: str) -> None:
    """Progress/diagnostics go to stderr: stdout is reserved for the final output
    path (the workflow reads the last stdout line)."""
    print(msg, file=sys.stderr, flush=True)


def daily_date_code() -> str:
    return datetime.now(IST).strftime("%Y%m%d")


def fetch_dates() -> list[str]:
    """Today plus ADVANCE_DAYS more, as YYYY-MM-DD strings for the
    `fromdate` param — one fetch per date, per city, per movie."""
    now = datetime.now(IST)
    return [(now + timedelta(days=n)).strftime("%Y-%m-%d") for n in range(ADVANCE_DAYS + 1)]


def output_path(mode: str, date_code: str, sid: int) -> str:
    base = os.environ.get(
        "DISTRICT_SHARD_OUTPUT_DIR",
        os.path.join("/tmp/district_shard_out", mode, date_code),
    )
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, f"detailed{sid}.json")


def _my_movies(movie_ids: list[str], sid: int, shard_count: int) -> list[str]:
    ids = sorted(movie_ids, key=int)
    return ids[sid - 1 :: shard_count]


def scrape_movie(mid: str, cities: set[str], dates: list[str]) -> list[dict]:
    rows: list[dict] = []
    lock = threading.Lock()
    # today's fetch needs no fromdate param (it's the default); later dates
    # do — see client.py's from_date arg.
    tasks = [(city, date if i > 0 else None) for city in cities for i, date in enumerate(dates)]
    wanted_dates = {d.replace("-", "") for d in dates}

    def worker(task: tuple[str, str | None]) -> None:
        city, from_date = task
        raw = client.fetch_with_retry(mid, city, from_date=from_date)
        time.sleep(random.uniform(0.15, 0.35))
        if raw is None:
            return
        parsed = parse_payload(raw, wanted_dates)
        if parsed:
            with lock:
                rows.extend(parsed)

    with ThreadPoolExecutor(max_workers=CITY_WORKERS) as pool:
        list(pool.map(worker, tasks))

    return rows


def _write_outputs(out_path: str, sid: int, rows: list[dict], status: dict) -> None:
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)
    status_file = os.path.join(os.path.dirname(out_path), f"status{sid}.json")
    with open(status_file, "w", encoding="utf-8") as f:
        json.dump({"shard": sid, "rows": len(rows), **status}, f, separators=(",", ":"))


def main() -> int:
    sid = shard_id()
    dates = fetch_dates()
    date_code = daily_date_code()
    out_path = output_path("daily", date_code, sid)

    try:
        listing_html = client.fetch_movies_listing_html()
    except Exception as e:
        # The listing page is the first request every shard makes. If it can't be
        # loaded (District answering HTTP 403, a network error, ...) this shard has
        # nothing to scrape: finish cleanly with an empty result so the run
        # completes, and let the combine job raise ONE visible warning for the
        # whole run instead of 24 red shard failures.
        reason = f"{type(e).__name__}: {redact_urls(str(e))[:200]}"
        _log(f"DISTRICT SHARD {sid} | movie listing unavailable | {reason}")
        _write_outputs(out_path, sid, [], {"listing_ok": False, "error": reason, "movies": 0, "fetch": dict(client.STATS)})
        print(out_path, flush=True)
        return 0

    all_movie_ids = list(discovery.discover_movies(listing_html).keys())
    my_movie_ids = _my_movies(all_movie_ids, sid, SHARD_COUNT)
    city_catalog = discovery.load_city_catalog()
    cities = discovery.all_target_cities(city_catalog)

    _log(
        f"DISTRICT SHARD {sid} | {len(my_movie_ids)}/{len(all_movie_ids)} movies | "
        f"{len(cities)} cities x {len(dates)} dates each | dates={dates}"
    )

    all_rows: list[dict] = []
    for i, mid in enumerate(my_movie_ids, 1):
        t0 = time.monotonic()
        try:
            rows = scrape_movie(mid, cities, dates)
            all_rows.extend(rows)
            _log(
                f"[{i}/{len(my_movie_ids)}] movie {mid} -> {len(rows)} rows "
                f"in {time.monotonic() - t0:.1f}s"
            )
        except Exception as e:
            _log(f"FAIL movie {mid} | {type(e).__name__}: {redact_urls(str(e))}")

    deduped = dedupe_rows(all_rows)
    stats = dict(client.STATS)
    _write_outputs(
        out_path, sid, deduped,
        {"listing_ok": True, "error": None, "movies": len(my_movie_ids), "fetch": stats},
    )

    _log(f"DONE | rows={len(deduped)} | page fetches {stats} | wrote {out_path}")
    print(out_path, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
