"""HTTP client for District's showtimes data.

District's per-movie showtimes page is a plain, unauthenticated public
webpage (server-rendered with the full dataset embedded in a
`__NEXT_DATA__` script tag) — no auth, no JS challenge.

Both direct-from-runner and Worker-proxied fetches have each independently
been observed getting blocked (403) by District at different times — direct
GH Actions runner IPs in mid-July, then the district-proxy Worker itself in
early August, likely because Cloudflare Workers are a common enough scraping
vector that District (or whatever's in front of it) started blocking that
traffic class specifically. Neither path has proven durably reliable on its
own, so every fetch tries direct first and falls back to the Worker
(district_worker/, bh repo) only on a 403 specifically — never on a 404 or
other error, so a movie that's genuinely not showing in a city doesn't
trigger a pointless extra round-trip. The fallback is a no-op if
DISTRICT_WORKER_URL/DISTRICT_WORKER_KEY aren't set.
"""

import os
import re
import time

import requests

API_TIMEOUT = 15
WORKER_URL = os.environ.get("DISTRICT_WORKER_URL", "")
WORKER_KEY = os.environ.get("DISTRICT_WORKER_KEY", "")

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
}

_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">([\s\S]*?)</script>'
)


class NotFoundError(Exception):
    """The movie/city combination doesn't exist on District (404)."""


def _worker_configured() -> bool:
    return bool(WORKER_URL and WORKER_KEY)


def _via_worker(params: dict) -> requests.Response:
    return requests.get(
        WORKER_URL,
        params=params,
        headers={"x-worker-key": WORKER_KEY},
        timeout=API_TIMEOUT,
    )


def fetch_movie_sessions_raw(movie_id: str, city_slug: str, from_date: str | None = None) -> dict:
    """Fetch the __NEXT_DATA__ payload for a (movie, city) pair. Tries
    district.in directly first, falling back to the district-proxy Worker
    only if that gets a 403 (see module docstring). Returns the parsed JSON
    blob (same shape as window.__NEXT_DATA__ in the browser).

    `from_date` (YYYY-MM-DD): a page fetch only ever returns ONE day's
    sessions (whatever `selectedShowDate` defaults to — today), even though
    the page's own metadata lists several available `sessionDates`. Getting
    a different date's sessions requires this explicit param (discovered by
    inspecting the site's own date-tab links, which carry
    `?fromdate=YYYY-MM-DD`) — one full extra fetch per date, not something
    that comes back for free in a single request."""
    # The page's own slug text is ignored by District's router — only the
    # "-in-{city}-MV{id}" suffix is actually resolved.
    url = f"https://www.district.in/movies/x-movie-tickets-in-{city_slug}-MV{movie_id}"
    direct_params = {"fromdate": from_date} if from_date else None
    resp = requests.get(url, params=direct_params, headers=_BROWSER_HEADERS, timeout=API_TIMEOUT)

    if resp.status_code == 403 and _worker_configured():
        direct_status = resp.status_code
        worker_params = {"movie_id": movie_id, "city": city_slug}
        if from_date:
            worker_params["from_date"] = from_date
        resp = _via_worker(worker_params)
        if resp.status_code == 404:
            raise NotFoundError(f"{movie_id}/{city_slug} not found on District")
        if resp.status_code != 200:
            raise RuntimeError(
                f"District blocked the direct fetch ({direct_status}) and the "
                f"Worker fallback also failed ({resp.status_code}): {resp.text[:200]}"
            )
        return resp.json()

    if resp.status_code == 404:
        raise NotFoundError(f"{movie_id}/{city_slug} not found on District")
    if resp.status_code != 200:
        raise RuntimeError(f"District returned {resp.status_code}")

    match = _NEXT_DATA_RE.search(resp.text)
    if not match:
        raise RuntimeError("No __NEXT_DATA__ found — page shape may have changed")
    import json

    return json.loads(match.group(1))


def fetch_movies_listing_html() -> str:
    """Fetch the general /movies/ listing page — used to discover currently
    showing/upcoming movie IDs and a sample of cities each is linked for.
    Same direct-first, Worker-on-403-fallback pattern as
    fetch_movie_sessions_raw."""
    resp = requests.get(
        "https://www.district.in/movies/", headers=_BROWSER_HEADERS, timeout=API_TIMEOUT
    )
    if resp.status_code == 403 and _worker_configured():
        resp = _via_worker({"mode": "discover"})
    resp.raise_for_status()
    return resp.text


def fetch_with_retry(
    movie_id: str, city_slug: str, retries: int = 2, from_date: str | None = None
) -> dict | None:
    """Best-effort fetch — returns None (not an exception) on persistent
    failure, since a single bad (movie, city) pair shouldn't stop the shard.
    A 404 short-circuits immediately (no session data to retry for; the
    movie just isn't running in that city)."""
    for attempt in range(retries + 1):
        try:
            return fetch_movie_sessions_raw(movie_id, city_slug, from_date=from_date)
        except NotFoundError:
            return None
        except Exception:
            if attempt == retries:
                return None
            time.sleep(1.5 * (attempt + 1))
    return None
