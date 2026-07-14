"""HTTP client for District's showtimes data.

District's per-movie showtimes page is a plain, unauthenticated public
webpage (server-rendered with the full dataset embedded in a
`__NEXT_DATA__` script tag) — confirmed by a direct unauthenticated fetch
during design, with no Cloudflare JS challenge or bot-detection page
encountered. That's meaningfully different from BMS, where GitHub Actions'
runner IPs are *confirmed* (via this project's own earlier investigation)
to get degraded responses from Cloudflare that a residential IP doesn't.
District has shown no such symptom yet.

So this defaults to fetching district.in directly — no Worker in the loop.
The district-proxy Worker (district_worker/, bh repo) exists as a fallback:
if a real GH Actions run shows blocking/degradation (mirroring exactly what
happened with BMS), set DISTRICT_WORKER_URL/DISTRICT_WORKER_KEY to route
through it instead, without any other code change. Don't deploy or wire up
the Worker preemptively for a problem that hasn't been observed.
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


def _use_worker() -> bool:
    return bool(WORKER_URL and WORKER_KEY)


def fetch_movie_sessions_raw(movie_id: str, city_slug: str) -> dict:
    """Fetch the __NEXT_DATA__ payload for a (movie, city) pair — directly
    from district.in by default, or via the district-proxy worker if
    configured. Returns the parsed JSON blob (same shape as
    window.__NEXT_DATA__ in the browser)."""
    if _use_worker():
        resp = requests.get(
            WORKER_URL,
            params={"movie_id": movie_id, "city": city_slug},
            headers={"x-worker-key": WORKER_KEY},
            timeout=API_TIMEOUT,
        )
        if resp.status_code == 404:
            raise NotFoundError(f"{movie_id}/{city_slug} not found on District")
        if resp.status_code != 200:
            raise RuntimeError(f"Worker returned {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    # The page's own slug text is ignored by District's router — only the
    # "-in-{city}-MV{id}" suffix is actually resolved.
    url = f"https://www.district.in/movies/x-movie-tickets-in-{city_slug}-MV{movie_id}"
    resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=API_TIMEOUT)
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
    showing/upcoming movie IDs and a sample of cities each is linked for."""
    if _use_worker():
        resp = requests.get(
            WORKER_URL,
            params={"mode": "discover"},
            headers={"x-worker-key": WORKER_KEY},
            timeout=API_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.text

    resp = requests.get(
        "https://www.district.in/movies/", headers=_BROWSER_HEADERS, timeout=API_TIMEOUT
    )
    resp.raise_for_status()
    return resp.text


def fetch_with_retry(movie_id: str, city_slug: str, retries: int = 2) -> dict | None:
    """Best-effort fetch — returns None (not an exception) on persistent
    failure, since a single bad (movie, city) pair shouldn't stop the shard.
    A 404 short-circuits immediately (no session data to retry for; the
    movie just isn't running in that city)."""
    for attempt in range(retries + 1):
        try:
            return fetch_movie_sessions_raw(movie_id, city_slug)
        except NotFoundError:
            return None
        except Exception:
            if attempt == retries:
                return None
            time.sleep(1.5 * (attempt + 1))
    return None
