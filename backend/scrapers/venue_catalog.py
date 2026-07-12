"""Discover live BMS venues per city, for refreshing the sharded venue list.

Separate from the byvenue showtimes scrape (parser.py) — this hits BMS's
per-city catalog endpoint instead, which lists every venue currently active in
that city regardless of what's playing there today. Reuses client.py's
per-thread identity rotation (thread_local cloudscraper session, reset on
failure) rather than one shared session for every request — a single session
serving hundreds of concurrent requests is exactly the pattern that gets a
session rate-limited/blocked after the first batch.
"""
import json

from backend.scrapers.sharded.client import get_identity, reset_identity

CATALOG_TIMEOUT = 12


def fetch_city_catalog(city_slug: str) -> dict:
    """Raises RuntimeError/requests exceptions on failure, matching
    client.py's fetch_api_raw contract, so callers can retry."""
    ident = get_identity()
    headers = ident.headers()

    homepage_url = f"https://in.bookmyshow.com/explore/home/{city_slug}"
    home_resp = ident.scraper.get(homepage_url, headers=headers, timeout=CATALOG_TIMEOUT)
    if home_resp.status_code != 200:
        raise RuntimeError(f"Homepage fetch failed: HTTP {home_resp.status_code}")

    catalog_url = "https://in.bookmyshow.com/serv/getData?cmd=QUICKBOOK&type=MT"
    catalog_resp = ident.scraper.get(catalog_url, headers=headers, timeout=CATALOG_TIMEOUT)
    if catalog_resp.status_code != 200:
        raise RuntimeError(f"Catalog fetch failed: HTTP {catalog_resp.status_code}")

    body = catalog_resp.text.strip()
    if not body.startswith("{"):
        raise RuntimeError("Blocked / HTML")

    return json.loads(body)


def extract_venues(data: dict, city_name: str, state_name: str) -> dict[str, dict]:
    """Returns {venue_code: venue_record} in the same PascalCase shape as the
    scraper's venue{N}.json shards (VenueCode/VenueName/City/State/...), so a
    freshly-discovered venue slots in identically to a hand-curated one."""
    try:
        raw = data["cinemas"]["BookMyShow"]["aiVN"]["venues"]
    except (KeyError, TypeError):
        return {}

    out: dict[str, dict] = {}
    for v in raw:
        code = v.get("VenueCode")
        if not code:
            continue
        out[code] = {
            "VenueCode": code,
            "VenueName": v.get("VenueName", ""),
            "VenueAddress": v.get("VenueAddress", ""),
            # The catalog payload's own City/State (when present) reflect the venue
            # itself; fall back to the city we queried under, which is always correct
            # for the region even when the payload omits per-venue City/State.
            "City": v.get("City") or city_name,
            "State": v.get("State") or state_name,
            "RegionCode": v.get("RegionCode", ""),
            "SubRegionCode": v.get("SubRegionCode", ""),
            "Latitude": str(v.get("VenueLatitude", "0")),
            "Longitude": str(v.get("VenueLongitude", "0")),
            "AvailableFormats": v.get("AvailableFormats", ""),
        }
    return out


def reset_identity_on_failure() -> None:
    """Re-exported so refresh_venues.py doesn't need its own import of
    client.py's thread-local reset — a failed request's thread should get a
    fresh cloudscraper session (new TLS handshake, new Cloudflare
    challenge-solve) before its next attempt, not keep hammering with the same
    now-possibly-flagged session."""
    reset_identity()
