"""Discover live BMS venues per city, for refreshing the sharded venue list.

Separate from the byvenue showtimes scrape (parser.py/client.py) — this hits
BMS's per-city catalog endpoint instead, which lists every venue currently
active in that city regardless of what's playing there today.
"""
import json

import cloudscraper

from backend.scrapers.sharded.client import Identity

_scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "desktop": True}
)


def fetch_city_catalog(city_slug: str) -> dict | None:
    ident = Identity()
    headers = ident.headers()

    homepage_url = f"https://in.bookmyshow.com/explore/home/{city_slug}"
    try:
        home_resp = _scraper.get(homepage_url, headers=headers, timeout=12)
        if home_resp.status_code != 200:
            return None

        catalog_url = "https://in.bookmyshow.com/serv/getData?cmd=QUICKBOOK&type=MT"
        catalog_resp = _scraper.get(catalog_url, headers=headers, timeout=12)
        if catalog_resp.status_code != 200:
            return None

        body = catalog_resp.text.strip()
        if not body.startswith("{"):
            return None

        return json.loads(body)
    except Exception:
        return None


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
