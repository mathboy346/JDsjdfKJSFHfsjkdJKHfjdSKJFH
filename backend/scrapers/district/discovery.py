"""Movie discovery and city targeting for the District scraper.

Movie discovery: the static `districtmovies.json` snapshot inherited from
`references/district_tracking-main/` is stale (doesn't include movies
released after that repo stopped updating), so it can't drive a live daily
scrape by itself. Instead, District's own https://www.district.in/movies/
listing page links every currently showing/upcoming movie directly as
movies/{slug}-MV{id} — confirmed live to include exactly the movies this
whole investigation is about (Evil Dead Burn, Lenin, Gatta Kusthi 2, Idhayam
Murali, Maa Inti Bangaaram), so it's used as the live movie catalog instead.

City targeting: this project started out targeting only the states/languages
known to have real BMS undercounting exposure (see git history for the
LANGUAGE_TO_STATES-based approach that shipped first). Once the goal became
full BMS parity — District as a standalone alternate backend, not just a
gap-filler — every movie is checked against the **entire** city catalog
(~830 cities). This is a real jump in request volume (see
daily_shard.py's concurrency + shard-count handling), but partial coverage
can't stand in for a full backend: a movie playing somewhere outside the
originally-targeted states would simply be invisible in District-only mode
otherwise.
"""

import json
import os
import re

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
CITY_CATALOG_FILE = os.path.join(_DATA_DIR, "district_cities.json")

_MOVIE_LINK_RE = re.compile(r"movies/[a-z0-9-]+-MV(\d+)")
_MOVIE_CITY_LINK_RE = re.compile(r"movies/[a-z0-9-]+-in-([a-z0-9-]+)-MV(\d+)")


def load_city_catalog() -> dict:
    with open(CITY_CATALOG_FILE, encoding="utf-8") as f:
        return json.load(f)


def discover_movies(listing_html: str) -> dict[str, set[str]]:
    """Parse the /movies/ listing page HTML into {movie_id: {city_slug, ...}}.
    Movies with no city-specific link (only a bare .../x-movie-tickets-MV{id})
    still appear, with an empty city set."""
    movies: dict[str, set[str]] = {}
    for mid in _MOVIE_LINK_RE.findall(listing_html):
        movies.setdefault(mid, set())
    for city, mid in _MOVIE_CITY_LINK_RE.findall(listing_html):
        movies.setdefault(mid, set()).add(city)
    return movies


def all_target_cities(city_catalog: dict) -> set[str]:
    """Every known District city — full-coverage targeting, no per-movie
    language/state narrowing."""
    return set(city_catalog["cities"].keys())
