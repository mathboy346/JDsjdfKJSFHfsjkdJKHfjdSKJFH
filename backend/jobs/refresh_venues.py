"""Regenerate the sharded venue{N}.json files from a live BMS city-catalog crawl.

The scraper only ever visits venues listed in backend/data/v{1..8}.json — those
files were a one-time snapshot and had no refresh mechanism, so newly-opened BMS
venues were invisible forever and venues that quietly went dark kept being
retried (and failing) every cycle. This job closes that gap: it crawls BMS's own
per-city catalog for every city in citiesbms.json, adds any venue codes it finds
that aren't already tracked, and drops codes that are both (a) absent from the
fresh crawl and (b) produced zero scraped shows in the last PRUNE_AFTER_DAYS days
— both conditions together, so a venue that's just between releases this week
(temporarily absent from city catalogs, but still scraping fine) is never removed.

Run via .github/workflows/refresh_venues.yml (workflow_dispatch, triggered by the
Heroku scheduler on the bh side, same pattern as pipeline_a/pipeline_b).
"""
import asyncio
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

from sqlalchemy import select, func

from backend.scrapers.venue_catalog import fetch_city_catalog, extract_venues
from backend.scrapers.sharded.paths import SHARD_COUNT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CITIES_FILE = os.path.join(_DATA_DIR, "citiesbms.json")
CATALOG_WORKERS = int(os.environ.get("VENUE_REFRESH_CONCURRENCY", "10"))
PRUNE_AFTER_DAYS = int(os.environ.get("VENUE_PRUNE_AFTER_DAYS", "21"))


def _venue_path(sid: int) -> str:
    return os.path.join(_DATA_DIR, f"v{sid}.json")


def load_existing_venues() -> dict[str, dict]:
    """Merge all 8 shard files into one {venue_code: record} map."""
    merged: dict[str, dict] = {}
    for sid in range(1, SHARD_COUNT + 1):
        with open(_venue_path(sid), encoding="utf-8") as f:
            merged.update(json.load(f))
    return merged


def _fetch_city_sync(city: dict) -> dict[str, dict]:
    slug = city.get("RegionSlug", "")
    if not slug:
        return {}
    data = fetch_city_catalog(slug)
    if not data:
        return {}
    return extract_venues(data, city.get("RegionName", ""), city.get("StateName", ""))


def crawl_all_cities(cities: list[dict]) -> dict[str, dict]:
    discovered: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=CATALOG_WORKERS) as pool:
        for result in pool.map(_fetch_city_sync, cities):
            discovered.update(result)
    return discovered


async def _venues_with_recent_shows(variant_venue_names: set[str], since: date) -> set[str]:
    """Which of these venue NAMES (bms_show_log has no venue_code column — it
    stores the name as scraped) produced at least one show since `since`."""
    if not variant_venue_names:
        return set()

    from backend.database import AsyncSessionLocal
    from backend.models import BmsShowLog

    active: set[str] = set()
    names = list(variant_venue_names)
    batch_size = 500
    async with AsyncSessionLocal() as db:
        for i in range(0, len(names), batch_size):
            batch = names[i : i + batch_size]
            q = (
                select(BmsShowLog.venue)
                .where(
                    BmsShowLog.venue.in_(batch),
                    BmsShowLog.show_date >= since,
                )
                .distinct()
            )
            rows = (await db.execute(q)).all()
            active.update(r[0] for r in rows if r[0])
    return active


async def build_refreshed_venue_map() -> tuple[dict[str, dict], int, int]:
    existing = load_existing_venues()
    logger.info("Existing tracked venues: %d", len(existing))

    with open(CITIES_FILE, encoding="utf-8") as f:
        cities = json.load(f)
    logger.info("Crawling %d city catalogs (concurrency=%d)...", len(cities), CATALOG_WORKERS)
    discovered = crawl_all_cities(cities)
    logger.info("Discovered %d venues across all city catalogs", len(discovered))

    new_codes = set(discovered) - set(existing)
    absent_codes = set(existing) - set(discovered)

    # Only consider pruning venues BMS's own catalog no longer lists — then require
    # they've also produced zero scraped shows recently before actually dropping
    # them, so a venue that's merely between releases this week (and so missing
    # from this week's "what's showing" catalog crawl) never gets removed just for
    # that; it only goes if it's ALSO been silent for weeks in our own scrape data.
    since = date.today() - timedelta(days=PRUNE_AFTER_DAYS)
    absent_names = {existing[c]["VenueName"] for c in absent_codes if existing[c].get("VenueName")}
    active_names = await _venues_with_recent_shows(absent_names, since)
    prune_codes = {
        c for c in absent_codes
        if existing[c].get("VenueName") not in active_names
    }

    logger.info(
        "New venues to add: %d | Absent from catalog: %d | Pruning (silent %d+ days too): %d",
        len(new_codes), len(absent_codes), PRUNE_AFTER_DAYS, len(prune_codes),
    )

    final = dict(existing)
    for c in prune_codes:
        del final[c]
    for c in new_codes:
        final[c] = discovered[c]

    return final, len(new_codes), len(prune_codes)


def reshard_and_write(venues: dict[str, dict]) -> None:
    """Distribute venues round-robin across SHARD_COUNT files, sorted by code for
    a deterministic, reproducible split (venue_code is the only real identity
    anything downstream relies on — which physical shard file a venue lands in is
    arbitrary and never depended on elsewhere)."""
    buckets: list[dict[str, dict]] = [{} for _ in range(SHARD_COUNT)]
    for i, code in enumerate(sorted(venues)):
        buckets[i % SHARD_COUNT][code] = venues[code]

    for sid in range(1, SHARD_COUNT + 1):
        path = _venue_path(sid)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(buckets[sid - 1], f, indent=2, ensure_ascii=False, sort_keys=True)
        logger.info("Wrote %s (%d venues)", path, len(buckets[sid - 1]))


async def sync_venue_table(venues: dict[str, dict]) -> None:
    """Keep the `venues` table (bh's backend/api/system.py reads a live count off
    it) in sync with the same refreshed list, instead of the stale, disconnected
    snapshot a separate bh-side job used to write there."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from backend.database import AsyncSessionLocal
    from backend.models import Venue

    records = [
        {
            "venue_code": code,
            "venue_name": v.get("VenueName", ""),
            "city": v.get("City", ""),
            "state": v.get("State", ""),
            "chain": (v.get("VenueName", "").split(":")[0].strip() if ":" in v.get("VenueName", "") else ""),
            "latitude": float(v.get("Latitude") or 0),
            "longitude": float(v.get("Longitude") or 0),
        }
        for code, v in venues.items()
    ]

    async with AsyncSessionLocal() as db:
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            stmt = pg_insert(Venue).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["venue_code"],
                set_={
                    "venue_name": stmt.excluded.venue_name,
                    "city": stmt.excluded.city,
                    "state": stmt.excluded.state,
                    "chain": stmt.excluded.chain,
                    "latitude": stmt.excluded.latitude,
                    "longitude": stmt.excluded.longitude,
                    "last_updated": func.now(),
                },
            )
            await db.execute(stmt)

        await db.execute(
            Venue.__table__.delete().where(Venue.venue_code.notin_(list(venues.keys())))
        )
        await db.commit()


async def main_async() -> None:
    final, n_added, n_pruned = await build_refreshed_venue_map()
    reshard_and_write(final)
    await sync_venue_table(final)
    logger.info(
        "Venue refresh complete: %d total (+%d added, -%d pruned)",
        len(final), n_added, n_pruned,
    )


def main() -> int:
    try:
        asyncio.run(main_async())
    except Exception:
        logger.exception("Venue refresh failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
