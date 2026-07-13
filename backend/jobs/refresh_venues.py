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
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

from sqlalchemy import select, func

from backend.scrapers.venue_catalog import fetch_city_catalog, extract_venues, reset_identity_on_failure
from backend.scrapers.sharded.paths import SHARD_COUNT, MAX_RECOVERY_ROUNDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CITIES_FILE = os.path.join(_DATA_DIR, "citiesbms.json")
# This job runs on a single GH Actions runner (one IP), unlike the main byvenue
# scraper which spreads across 8 separate runners — keep concurrency modest so
# one runner's request burst doesn't trip rate-limiting on its own.
CATALOG_WORKERS = int(os.environ.get("VENUE_REFRESH_CONCURRENCY", "5"))
PRUNE_AFTER_DAYS = int(os.environ.get("VENUE_PRUNE_AFTER_DAYS", "21"))
# Backoff before each retry round: 15s, 30s, 60s, 120s, 240s (capped).
RETRY_BACKOFF_BASE_SECONDS = 15
RETRY_BACKOFF_CAP_SECONDS = 240


def _venue_path(sid: int) -> str:
    return os.path.join(_DATA_DIR, f"v{sid}.json")


def load_existing_venues() -> dict[str, dict]:
    """Merge all 8 shard files into one {venue_code: record} map."""
    merged: dict[str, dict] = {}
    for sid in range(1, SHARD_COUNT + 1):
        with open(_venue_path(sid), encoding="utf-8") as f:
            merged.update(json.load(f))
    return merged


def _fetch_city_sync(city: dict) -> tuple[dict[str, dict], bool]:
    """Returns (discovered_venues, success). A city with no RegionSlug isn't a
    failure — there's nothing to fetch — so it's reported as success with no
    venues, and never gets retried."""
    slug = city.get("RegionSlug", "")
    if not slug:
        return {}, True
    try:
        data = fetch_city_catalog(slug)
    except Exception as e:
        logger.warning("Fetch failed for %s: %s: %s", slug, type(e).__name__, e)
        reset_identity_on_failure()
        return {}, False
    return extract_venues(data, city.get("RegionName", ""), city.get("StateName", "")), True


def crawl_all_cities(cities: list[dict]) -> dict[str, dict]:
    """Crawl every city's catalog, retrying failures across up to
    MAX_RECOVERY_ROUNDS rounds with exponential backoff between rounds — the
    same retry-round shape the main byvenue scraper uses (runner.py), plus an
    actual backoff delay before each round, since this job runs concurrent
    requests from a single runner IP rather than the main scraper's one-request-
    at-a-time-per-shard pattern, so it needs to actively back off to give any
    rate-limit window time to clear rather than just immediately retrying."""
    discovered: dict[str, dict] = {}
    remaining = list(cities)
    lock = threading.Lock()

    for round_num in range(MAX_RECOVERY_ROUNDS + 1):
        if not remaining:
            break

        if round_num > 0:
            backoff = min(
                RETRY_BACKOFF_BASE_SECONDS * (2 ** (round_num - 1)),
                RETRY_BACKOFF_CAP_SECONDS,
            )
            logger.info(
                "Retry round %d/%d: backing off %ds before retrying %d cities...",
                round_num, MAX_RECOVERY_ROUNDS, backoff, len(remaining),
            )
            time.sleep(backoff)

        failed: list[dict] = []

        def worker(city: dict) -> None:
            venues, ok = _fetch_city_sync(city)
            # Jittered delay per request, same spirit as runner.py's inter-request
            # sleep — spreads this runner's request rate out instead of bursting.
            time.sleep(random.uniform(0.3, 0.7))
            with lock:
                if ok:
                    discovered.update(venues)
                else:
                    failed.append(city)

        label = "initial pass" if round_num == 0 else f"retry round {round_num}"
        logger.info(
            "Crawling %d cities (%s, concurrency=%d)...",
            len(remaining), label, CATALOG_WORKERS,
        )
        with ThreadPoolExecutor(max_workers=CATALOG_WORKERS) as pool:
            list(pool.map(worker, remaining))

        logger.info("%d/%d cities failed this round", len(failed), len(remaining))
        remaining = failed

    if remaining:
        logger.warning(
            "Giving up on %d cities after %d retry rounds (transient network/anti-bot "
            "issue — these just won't contribute new venues this run, existing tracked "
            "venues for these cities are untouched): %s",
            len(remaining), MAX_RECOVERY_ROUNDS,
            [c.get("RegionSlug") for c in remaining],
        )

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

    # Grace period: a venue added within the last PRUNE_AFTER_DAYS days hasn't
    # necessarily been scraped by the byvenue scraper even once yet (it only
    # started being visited once THIS run's updated venue list ships), so it
    # has no bms_show_log history to prove itself with — without this, a
    # newly-added venue that a later run's (variance-prone) catalog crawl
    # simply doesn't happen to re-discover would look identical to a
    # genuinely dead one and get pruned before it ever had a fair chance.
    # Confirmed happening in practice: 485 venues added in one run, several
    # pruned again in the very next one purely because that run's crawl
    # didn't re-find them, despite being real, current listings.
    today_iso = date.today().isoformat()
    cutoff_iso = (date.today() - timedelta(days=PRUNE_AFTER_DAYS)).isoformat()

    def _in_grace_period(code: str) -> bool:
        first_tracked = existing[code].get("FirstTracked")
        return bool(first_tracked) and first_tracked >= cutoff_iso

    # Only consider pruning venues BMS's own catalog no longer lists — then require
    # they've also produced zero scraped shows recently before actually dropping
    # them, so a venue that's merely between releases this week (and so missing
    # from this week's "what's showing" catalog crawl) never gets removed just for
    # that; it only goes if it's ALSO been silent for weeks in our own scrape data.
    since = date.today() - timedelta(days=PRUNE_AFTER_DAYS)
    prune_candidates = {c for c in absent_codes if not _in_grace_period(c)}
    absent_names = {existing[c]["VenueName"] for c in prune_candidates if existing[c].get("VenueName")}
    active_names = await _venues_with_recent_shows(absent_names, since)
    prune_codes = {
        c for c in prune_candidates
        if existing[c].get("VenueName") not in active_names
    }
    protected_new = len(absent_codes) - len(prune_candidates)

    logger.info(
        "New venues to add: %d | Absent from catalog: %d (%d in grace period, protected) | "
        "Pruning (silent %d+ days too): %d",
        len(new_codes), len(absent_codes), protected_new, PRUNE_AFTER_DAYS, len(prune_codes),
    )

    final = dict(existing)
    for c in prune_codes:
        del final[c]
    for c in new_codes:
        final[c] = {**discovered[c], "FirstTracked": today_iso}

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
