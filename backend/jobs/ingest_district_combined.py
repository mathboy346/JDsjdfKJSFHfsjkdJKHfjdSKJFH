"""Ingest combined District shard rows: granular (district_show_log) and
aggregate (district_current_daily/district_daily_city/etc.) tables.

Unlike BMS (separate pipeline_a.yml/pipeline_b.yml for daily vs. advance,
with an explicit --mode flag), a single District scrape run already covers
today plus T+1/T+2 in one shot (daily_shard.py's ADVANCE_DAYS) -- so mode
here is inferred per date group (today -> "daily", anything later ->
"advance") rather than passed in from the CLI.

Movie matching (district_movie_id -> variant_key) happens once up front,
before grouping by date, since it's the same resolution regardless of which
date a row belongs to -- see movie_matching.py.
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone

from sqlalchemy.exc import DBAPIError

from backend.database import AsyncSessionLocal
from backend.jobs.ingest_district_aggregates import ingest_district_aggregates
from backend.processors.aggregator import aggregate_rows
from backend.scrapers.district.movie_matching import resolve_variant_keys
from backend.scrapers.district.show_log import ingest_district_show_log

logger = logging.getLogger(__name__)
INGEST_MAX_RETRIES = 5
IST = timezone(timedelta(hours=5, minutes=30))


def _is_deadlock(exc: BaseException) -> bool:
    while exc is not None:
        if type(exc).__name__ == "DeadlockDetectedError":
            return True
        if "deadlock detected" in str(exc).lower():
            return True
        exc = exc.__cause__ or exc.__context__  # type: ignore[assignment]
    return False


def _group_rows_by_date(rows: list[dict]) -> dict[date, list[dict]]:
    groups: dict[date, list[dict]] = {}
    for r in rows:
        raw = r.get("date")
        if not raw:
            continue
        show_date = datetime.strptime(raw, "%Y%m%d").date()
        groups.setdefault(show_date, []).append(r)
    return groups


def _mode_for_date(show_date: date) -> str:
    return "daily" if show_date == datetime.now(IST).date() else "advance"


async def _ingest_granular(rows: list[dict], mode: str, show_date: date) -> None:
    t0 = time.monotonic()
    await ingest_district_show_log(rows, show_date=show_date)
    logger.info(
        "Granular district_show_log complete for %s (%s) in %.1fs",
        show_date, mode, time.monotonic() - t0,
    )


async def _ingest_aggregates(summary: dict, mode: str, show_date: date) -> None:
    t0 = time.monotonic()
    for attempt in range(1, INGEST_MAX_RETRIES + 1):
        try:
            async with AsyncSessionLocal() as db:
                await ingest_district_aggregates(db, summary, snap_type=mode, date_for=show_date)
            break
        except DBAPIError as exc:
            if _is_deadlock(exc) and attempt < INGEST_MAX_RETRIES:
                delay = min(2 ** attempt, 30)
                logger.warning(
                    "Deadlock during %s/%s District aggregate ingest (attempt %d/%d), retrying in %ds",
                    mode, show_date, attempt, INGEST_MAX_RETRIES, delay,
                )
                await asyncio.sleep(delay)
                continue
            raise

    logger.info(
        "%s/%s District aggregate ingest complete in %.1fs", mode, show_date, time.monotonic() - t0
    )


async def ingest_from_rows(rows: list[dict]) -> None:
    if not rows:
        logger.warning("No rows to ingest — skipping")
        return

    rows = await resolve_variant_keys(rows)

    groups = _group_rows_by_date(rows)
    if not groups:
        logger.warning("No rows had a parseable `date` field — skipping")
        return

    for show_date, date_rows in sorted(groups.items()):
        mode = _mode_for_date(show_date)
        logger.info("Loaded %d rows for %s (%s)", len(date_rows), mode, show_date)

        t0 = time.monotonic()
        agg_input = [{**r, "movie": r["variant_key"]} for r in date_rows if r.get("variant_key")]
        summary = aggregate_rows(agg_input)
        logger.info(
            "Aggregated %d movie variants (%s/%s) in %.1fs",
            len(summary), mode, show_date, time.monotonic() - t0,
        )

        await asyncio.gather(
            _ingest_granular(date_rows, mode, show_date),
            _ingest_aggregates(summary, mode, show_date),
        )


async def main_async(rows_file: str) -> None:
    with open(rows_file, encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise ValueError(f"Expected list in {rows_file}, got {type(rows).__name__}")
    logger.info("Loaded %d rows from %s", len(rows), rows_file)
    await ingest_from_rows(rows)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s"
    )
    parser = argparse.ArgumentParser(description="Ingest combined District shard rows")
    parser.add_argument("--rows-file", required=True)
    args = parser.parse_args()

    try:
        asyncio.run(main_async(args.rows_file))
    except Exception:
        logger.exception("District ingest failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
