"""Ingest combined shard rows into Postgres. Used by GH Actions combiner job."""

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import date, datetime

from sqlalchemy.exc import DBAPIError

from backend.database import AsyncSessionLocal
from backend.ingest import ingest_rows
from backend.processors.aggregator import aggregate_rows
from backend.scrapers.show_log import ingest_show_log

logger = logging.getLogger(__name__)
INGEST_MAX_RETRIES = 5
# Keep in sync with LIVE_CUTOFF_HOURS in the bh repo's
# backend/processors/live_cutoff.py (this repo has no API layer, so no shared
# import is possible — the two must be changed together).
LIVE_CUTOFF_MINUTES = 3 * 60


def _cutoff_rows_for_aggregation(rows: list[dict], mode: str) -> list[dict]:
    """Scope rows for the CurrentDaily/DailyHistory/DailyCity/DailyChain snapshot
    aggregate to the same live-cutoff window used by live queries in the bh
    repo (backend/api/live_totals.py) — otherwise these snapshots (and anything
    built from them, like the movie page's hourly gross chart) show a "whole
    day" figure while the live pages show the cutoff-scoped one.

    Uses each row's `minsLeft` (minutes until showtime, computed at scrape time
    by daily_shard.py's minutes_left()) rather than re-deriving "now" here,
    since ingest can run a few minutes after the scrape itself — minsLeft
    reflects the moment each row was actually captured.

    Advance rows (mode="advance") cover T+1/T+2/T+3, all entirely future —
    no cutoff applies there, and those rows don't carry minsLeft at all.
    """
    if mode != "daily":
        return rows
    return [
        r for r in rows
        if r.get("minsLeft") is None or r["minsLeft"] <= LIVE_CUTOFF_MINUTES
    ]


def _is_deadlock(exc: BaseException) -> bool:
    while exc is not None:
        if type(exc).__name__ == "DeadlockDetectedError":
            return True
        if "deadlock detected" in str(exc).lower():
            return True
        exc = exc.__cause__ or exc.__context__  # type: ignore[assignment]
    return False


def _group_rows_by_date(rows: list[dict]) -> dict[date, list[dict]]:
    """Split combined rows by their own `date` field (set at scrape time — see
    runner.py's enrich()/daily_row_filter()) rather than assuming a single
    global date. Daily mode always yields exactly one group (today); advance
    mode now yields up to 3 (T+1/T+2/T+3), since a single pipeline run scrapes
    the whole advance window."""
    groups: dict[date, list[dict]] = {}
    for r in rows:
        raw = r.get("date")
        if not raw:
            continue
        show_date = datetime.strptime(raw, "%Y%m%d").date()
        groups.setdefault(show_date, []).append(r)
    return groups


async def _ingest_granular(rows: list[dict], mode: str, show_date: date) -> None:
    t0 = time.monotonic()
    await ingest_show_log(None, rows, show_date=show_date)
    logger.info(
        "Granular show_log complete for %s (%s) in %.1fs",
        show_date,
        mode,
        time.monotonic() - t0,
    )


async def _ingest_aggregates(summary: dict, mode: str, show_date: date) -> None:
    t0 = time.monotonic()
    for attempt in range(1, INGEST_MAX_RETRIES + 1):
        try:
            async with AsyncSessionLocal() as db:
                await ingest_rows(db, summary, snap_type=mode, date_for=show_date)
            break
        except DBAPIError as exc:
            if _is_deadlock(exc) and attempt < INGEST_MAX_RETRIES:
                delay = min(2 ** attempt, 30)
                logger.warning(
                    "Deadlock during %s/%s ingest (attempt %d/%d), retrying in %ds",
                    mode,
                    show_date,
                    attempt,
                    INGEST_MAX_RETRIES,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            raise

    logger.info(
        "%s/%s aggregate ingest complete in %.1fs",
        mode,
        show_date,
        time.monotonic() - t0,
    )


async def _ingest_one_date(
    show_date: date,
    date_rows: list[dict],
    mode: str,
    *,
    granular_only: bool,
    aggregates_only: bool,
) -> None:
    if granular_only:
        logger.info("Loaded %d rows for granular ingest (%s/%s)", len(date_rows), mode, show_date)
        await _ingest_granular(date_rows, mode, show_date)
        return

    agg_rows = _cutoff_rows_for_aggregation(date_rows, mode)
    t0 = time.monotonic()
    summary = aggregate_rows(agg_rows)
    logger.info(
        "Aggregated %d movie variants (%s/%s) in %.1fs (%d/%d rows in live-cutoff window)",
        len(summary),
        mode,
        show_date,
        time.monotonic() - t0,
        len(agg_rows),
        len(date_rows),
    )

    if aggregates_only:
        await _ingest_aggregates(summary, mode, show_date)
        return

    # Full ingest: granular + aggregates in parallel (separate DB connections).
    full_t0 = time.monotonic()
    await asyncio.gather(
        _ingest_granular(date_rows, mode, show_date),
        _ingest_aggregates(summary, mode, show_date),
    )
    logger.info("Full %s/%s ingest finished in %.1fs", mode, show_date, time.monotonic() - full_t0)


async def ingest_from_rows(
    rows: list[dict],
    mode: str,
    *,
    granular_only: bool = False,
    aggregates_only: bool = False,
) -> None:
    if not rows:
        logger.warning("No rows to ingest — skipping")
        return

    if granular_only and aggregates_only:
        raise ValueError("Cannot set both --granular-only and --aggregates-only")

    groups = _group_rows_by_date(rows)
    if not groups:
        logger.warning("No rows had a parseable `date` field — skipping")
        return

    for show_date in sorted(groups):
        await _ingest_one_date(
            show_date,
            groups[show_date],
            mode,
            granular_only=granular_only,
            aggregates_only=aggregates_only,
        )


async def main_async(
    mode: str,
    rows_file: str,
    *,
    granular_only: bool = False,
    aggregates_only: bool = False,
) -> None:
    with open(rows_file, encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        raise ValueError(f"Expected list in {rows_file}, got {type(rows).__name__}")
    logger.info("Loaded %d rows from %s", len(rows), rows_file)
    await ingest_from_rows(
        rows,
        mode,
        granular_only=granular_only,
        aggregates_only=aggregates_only,
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    parser = argparse.ArgumentParser(description="Ingest combined shard rows")
    parser.add_argument("--mode", choices=["advance", "daily"], required=True)
    parser.add_argument("--rows-file", required=True)
    parser.add_argument(
        "--granular-only",
        action="store_true",
        help="Only ingest bms_show_log (per-show rows)",
    )
    parser.add_argument(
        "--aggregates-only",
        action="store_true",
        help="Only ingest aggregate tables (current/history/city/chain)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(
            main_async(
                args.mode,
                args.rows_file,
                granular_only=args.granular_only,
                aggregates_only=args.aggregates_only,
            )
        )
    except Exception:
        logger.exception("Ingest failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
