"""Ingest combined shard rows into Postgres. Used by GH Actions combiner job."""

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy.exc import DBAPIError

from backend.database import AsyncSessionLocal
from backend.ingest import ingest_rows
from backend.processors.aggregator import aggregate_rows
from backend.scrapers.show_log import ingest_show_log

IST = timezone(timedelta(hours=5, minutes=30))
logger = logging.getLogger(__name__)
INGEST_MAX_RETRIES = 5


def _is_deadlock(exc: BaseException) -> bool:
    while exc is not None:
        if type(exc).__name__ == "DeadlockDetectedError":
            return True
        if "deadlock detected" in str(exc).lower():
            return True
        exc = exc.__cause__ or exc.__context__  # type: ignore[assignment]
    return False


def _show_date_for_mode(mode: str):
    now = datetime.now(IST)
    if mode == "advance":
        return (now + timedelta(days=1)).date()
    return now.date()


async def _ingest_granular(rows: list[dict], mode: str) -> None:
    show_date = _show_date_for_mode(mode)
    t0 = time.monotonic()
    await ingest_show_log(None, rows, show_date=show_date)
    logger.info(
        "Granular show_log complete for %s (%s) in %.1fs",
        show_date,
        mode,
        time.monotonic() - t0,
    )


async def _ingest_aggregates(summary: dict, mode: str) -> None:
    t0 = time.monotonic()
    for attempt in range(1, INGEST_MAX_RETRIES + 1):
        try:
            async with AsyncSessionLocal() as db:
                await ingest_rows(db, summary, snap_type=mode)
            break
        except DBAPIError as exc:
            if _is_deadlock(exc) and attempt < INGEST_MAX_RETRIES:
                delay = min(2 ** attempt, 30)
                logger.warning(
                    "Deadlock during %s ingest (attempt %d/%d), retrying in %ds",
                    mode,
                    attempt,
                    INGEST_MAX_RETRIES,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            raise

    logger.info(
        "%s aggregate ingest complete in %.1fs",
        mode,
        time.monotonic() - t0,
    )


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

    if granular_only:
        logger.info("Loaded %d rows for granular ingest (%s)", len(rows), mode)
        await _ingest_granular(rows, mode)
        return

    t0 = time.monotonic()
    summary = aggregate_rows(rows)
    logger.info(
        "Aggregated %d movie variants (%s) in %.1fs",
        len(summary),
        mode,
        time.monotonic() - t0,
    )

    if aggregates_only:
        await _ingest_aggregates(summary, mode)
        return

    # Full ingest: granular + aggregates in parallel (separate DB connections).
    full_t0 = time.monotonic()
    await asyncio.gather(
        _ingest_granular(rows, mode),
        _ingest_aggregates(summary, mode),
    )
    logger.info("Full %s ingest finished in %.1fs", mode, time.monotonic() - full_t0)


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
