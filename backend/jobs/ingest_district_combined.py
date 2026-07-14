"""Ingest combined District shard rows into district_show_log.

Granular-only by design (no CurrentDaily/DailyHistory/DailyCity/DailyChain
equivalents) — those tables are BMS-specific aggregates feeding the site's
display layer, and per docs/district-integration-plan.md, District rows
don't feed anything display-facing yet (that's gated behind the Phase 5
seat-inventory question). This job is pure data collection.
"""

import argparse
import asyncio
import json
import logging
import sys
from datetime import date, datetime

from backend.scrapers.district.show_log import ingest_district_show_log

logger = logging.getLogger(__name__)


def _group_rows_by_date(rows: list[dict]) -> dict[date, list[dict]]:
    groups: dict[date, list[dict]] = {}
    for r in rows:
        raw = r.get("date")
        if not raw:
            continue
        show_date = datetime.strptime(raw, "%Y%m%d").date()
        groups.setdefault(show_date, []).append(r)
    return groups


async def ingest_from_rows(rows: list[dict]) -> None:
    if not rows:
        logger.warning("No rows to ingest — skipping")
        return

    groups = _group_rows_by_date(rows)
    if not groups:
        logger.warning("No rows had a parseable `date` field — skipping")
        return

    for show_date, date_rows in sorted(groups.items()):
        await ingest_district_show_log(date_rows, show_date)


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
