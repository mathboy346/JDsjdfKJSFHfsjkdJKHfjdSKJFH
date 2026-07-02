"""Advance (T+1) shard scraper."""

import json
import sys

from backend.scrapers.sharded.paths import (
    advance_date_code,
    detailed_path,
    log_path,
    shard_id,
    venues_path,
)
from backend.scrapers.sharded.runner import make_logger, save_detailed, scrape_shard


def main() -> int:
    sid = shard_id()
    date_code = advance_date_code()
    vpath = venues_path(sid)

    log = make_logger(log_path("advance", date_code, sid))
    log(f"ADVANCE SHARD {sid} STARTED | date={date_code}")

    with open(vpath, encoding="utf-8") as f:
        venues = json.load(f)

    detailed = scrape_shard(
        venues,
        date_code,
        log_path("advance", date_code, sid),
    )

    out_path = detailed_path("advance", date_code, sid)
    save_detailed(out_path, detailed)
    log(f"DONE | Shows={len(detailed)} | wrote {out_path}")
    print(out_path, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
