"""Daily shard scraper with cutoff window."""

import json
import os
import sys
from datetime import datetime

from backend.scrapers.sharded.paths import (
    daily_date_code,
    detailed_path,
    log_path,
    shard_id,
    venues_path,
    IST,
)
from backend.scrapers.sharded.runner import make_logger, save_detailed, scrape_shard

CUTOFF_MINUTES = int(os.environ.get("DAILY_CUTOFF_MINUTES", "200"))


def minutes_left(show_time_str: str) -> float:
    try:
        now = datetime.now(IST)
        t = datetime.strptime(show_time_str, "%I:%M %p")
        t = t.replace(year=now.year, month=now.month, day=now.day, tzinfo=IST)
        return (t - now).total_seconds() / 60
    except Exception:
        return 9999.0


def daily_row_filter(rows: list[dict], vcode: str, meta: dict) -> list[dict]:
    kept = []
    for r in rows:
        mins = minutes_left(r.get("time", ""))
        if mins <= CUTOFF_MINUTES:
            row = dict(r)
            row["minsLeft"] = round(mins, 1)
            row["city"] = meta.get("City", "Unknown")
            row["state"] = meta.get("State", "Unknown")
            row["source"] = "BMS"
            row["date"] = daily_date_code()
            kept.append(row)
    return kept


def main() -> int:
    sid = shard_id()
    date_code = daily_date_code()
    vpath = venues_path(sid)
    lf = log_path("daily", date_code, sid)

    log = make_logger(lf)
    log(f"DAILY SHARD {sid} STARTED | date={date_code} | cutoff={CUTOFF_MINUTES}m")

    with open(vpath, encoding="utf-8") as f:
        venues = json.load(f)

    detailed = scrape_shard(
        venues,
        date_code,
        lf,
        row_filter=daily_row_filter,
    )

    out_path = detailed_path("daily", date_code, sid)
    save_detailed(out_path, detailed)
    log(f"DONE | Shows={len(detailed)} | wrote {out_path}")
    print(out_path, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
