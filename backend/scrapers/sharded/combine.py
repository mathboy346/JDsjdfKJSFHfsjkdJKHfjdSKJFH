"""Combine shard detailed JSON files."""

import argparse
import json
import os
import sys

from backend.scrapers.parser import dedupe_rows
from backend.scrapers.sharded.paths import SHARD_COUNT, advance_date_code, daily_date_code


def load_json(path: str) -> list[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def normalize_row(r: dict, date_code: str) -> dict:
    r = dict(r)
    r["movie"] = r.get("movie") or "Unknown"
    r["city"] = r.get("city") or "Unknown"
    r["state"] = r.get("state") or "Unknown"
    r["venue"] = r.get("venue") or "Unknown"
    r["address"] = r.get("address") or ""
    r["time"] = r.get("time") or ""
    r["audi"] = r.get("audi") or ""
    r["session_id"] = str(r.get("session_id") or "")
    r["chain"] = r.get("chain") or "Unknown"
    r["source"] = r.get("source") or "BMS"
    r["date"] = r.get("date") or date_code
    r["totalSeats"] = int(r.get("totalSeats") or 0)
    r["available"] = int(r.get("available") or 0)
    r["sold"] = int(r.get("sold") or 0)
    r["gross"] = float(r.get("gross") or 0.0)
    if "minsLeft" in r and r["minsLeft"] is not None:
        r["minsLeft"] = float(r["minsLeft"])
    return r


def combine_shards(input_dir: str, date_code: str) -> list[dict]:
    all_rows: list[dict] = []

    for i in range(1, SHARD_COUNT + 1):
        path = os.path.join(input_dir, f"detailed{i}.json")
        if not os.path.isfile(path):
            alt = os.path.join(input_dir, f"detailed-{i}.json")
            path = alt if os.path.isfile(alt) else path
        data = load_json(path)
        if data:
            print(f"detailed{i}.json -> {len(data)} rows", flush=True)
            all_rows.extend(data)

    print(f"Raw rows: {len(all_rows)}", flush=True)
    all_rows = [normalize_row(r, date_code) for r in all_rows]
    final_rows = dedupe_rows(all_rows)
    print(f"Final detailed rows: {len(final_rows)}", flush=True)

    final_rows.sort(key=lambda x: (x["movie"], x["city"], x["venue"], x["time"]))
    return final_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Combine shard outputs")
    parser.add_argument("--mode", choices=["advance", "daily"], required=True)
    parser.add_argument("--input-dir", required=True)
    parser.add_argument(
        "--output",
        help="Write combined rows JSON (default: stdout path only)",
    )
    args = parser.parse_args()

    date_code = daily_date_code() if args.mode == "daily" else advance_date_code()
    rows = combine_shards(args.input_dir, date_code)

    out = args.output or os.path.join(args.input_dir, "final_rows.json")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    print(f"Wrote {out} ({len(rows)} rows)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
