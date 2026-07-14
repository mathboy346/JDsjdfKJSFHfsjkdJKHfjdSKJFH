"""Combine District shard detailed JSON files. Mirrors
backend/scrapers/sharded/combine.py's shape for BMS."""

import argparse
import glob
import json
import os

from backend.scrapers.district.daily_shard import daily_date_code
from backend.scrapers.district.parser import dedupe_rows


def load_json(path: str) -> list[dict]:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def combine_shards(input_dir: str) -> list[dict]:
    all_rows: list[dict] = []
    for path in sorted(glob.glob(os.path.join(input_dir, "detailed*.json"))):
        data = load_json(path)
        if data:
            print(f"{os.path.basename(path)} -> {len(data)} rows", flush=True)
            all_rows.extend(data)

    print(f"Raw rows: {len(all_rows)}", flush=True)
    final_rows = dedupe_rows(all_rows)
    print(f"Final detailed rows: {len(final_rows)}", flush=True)
    final_rows.sort(key=lambda x: (x["movie"], x["city"], x["venue"], x["time"]))
    return final_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Combine District shard outputs")
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()

    rows = combine_shards(args.input_dir)
    out = args.output or os.path.join(args.input_dir, "final_rows.json")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False)

    print(f"Wrote {out} ({len(rows)} rows)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
