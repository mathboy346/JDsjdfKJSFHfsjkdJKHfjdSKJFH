"""Combine District shard detailed JSON files. Mirrors
backend/scrapers/sharded/combine.py's shape for BMS, including the report: a
cycle where District returned little or nothing never fails this step. It is
surfaced as ONE warning annotation + job summary (built from the per-shard
status files), and whatever rows do exist still flow on to ingest."""

import argparse
import glob
import json
import os
import sys

from backend.scrapers.district.daily_shard import SHARD_COUNT
from backend.scrapers.district.parser import dedupe_rows

# Share of failed page fetches above which the cycle is flagged as degraded.
WARN_FAILED_FRACTION = 0.05


def load_json(path: str):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def load_rows(path: str) -> list[dict]:
    data = load_json(path)
    return data if isinstance(data, list) else []


def combine_shards(input_dir: str) -> list[dict]:
    all_rows: list[dict] = []
    for path in sorted(glob.glob(os.path.join(input_dir, "detailed*.json"))):
        data = load_rows(path)
        if data:
            print(f"{os.path.basename(path)} -> {len(data)} rows", flush=True)
            all_rows.extend(data)

    print(f"Raw rows: {len(all_rows)}", flush=True)
    final_rows = dedupe_rows(all_rows)
    print(f"Final detailed rows: {len(final_rows)}", flush=True)
    final_rows.sort(key=lambda x: (x["movie"], x["city"], x["venue"], x["time"]))
    return final_rows


def shard_report(input_dir: str, rows: int) -> dict:
    statuses = []
    for path in glob.glob(os.path.join(input_dir, "status*.json")):
        st = load_json(path)
        if isinstance(st, dict) and "shard" in st:
            statuses.append(st)

    reported = {int(s["shard"]) for s in statuses}
    no_output = [n for n in range(1, SHARD_COUNT + 1) if n not in reported]
    listing_failed = sorted(int(s["shard"]) for s in statuses if not s.get("listing_ok", True))
    first_error = next((s.get("error") for s in statuses if s.get("error")), None)

    fetch = {"ok": 0, "not_found": 0, "failed": 0, "denied": 0}
    for s in statuses:
        for k in fetch:
            fetch[k] += int((s.get("fetch") or {}).get(k, 0))

    return {
        "rows": rows,
        "shards_expected": SHARD_COUNT,
        "shards_reported": len(reported),
        "shards_no_output": no_output,
        "shards_listing_failed": listing_failed,
        "first_error": first_error,
        "fetch": fetch,
    }


def annotate(report: dict) -> None:
    fetch = report["fetch"]
    attempted = fetch["ok"] + fetch["not_found"] + fetch["failed"]
    failed_frac = (fetch["failed"] / attempted) if attempted else 0.0

    line = (
        f"shows={report['rows']} shards_reported={report['shards_reported']}/{report['shards_expected']} "
        f"listing_failed={len(report['shards_listing_failed'])} no_output={len(report['shards_no_output'])} "
        f"page_fetches ok={fetch['ok']} not_found={fetch['not_found']} failed={fetch['failed']} denied={fetch['denied']}"
    )
    print(f"COVERAGE {line}", flush=True)

    problems = []
    if report["shards_listing_failed"]:
        problems.append(
            f"{len(report['shards_listing_failed'])}/{report['shards_expected']} shards could not load "
            f"District's movie listing (first error: {report['first_error']})"
        )
    if report["shards_no_output"]:
        problems.append(f"{len(report['shards_no_output'])} shards produced no output")
    if failed_frac > WARN_FAILED_FRACTION:
        problems.append(
            f"{fetch['failed']}/{attempted} page fetches failed ({fetch['denied']} of them HTTP 403)"
        )
    problem_text = "; ".join(problems)

    if report["rows"] == 0:
        print(f"::warning title=No District sessions scraped::{problem_text or line}", flush=True)
    elif problems:
        print(f"::warning title=District scrape degraded::{problem_text}", flush=True)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("### District scrape\n\n")
            f.write("| shows | shards reported | page fetches ok / not found / failed (403) |\n|---|---|---|\n")
            f.write(
                f"| {report['rows']} | {report['shards_reported']} / {report['shards_expected']} "
                f"| {fetch['ok']} / {fetch['not_found']} / {fetch['failed']} ({fetch['denied']}) |\n"
            )
            for p in problems:
                f.write(f"\n- {p}")
            f.write("\n")


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

    annotate(shard_report(args.input_dir, len(rows)))
    print(f"Wrote {out} ({len(rows)} rows)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
