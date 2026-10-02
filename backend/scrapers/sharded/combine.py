"""Combine shard detailed JSON files, and report how complete the cycle was.

Missing data never fails this step: a shard that produced no output, only some
of its venues, or nothing at all is reported as a GitHub warning annotation + a
job summary (and in coverage.json) instead of a red build, and the combined
rows that do exist still flow on to ingest.
"""

import argparse
import glob
import json
import os
import sys

from backend.scrapers.parser import dedupe_rows
from backend.scrapers.sharded.paths import (
    SHARD_COUNT,
    advance_date_code,
    daily_date_code,
    venues_path,
)

# Below this share of venues scraped the cycle is flagged as partial.
WARN_COVERAGE = 0.98


def load_json(path: str):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def load_rows(path: str) -> list[dict]:
    data = load_json(path)
    return data if isinstance(data, list) else []


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
    """Merge every detailed*.json shard output found in input_dir. Glob-based
    rather than a fixed shard-count loop since advance runs emit one file per
    (shard, day_offset) pair — the count varies by mode. Scoped to the
    `detailed*` prefix (not a bare `*.json`) so the status files that sit next to
    them, or a stale final_rows.json left over from a prior local run, never get
    ingested as if they were shard input."""
    all_rows: list[dict] = []

    for path in sorted(glob.glob(os.path.join(input_dir, "detailed*.json"))):
        data = load_rows(path)
        if data:
            print(f"{os.path.basename(path)} -> {len(data)} rows", flush=True)
            all_rows.extend(data)

    print(f"Raw rows: {len(all_rows)}", flush=True)
    all_rows = [normalize_row(r, date_code) for r in all_rows]
    final_rows = dedupe_rows(all_rows)
    print(f"Final detailed rows: {len(final_rows)}", flush=True)

    final_rows.sort(key=lambda x: (x["movie"], x["city"], x["venue"], x["time"]))
    return final_rows


def _shard_venue_count(sid: int) -> int:
    data = load_json(venues_path(sid))
    return len(data) if isinstance(data, dict) else 0


def coverage_report(input_dir: str, mode: str, rows: int) -> dict:
    """How much of the expected scrape landed. A (shard, day) unit with no status
    file produced nothing at all (its job failed or timed out before finishing)."""
    offsets = [0] if mode == "daily" else [
        int(x) for x in os.environ.get("ADVANCE_OFFSETS", "1,2,3").split(",") if x.strip()
    ]

    statuses: dict[tuple[int, int], dict] = {}
    for path in glob.glob(os.path.join(input_dir, "status*.json")):
        st = load_json(path)
        if isinstance(st, dict) and "shard" in st:
            statuses[(int(st["shard"]), int(st.get("day_offset", 0)))] = st

    def label(sid: int, off: int) -> str:
        return f"shard {sid}" if mode == "daily" else f"shard {sid} / T+{off}"

    expected = scraped = 0
    no_output: list[str] = []
    partial: list[str] = []
    budget_hit: list[str] = []
    for sid in range(1, SHARD_COUNT + 1):
        n = _shard_venue_count(sid)
        for off in offsets:
            expected += n
            st = statuses.get((sid, off))
            if st is None:
                no_output.append(label(sid, off))
                continue
            assigned = int(st.get("assigned", 0))
            failed = len(st.get("failed", []))
            scraped += max(0, assigned - failed)
            if failed:
                partial.append(f"{label(sid, off)} ({max(0, assigned - failed)}/{assigned})")
            if st.get("budget_hit"):
                budget_hit.append(label(sid, off))

    coverage = (scraped / expected) if expected else 0.0
    return {
        "mode": mode,
        "rows": rows,
        "venues_expected": expected,
        "venues_scraped": scraped,
        "coverage": round(coverage, 4),
        "units_with_no_output": no_output,
        "units_partial": partial,
        "units_time_budget_hit": budget_hit,
    }


def annotate(report: dict) -> None:
    """Print the coverage line, and surface problems as warning annotations
    (visible on the run page) plus a job summary — without failing the step."""
    pct = report["coverage"] * 100
    line = (
        f"mode={report['mode']} shows={report['rows']} coverage={pct:.1f}% "
        f"({report['venues_scraped']}/{report['venues_expected']} venues scraped)"
    )
    print(f"COVERAGE {line}", flush=True)

    details = []
    if report["units_with_no_output"]:
        details.append("no output from: " + ", ".join(report["units_with_no_output"]))
    if report["units_partial"]:
        details.append("partial: " + ", ".join(report["units_partial"]))
    if report["units_time_budget_hit"]:
        details.append("time budget hit: " + ", ".join(report["units_time_budget_hit"]))
    detail_text = "; ".join(details)

    if report["rows"] == 0:
        print(f"::warning title=No shows scraped ({report['mode']})::{line}. {detail_text}", flush=True)
    elif report["coverage"] < WARN_COVERAGE:
        print(f"::warning title=Partial scrape coverage ({report['mode']})::{line}. {detail_text}", flush=True)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write(f"### Scrape coverage — {report['mode']}\n\n")
            f.write("| shows | venues scraped | coverage |\n|---|---|---|\n")
            f.write(
                f"| {report['rows']} | {report['venues_scraped']} / {report['venues_expected']} | {pct:.1f}% |\n"
            )
            for text in details:
                f.write(f"\n- {text}")
            f.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Combine shard outputs")
    parser.add_argument("--mode", choices=["advance", "daily"], required=True)
    parser.add_argument("--input-dir", required=True)
    parser.add_argument(
        "--output",
        help="Write combined rows JSON (default: <input-dir>/final_rows.json)",
    )
    args = parser.parse_args()

    date_code = daily_date_code() if args.mode == "daily" else advance_date_code()
    rows = combine_shards(args.input_dir, date_code)

    out = args.output or os.path.join(args.input_dir, "final_rows.json")
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)

    report = coverage_report(args.input_dir, args.mode, len(rows))
    cov_path = os.path.join(os.path.dirname(out) or ".", "coverage.json")
    with open(cov_path, "w", encoding="utf-8") as f:
        json.dump(report, f)

    annotate(report)
    print(f"Wrote {out} ({len(rows)} rows)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
