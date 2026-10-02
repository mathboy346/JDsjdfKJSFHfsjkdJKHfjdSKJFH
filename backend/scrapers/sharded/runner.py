"""Shared shard scrape loop.

Important: in CI we must keep stdout clean so workflows can capture the final
output path reliably. Human-readable summaries go to stderr.

Every shard also writes a small status file next to its rows (venues assigned /
missing, request count, whether the time budget was hit) so the combine job can
report how complete a cycle was instead of only seeing which row files exist.
"""

import json
import os
import random
import sys
import time
from datetime import datetime
from typing import Callable

from backend.scrapers.parser import dedupe_rows, parse_payload
from backend.scrapers.sharded import client
from backend.scrapers.sharded.paths import IST, MAX_RECOVERY_ROUNDS, status_path


def make_logger(log_file: str):
    echo_stdout = os.environ.get("SHARD_ECHO_LOGS", "") == "1"

    def log(msg: str) -> None:
        ts = datetime.now(IST).strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        if echo_stdout:
            print(line, flush=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    return log


def _time_budget_s() -> float:
    """Wall-clock budget for one shard run (SHARD_TIME_BUDGET_S, seconds; 0 or
    unset = unlimited). Set a little under the workflow's step timeout so a slow
    shard stops cleanly and still writes the rows it has, rather than being
    killed by the CI timeout and losing everything."""
    try:
        return float(os.environ.get("SHARD_TIME_BUDGET_S", "0"))
    except ValueError:
        return 0.0


def scrape_shard(
    venues: dict,
    date_code: str,
    log_file: str,
    row_filter: Callable[[list[dict], str, dict], list[dict]] | None = None,
) -> dict:
    """
    Scrape all venues in one shard dict (keyed by venue code).
    row_filter receives (rows, vcode, venue_meta) and returns rows to keep.

    Returns {rows, failed, attempts, elapsed_s, budget_hit}: `failed` lists every
    venue that was never scraped successfully (failed all retries, or not reached
    because the time budget ran out).
    """
    log = make_logger(log_file)
    client.set_log_fn(log)

    started = time.monotonic()
    budget = _time_budget_s()

    def out_of_time() -> bool:
        return budget > 0 and (time.monotonic() - started) > budget

    all_rows: list[dict] = []
    retry: set[str] = set()
    done: set[str] = set()
    attempts = 0
    budget_hit = False

    def enrich(rows: list[dict], vcode: str) -> list[dict]:
        meta = venues[vcode]
        out = []
        for r in rows:
            r = dict(r)
            r["city"] = meta.get("City", "Unknown")
            r["state"] = meta.get("State", "Unknown")
            r["source"] = "BMS"
            r["date"] = date_code
            out.append(r)
        return out

    def process_venue(vcode: str) -> None:
        raw = client.fetch_api_raw(vcode, date_code)
        rows = parse_payload(raw, date_code)
        if row_filter:
            rows = row_filter(rows, vcode, venues[vcode])
        else:
            rows = enrich(rows, vcode)
        all_rows.extend(rows)

    for i, vcode in enumerate(venues, 1):
        if out_of_time():
            budget_hit = True
            log(
                f"TIME BUDGET ({int(budget)}s) HIT at venue {i}/{len(venues)} — "
                "stopping; the remaining venues are left for the next cycle"
            )
            break
        log(f"[{i}/{len(venues)}] {vcode}")
        attempts += 1
        try:
            process_venue(vcode)
            done.add(vcode)
        except Exception as e:
            retry.add(vcode)
            client.reset_identity()
            log(f"FAIL {vcode} | {type(e).__name__}: {str(e)[:80]}")
        time.sleep(random.uniform(0.35, 0.7))

    for attempt in range(1, MAX_RECOVERY_ROUNDS + 1):
        if not retry or budget_hit:
            break

        log(f"RETRY ROUND {attempt} | Remaining: {len(retry)}")
        current_retry = list(retry)
        retry.clear()

        for idx, vcode in enumerate(current_retry):
            if out_of_time():
                budget_hit = True
                retry.update(current_retry[idx:])
                log(f"TIME BUDGET ({int(budget)}s) HIT during retry round {attempt} — stopping")
                break
            log(f"[RETRY-{attempt}] {vcode}")
            attempts += 1
            try:
                process_venue(vcode)
                done.add(vcode)
            except Exception as e:
                retry.add(vcode)
                client.reset_identity()
                log(f"RETRY FAIL {vcode} | {type(e).__name__}: {str(e)[:80]}")
            time.sleep(random.uniform(0.4, 0.8))

    failed = [c for c in venues if c not in done]
    if failed:
        log(f"FINAL FAILED VENUES: {len(failed)}")

    log("Deduping shows")
    return {
        "rows": dedupe_rows(all_rows),
        "failed": failed,
        "attempts": attempts,
        "elapsed_s": round(time.monotonic() - started, 1),
        "budget_hit": budget_hit,
    }


def save_detailed(path: str, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def finish_shard(
    mode: str,
    sid: int,
    day_offset: int,
    date_code: str,
    venues: dict,
    result: dict,
    detailed_out: str,
) -> None:
    """Write the shard's rows and status file, and print a one-line summary
    (stderr, so stdout stays reserved for the output path)."""
    rows = result["rows"]
    save_detailed(detailed_out, rows)

    status = {
        "mode": mode,
        "shard": sid,
        "day_offset": day_offset,
        "date": date_code,
        "assigned": len(venues),
        "failed": result["failed"],
        "rows": len(rows),
        "attempts": result["attempts"],
        "elapsed_s": result["elapsed_s"],
        "budget_hit": result["budget_hit"],
    }
    with open(status_path(mode, date_code, sid), "w", encoding="utf-8") as f:
        json.dump(status, f, separators=(",", ":"))

    ok = len(venues) - len(result["failed"])
    print(
        f"SHARD {sid} ({mode}, day+{day_offset}): {ok}/{len(venues)} venues scraped, "
        f"{len(rows)} shows, {len(result['failed'])} missing, {result['attempts']} requests, "
        f"{result['elapsed_s']}s" + (", time budget hit" if result["budget_hit"] else ""),
        file=sys.stderr,
        flush=True,
    )
