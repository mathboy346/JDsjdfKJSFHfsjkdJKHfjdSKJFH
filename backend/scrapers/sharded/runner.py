"""Shared shard scrape loop.

Important: in CI we must keep stdout clean so workflows can capture the final
output path reliably.
"""

import json
import os
import random
import time
from datetime import datetime
from typing import Callable

from backend.scrapers.parser import dedupe_rows, parse_payload
from backend.scrapers.sharded import client
from backend.scrapers.sharded.paths import IST, MAX_RECOVERY_ROUNDS


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


def scrape_shard(
    venues: dict,
    date_code: str,
    log_file: str,
    row_filter: Callable[[list[dict], str, dict], list[dict]] | None = None,
) -> list[dict]:
    """
    Scrape all venues in one shard dict (keyed by venue code).
    row_filter receives (rows, vcode, venue_meta) and returns rows to keep.
    """
    log = make_logger(log_file)
    client.set_log_fn(log)

    all_rows: list[dict] = []
    retry: set[str] = set()

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
        log(f"[{i}/{len(venues)}] {vcode}")
        try:
            process_venue(vcode)
        except Exception as e:
            retry.add(vcode)
            client.reset_identity()
            log(f"FAIL {vcode} | {type(e).__name__}")
        time.sleep(random.uniform(0.35, 0.7))

    for attempt in range(1, MAX_RECOVERY_ROUNDS + 1):
        if not retry:
            break

        log(f"RETRY ROUND {attempt} | Remaining: {len(retry)}")
        current_retry = list(retry)
        retry.clear()

        for vcode in current_retry:
            log(f"[RETRY-{attempt}] {vcode}")
            try:
                process_venue(vcode)
            except Exception as e:
                retry.add(vcode)
                client.reset_identity()
                log(f"RETRY FAIL {vcode} | {type(e).__name__}")
            time.sleep(random.uniform(0.4, 0.8))

    if retry:
        log(f"FINAL FAILED VENUES: {len(retry)}")

    log("Deduping shows")
    return dedupe_rows(all_rows)


def save_detailed(path: str, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)
