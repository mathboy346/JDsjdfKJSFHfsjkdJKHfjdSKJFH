"""Combine per-shard discovery results from recover_ap_telangana.yml, merge
any genuinely new venue codes into the tracked venue{N}.json shards (stamped
with FirstTracked so they get the same grace period before being pruning-
eligible), and sync bh's venues table.

Purely additive — no pruning here. Pruning stays the regular daily
refresh_venues.py job's job, gated by its own grace-period + 21-day-silence
safety checks, so this recovery run can't itself remove anything.
"""
import asyncio
import glob
import json
import sys
from datetime import date

from backend.jobs.refresh_venues import load_existing_venues, reshard_and_write, sync_venue_table


def main() -> int:
    existing = load_existing_venues()
    print(f"Existing tracked venues: {len(existing)}", flush=True)

    discovered: dict[str, dict] = {}
    shard_files = sorted(glob.glob("shard_data/discovered_shard_*.json"))
    print(f"Combining {len(shard_files)} shard result files", flush=True)
    for path in shard_files:
        with open(path, encoding="utf-8") as f:
            shard_venues = json.load(f)
        print(f"  {path}: {len(shard_venues)} venues", flush=True)
        discovered.update(shard_venues)
    print(f"Discovered across all shards (deduped): {len(discovered)}", flush=True)

    new_codes = set(discovered) - set(existing)
    print(f"New venue codes to add: {len(new_codes)}", flush=True)

    today_iso = date.today().isoformat()
    merged = dict(existing)
    for code in new_codes:
        merged[code] = {**discovered[code], "FirstTracked": today_iso}

    reshard_and_write(merged)
    asyncio.run(sync_venue_table(merged))
    print(
        f"Recovery complete: {len(merged)} total (+{len(new_codes)} added, 0 pruned)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
