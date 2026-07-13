"""One shard of a sharded, region-targeted venue-catalog crawl.

The regular refresh_venues.py crawl runs as a single GH Actions job — one
runner, one IP. When we ran it against the newly-expanded 611-city list, it
found only 24 new venues, versus 572 an identical crawl found running
directly from a residential IP moments earlier (zero failures either way —
both "succeeded", they just got very different amounts of real data back).
That's consistent with BMS/Cloudflare degrading responses for GitHub
Actions' runner IP more readily than a residential one, for this endpoint.

The main byvenue scraper (pipeline_a.yml) avoids exactly this by sharding
across 8 parallel jobs, each its own runner (so each gets a different IP and
sends a fraction of the request volume). This script is that same pattern
applied to a region-targeted catalog recovery: split Andhra Pradesh/
Telangana's city list across SHARD_COUNT parallel jobs instead of crawling
it all from one.

Purely a discovery step — writes discovered venues to a JSON file for the
combine job to merge. No DB access, no pruning here (pruning stays the
regular daily refresh_venues.py job's responsibility, gated by its own
grace-period + 21-day-silence safety checks).
"""
import json
import os
import sys

from backend.jobs.refresh_venues import crawl_all_cities, CITIES_FILE

TARGET_STATES = {"Andhra Pradesh", "Telangana"}


def main() -> int:
    shard_id = int(os.environ.get("SHARD_ID", "1"))
    shard_count = int(os.environ.get("RECOVERY_SHARD_COUNT", "8"))

    with open(CITIES_FILE, encoding="utf-8") as f:
        all_cities = json.load(f)
    region_cities = [c for c in all_cities if c.get("StateName") in TARGET_STATES]
    my_cities = region_cities[shard_id - 1 :: shard_count]

    print(
        f"Shard {shard_id}/{shard_count}: crawling {len(my_cities)} of "
        f"{len(region_cities)} AP/Telangana cities",
        flush=True,
    )
    discovered = crawl_all_cities(my_cities)
    print(f"Shard {shard_id}: discovered {len(discovered)} venues", flush=True)

    out_path = f"discovered_shard_{shard_id}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(discovered, f, ensure_ascii=False)
    print(out_path, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
