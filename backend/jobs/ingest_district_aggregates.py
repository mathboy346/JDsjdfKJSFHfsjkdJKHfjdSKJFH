"""Upsert aggregated District rows into the District aggregate tables.
Mirrors backend/ingest.py:ingest_rows() (BMS) almost exactly -- same
Movie/MovieVariant upsert logic (including norm_key collision handling),
same batching -- just targeting district_current_daily/district_daily_city/
etc. instead of BMS's tables. A separate function rather than a `platform`
param on ingest_rows() itself, so BMS's ingestion path is untouched.

Movie/MovieVariant are the one thing NOT duplicated: they're the shared
identity layer across both platforms by design (a District row's `movie`
key here is already a resolved variant_key -- see movie_matching.py -- so
creating/reusing Movie/MovieVariant rows from it works identically to BMS,
including picking up an existing BMS-created row for any matched movie).
"""

from datetime import date as date_type, datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.ingest import _batch_upsert, INGEST_BATCH_SIZE
from backend.processors.nett import estimate_nett_cr
from backend.processors.normalizer import (
    parse_variant_key, normalize_for_matching, slugify, classify_language
)

IST = timezone(timedelta(hours=5, minutes=30))
# Distinct from BMS's INGEST_ADVISORY_LOCK_ID (7320451) -- District and BMS
# ingestion touch entirely disjoint tables, so there's no reason for one to
# block the other.
DISTRICT_INGEST_ADVISORY_LOCK_ID = 7320452


async def ingest_district_aggregates(
    db: AsyncSession, rows: dict, snap_type: str, date_for: date_type
) -> None:
    """rows: output of aggregate_rows(), keyed by variant_key (resolved via
    movie_matching.py before aggregation, not District's raw movie title)."""
    from backend.models import (
        Movie, MovieVariant,
        DistrictCurrentAdvance, DistrictCurrentDaily,
        DistrictAdvanceCity, DistrictDailyCity,
        DistrictAdvanceChain, DistrictDailyChain,
        DistrictAdvanceHistory, DistrictDailyHistory,
    )

    now = datetime.now(IST)

    await db.execute(
        text("SELECT pg_advisory_xact_lock(:id)"),
        {"id": DISTRICT_INGEST_ADVISORY_LOCK_ID},
    )

    movies_by_slug: dict[str, dict] = {}
    variant_records: list[dict] = []
    snap_records: list[dict] = []
    hist_records: list[dict] = []
    city_records: list[dict] = []
    chain_records: list[dict] = []

    snap_model = DistrictCurrentAdvance if snap_type == "advance" else DistrictCurrentDaily
    hist_model = DistrictAdvanceHistory if snap_type == "advance" else DistrictDailyHistory
    city_model = DistrictAdvanceCity if snap_type == "advance" else DistrictDailyCity
    chain_model = DistrictAdvanceChain if snap_type == "advance" else DistrictDailyChain

    for variant_key, md in sorted(rows.items()):
        parsed = parse_variant_key(variant_key)
        cname = parsed["name"]
        slug_val = slugify(cname)
        norm = normalize_for_matching(cname)
        lang_grp = classify_language(parsed["language"])

        movies_by_slug[slug_val] = {
            "slug": slug_val,
            "canonical_name": cname,
            "norm_key": norm,
            "last_updated": now,
        }
        variant_records.append({
            "variant_key": variant_key,
            "movie_slug": slug_val,
            "language": parsed["language"],
            "lang_group": lang_grp,
            "format": parsed["format"],
        })

        shows = md["shows"]
        gross = md["gross"]
        sold = md["sold"]
        seats = md["totalSeats"]
        occ = md["occupancy"]
        nett = estimate_nett_cr(gross, shows)

        snap_records.append({
            "variant_key": variant_key,
            "date_for": date_for,
            "shows": shows,
            "gross": gross,
            "sold": sold,
            "total_seats": seats,
            "venues": md["venues"],
            "cities": md["cities"],
            "fastfilling": md["fastfilling"],
            "housefull": md["housefull"],
            "occupancy": occ,
            "nett_cr_est": nett,
            "fetched_at": now,
        })
        hist_records.append({
            "variant_key": variant_key,
            "date_for": date_for,
            "snapshot_at": now,
            "shows": shows,
            "gross": gross,
            "sold": sold,
            "total_seats": seats,
            "occupancy": occ,
            "nett_cr_est": nett,
        })

        for city_data in md.get("details", []):
            city_records.append({
                "variant_key": variant_key,
                "date_for": date_for,
                "city": city_data["city"],
                "state": city_data["state"],
                "region": city_data["region"],
                "venues": city_data["venues"],
                "shows": city_data["shows"],
                "gross": city_data["gross"],
                "sold": city_data["sold"],
                "total_seats": city_data["totalSeats"],
                "fastfilling": city_data["fastfilling"],
                "housefull": city_data["housefull"],
                "occupancy": city_data["occupancy"],
            })

        for c in md.get("Chain_details", []):
            chain_records.append({
                "variant_key": variant_key,
                "date_for": date_for,
                "chain": c["chain"],
                "is_pic": c["is_pic"],
                "venues": c["venues"],
                "shows": c["shows"],
                "gross": c["gross"],
                "sold": c["sold"],
                "total_seats": c["totalSeats"],
                "gross_adj": c["gross_adj"],
                "sold_adj": c["sold_adj"],
                "fastfilling": c["fastfilling"],
                "housefull": c["housefull"],
                "occupancy": c["occupancy"],
            })

    # Same norm_key collision handling as ingest.py:ingest_rows() -- a
    # District-only movie could collide with an existing BMS-created (or
    # another District-created) Movie row on norm_key without matching on
    # slug.
    candidate_norms = {rec["norm_key"] for rec in movies_by_slug.values() if rec["norm_key"]}
    existing_owners: dict[str, str] = {}
    if candidate_norms:
        result = await db.execute(
            text("SELECT slug, norm_key FROM movies WHERE norm_key = ANY(:norms)"),
            {"norms": list(candidate_norms)},
        )
        existing_owners = {row.norm_key: row.slug for row in result}

    claimed: dict[str, str] = {}
    for slug_val, rec in movies_by_slug.items():
        norm = rec["norm_key"]
        if not norm:
            continue
        existing_owner = existing_owners.get(norm)
        if existing_owner and existing_owner != slug_val:
            rec["norm_key"] = None
            continue
        if claimed.setdefault(norm, slug_val) != slug_val:
            rec["norm_key"] = None

    movie_list = list(movies_by_slug.values())
    await _batch_upsert(
        db, Movie, movie_list, ["slug"], ["norm_key", "last_updated"]
    )

    for i in range(0, len(variant_records), INGEST_BATCH_SIZE):
        batch = variant_records[i : i + INGEST_BATCH_SIZE]
        await db.execute(
            insert(MovieVariant)
            .values(batch)
            .on_conflict_do_update(
                index_elements=["variant_key"],
                set_={
                    "language": insert(MovieVariant).excluded.language,
                    "lang_group": insert(MovieVariant).excluded.lang_group,
                    "format": insert(MovieVariant).excluded.format,
                },
            )
        )

    snap_update_cols = [
        "date_for", "shows", "gross", "sold", "total_seats", "venues", "cities",
        "fastfilling", "housefull", "occupancy", "nett_cr_est", "fetched_at",
    ]
    snap_index_elements = (
        ["variant_key", "date_for"] if snap_type == "advance" else ["variant_key"]
    )
    await _batch_upsert(
        db, snap_model, snap_records, snap_index_elements, snap_update_cols
    )

    for i in range(0, len(hist_records), INGEST_BATCH_SIZE):
        batch = hist_records[i : i + INGEST_BATCH_SIZE]
        await db.execute(insert(hist_model).values(batch))

    city_update_cols = [
        "state", "region", "venues", "shows", "gross", "sold", "total_seats",
        "fastfilling", "housefull", "occupancy",
    ]
    await _batch_upsert(
        db, city_model, city_records,
        ["variant_key", "date_for", "city"],
        city_update_cols,
    )

    chain_update_cols = [
        "venues", "shows", "gross", "sold", "total_seats", "gross_adj", "sold_adj",
        "fastfilling", "housefull", "occupancy",
    ]
    await _batch_upsert(
        db, chain_model, chain_records,
        ["variant_key", "date_for", "chain"],
        chain_update_cols,
    )

    await db.commit()
