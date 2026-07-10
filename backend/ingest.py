from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.processors.nett import estimate_nett_cr
from backend.processors.normalizer import (
    parse_variant_key, normalize_for_matching, slugify, classify_language
)

IST = timezone(timedelta(hours=5, minutes=30))
INGEST_ADVISORY_LOCK_ID = 7320451
INGEST_BATCH_SIZE = 1000


def _dedupe_by_key(records: list[dict], key_fields: list[str]) -> list[dict]:
    """
    PostgreSQL rejects INSERT … ON CONFLICT when the same statement contains
    duplicate conflict keys. Last row wins (matches sequential upsert order).
    """
    by_key: dict[tuple, dict] = {}
    for rec in records:
        key = tuple(rec[field] for field in key_fields)
        by_key[key] = rec
    return list(by_key.values())


async def _batch_upsert(
    db: AsyncSession,
    table,
    records: list[dict],
    index_elements: list[str],
    update_cols: list[str],
) -> None:
    if not records:
        return
    records = _dedupe_by_key(records, index_elements)
    for i in range(0, len(records), INGEST_BATCH_SIZE):
        batch = _dedupe_by_key(
            records[i : i + INGEST_BATCH_SIZE], index_elements
        )
        stmt = insert(table).values(batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=index_elements,
            set_={col: stmt.excluded[col] for col in update_cols},
        )
        await db.execute(stmt)


async def ingest_rows(
    db: AsyncSession, rows: dict, snap_type: str
) -> None:
    """
    Upsert aggregated rows into DB. snap_type: 'advance' | 'daily'
    rows: output of aggregate_rows() — keys are variant_keys
    """
    from backend.models import (
        Movie, MovieVariant, CurrentAdvance, CurrentDaily,
        AdvanceCity, DailyCity, AdvanceChain, DailyChain,
        AdvanceHistory, DailyHistory,
    )

    now = datetime.now(IST)
    date_for = (now + timedelta(days=1)).date() if snap_type == "advance" else now.date()

    await db.execute(
        text("SELECT pg_advisory_xact_lock(:id)"),
        {"id": INGEST_ADVISORY_LOCK_ID},
    )

    movies_by_slug: dict[str, dict] = {}
    variant_records: list[dict] = []
    snap_records: list[dict] = []
    hist_records: list[dict] = []
    city_records: list[dict] = []
    chain_records: list[dict] = []

    snap_model = CurrentAdvance if snap_type == "advance" else CurrentDaily
    hist_model = AdvanceHistory if snap_type == "advance" else DailyHistory
    city_model = AdvanceCity if snap_type == "advance" else DailyCity
    chain_model = AdvanceChain if snap_type == "advance" else DailyChain

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

    # `norm_key` carries its own UNIQUE constraint, separate from the `slug`
    # PK. ON CONFLICT (slug) only suppresses conflicts on the slug index — if
    # two different slugs (within this batch, or one new vs. an existing DB
    # row) claim the same norm_key, the INSERT still raises an uncaught
    # IntegrityError and takes down the whole batch. Drop norm_key for
    # whichever slug doesn't get to keep it.
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
    await _batch_upsert(
        db, snap_model, snap_records, ["variant_key"], snap_update_cols
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
