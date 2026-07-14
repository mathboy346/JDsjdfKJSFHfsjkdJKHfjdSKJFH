"""Bulk-load District rows into district_show_log. Mirrors
backend/scrapers/show_log.py's COPY-based upsert for BMS, targeting the
separate district_show_log table instead (see
docs/district-integration-plan.md section 2 for why it's separate)."""

import os
import time
from datetime import date

import asyncpg

from backend.config import get_settings
from backend.scrapers.show_log import _asyncpg_dsn, _asyncpg_ssl

SHOW_LOG_BATCH_SIZE = int(os.environ.get("SHOW_LOG_BATCH_SIZE", "3000"))

_DISTRICT_SHOW_LOG_COLUMNS = [
    "show_date",
    "session_key",
    "movie",
    "district_movie_id",
    "variant_key",
    "language",
    "runtime_minutes",
    "venue",
    "district_venue_id",
    "chain",
    "client_id",
    "city",
    "state",
    "time",
    "audi",
    "session_id",
    "total_seats",
    "sold",
    "available",
    "gross",
    "occupancy",
]


def _district_show_log_record(row: dict, show_date: date) -> dict:
    session_key = f"{row['venue']}|{row['time']}|{row['session_id']}|{row['audi']}"
    occ = (row["sold"] / row["totalSeats"] * 100) if row["totalSeats"] else 0.0
    return {
        "show_date": show_date,
        "session_key": session_key,
        "movie": row["movie"],
        "district_movie_id": str(row.get("district_movie_id") or ""),
        "variant_key": row.get("variant_key") or "",
        "language": row.get("language") or "",
        "runtime_minutes": row.get("runtime_minutes"),
        "venue": row["venue"],
        "district_venue_id": str(row.get("district_venue_id") or ""),
        "chain": row["chain"],
        "client_id": row.get("client_id") or "",
        "city": row.get("city") or "Unknown",
        "state": row.get("state") or "Unknown",
        "time": row["time"],
        "audi": row["audi"],
        "session_id": row["session_id"],
        "total_seats": row["totalSeats"],
        "sold": row["sold"],
        "available": row["available"],
        "gross": row["gross"],
        "occupancy": occ,
    }


def _district_show_log_tuples(rows: list[dict], show_date: date) -> list[tuple]:
    by_session: dict[str, tuple] = {}
    for row in rows:
        rec = _district_show_log_record(row, show_date)
        by_session[rec["session_key"]] = tuple(rec[col] for col in _DISTRICT_SHOW_LOG_COLUMNS)
    return list(by_session.values())


async def ingest_district_show_log(rows: list[dict], show_date: date) -> None:
    records = _district_show_log_tuples(rows, show_date)
    if not records:
        return

    settings = get_settings()
    dsn = _asyncpg_dsn(settings.database_url)
    ssl = _asyncpg_ssl(settings.database_url)

    t0 = time.monotonic()
    conn = await asyncpg.connect(dsn, ssl=ssl)
    try:
        async with conn.transaction():
            await conn.execute(
                """
                CREATE TEMP TABLE district_show_log_staging (
                    show_date date NOT NULL,
                    session_key varchar(600) NOT NULL,
                    movie varchar(400),
                    district_movie_id varchar(50),
                    variant_key varchar(400),
                    language varchar(50),
                    runtime_minutes integer,
                    venue varchar(300),
                    district_venue_id varchar(50),
                    chain varchar(150),
                    client_id varchar(100),
                    city varchar(100),
                    state varchar(100),
                    time varchar(20),
                    audi varchar(200),
                    session_id varchar(100),
                    total_seats integer,
                    sold integer,
                    available integer,
                    gross double precision,
                    occupancy double precision
                ) ON COMMIT DROP
                """
            )
            await conn.copy_records_to_table(
                "district_show_log_staging",
                records=records,
                columns=_DISTRICT_SHOW_LOG_COLUMNS,
            )
            await conn.execute(
                """
                INSERT INTO district_show_log (
                    show_date, session_key, movie, district_movie_id, variant_key,
                    language, runtime_minutes, venue, district_venue_id, chain,
                    client_id, city, state, time, audi, session_id, total_seats,
                    sold, available, gross, occupancy
                )
                SELECT
                    show_date, session_key, movie, district_movie_id, variant_key,
                    language, runtime_minutes, venue, district_venue_id, chain,
                    client_id, city, state, time, audi, session_id, total_seats,
                    sold, available, gross, occupancy
                FROM district_show_log_staging
                ON CONFLICT (show_date, session_key) DO UPDATE SET
                    variant_key = EXCLUDED.variant_key,
                    total_seats = EXCLUDED.total_seats,
                    sold = EXCLUDED.sold,
                    available = EXCLUDED.available,
                    gross = EXCLUDED.gross,
                    occupancy = EXCLUDED.occupancy,
                    last_updated_at = NOW()
                """
            )
    finally:
        await conn.close()

    print(
        f"District show log COPY merge complete: {len(records)} rows for "
        f"{show_date} in {time.monotonic() - t0:.1f}s",
        flush=True,
    )
