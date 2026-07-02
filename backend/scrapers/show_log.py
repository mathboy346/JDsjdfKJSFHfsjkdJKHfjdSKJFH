import logging
import os
import time
from datetime import datetime, timedelta, timezone, date

import asyncpg
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.config import get_settings
from backend.scrapers.parser import parse_payload

IST = timezone(timedelta(hours=5, minutes=30))
CUTOFF_MINUTES = int(os.environ.get("DAILY_CUTOFF_MINUTES", "200"))
SHOW_LOG_BATCH_SIZE = int(os.environ.get("SHOW_LOG_BATCH_SIZE", "3000"))
SHOW_LOG_USE_COPY = os.environ.get("SHOW_LOG_USE_COPY", "1").lower() not in (
    "0",
    "false",
    "no",
)

logger = logging.getLogger(__name__)

_SHOW_LOG_COLUMNS = [
    "show_date",
    "session_key",
    "movie",
    "variant_key",
    "venue",
    "chain",
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
    "mins_left",
]


def _asyncpg_dsn(database_url: str) -> str:
    """Normalize SQLAlchemy/Heroku URLs for asyncpg.connect."""
    url = database_url
    if url.startswith("postgresql+asyncpg://"):
        url = "postgresql://" + url[len("postgresql+asyncpg://") :]
    elif url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    return url


def _asyncpg_ssl(database_url: str) -> str | None:
    if "localhost" in database_url or "127.0.0.1" in database_url:
        return None
    return "require"


def _show_log_record(row: dict, show_date: date) -> dict:
    session_key = (
        f"{row['venue']}|{row['time']}|{row['session_id']}|{row['audi']}"
    )
    occ = (row["sold"] / row["totalSeats"] * 100) if row["totalSeats"] else 0.0
    return {
        "show_date": show_date,
        "session_key": session_key,
        "movie": row["movie"],
        "variant_key": row["movie"],
        "venue": row["venue"],
        "chain": row["chain"],
        "city": row.get("city", "Unknown"),
        "state": row.get("state", "Unknown"),
        "time": row["time"],
        "audi": row["audi"],
        "session_id": row["session_id"],
        "total_seats": row["totalSeats"],
        "sold": row["sold"],
        "available": row["available"],
        "gross": row["gross"],
        "occupancy": occ,
        "mins_left": row.get("minsLeft"),
    }


def _show_log_tuples(rows: list[dict], show_date: date) -> list[tuple]:
    """Build deduped COPY tuples keyed by (show_date, session_key)."""
    by_session: dict[str, tuple] = {}
    for row in rows:
        rec = _show_log_record(row, show_date)
        by_session[rec["session_key"]] = tuple(rec[col] for col in _SHOW_LOG_COLUMNS)
    return list(by_session.values())


def parse_daily_payload(data: dict, date_code: str) -> list[dict]:
    """Parse payload, filtering to shows within CUTOFF_MINUTES of now."""
    rows = parse_payload(data, date_code)
    now_ist = datetime.now(IST)
    filtered = []
    for row in rows:
        show_time_str = row.get("time", "")
        try:
            show_dt = datetime.strptime(
                f"{now_ist.strftime('%Y-%m-%d')} {show_time_str}",
                "%Y-%m-%d %I:%M %p",
            ).replace(tzinfo=IST)
            mins_diff = (show_dt - now_ist).total_seconds() / 60
            row["minsLeft"] = round(mins_diff, 1)
            if abs(mins_diff) <= CUTOFF_MINUTES:
                filtered.append(row)
        except ValueError:
            row["minsLeft"] = None
            filtered.append(row)
    return filtered


async def _ingest_show_log_copy(rows: list[dict], show_date: date) -> None:
    """
    Bulk-load show rows via COPY into a temp table, then merge with ON CONFLICT.
    Much faster than row-by-row upserts over WAN (GitHub Actions → Heroku).
    """
    records = _show_log_tuples(rows, show_date)
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
                CREATE TEMP TABLE bms_show_log_staging (
                    show_date date NOT NULL,
                    session_key varchar(600) NOT NULL,
                    movie varchar(400),
                    variant_key varchar(400),
                    venue varchar(300),
                    chain varchar(150),
                    city varchar(100),
                    state varchar(100),
                    time varchar(20),
                    audi varchar(200),
                    session_id varchar(100),
                    total_seats integer,
                    sold integer,
                    available integer,
                    gross double precision,
                    occupancy double precision,
                    mins_left double precision
                ) ON COMMIT DROP
                """
            )
            await conn.copy_records_to_table(
                "bms_show_log_staging",
                records=records,
                columns=_SHOW_LOG_COLUMNS,
            )
            await conn.execute(
                """
                INSERT INTO bms_show_log (
                    show_date, session_key, movie, variant_key, venue, chain,
                    city, state, time, audi, session_id, total_seats, sold,
                    available, gross, occupancy, mins_left
                )
                SELECT
                    show_date, session_key, movie, variant_key, venue, chain,
                    city, state, time, audi, session_id, total_seats, sold,
                    available, gross, occupancy, mins_left
                FROM bms_show_log_staging
                ON CONFLICT (show_date, session_key) DO UPDATE SET
                    total_seats = EXCLUDED.total_seats,
                    sold = EXCLUDED.sold,
                    available = EXCLUDED.available,
                    gross = EXCLUDED.gross,
                    occupancy = EXCLUDED.occupancy,
                    mins_left = EXCLUDED.mins_left,
                    last_updated_at = NOW()
                """
            )
    finally:
        await conn.close()

    logger.info(
        "Show log COPY merge complete: %d rows for %s in %.1fs",
        len(records),
        show_date,
        time.monotonic() - t0,
    )


async def _ingest_show_log_batched(
    db: AsyncSession, rows: list[dict], show_date: date
) -> None:
    """Fallback: batched INSERT … ON CONFLICT (slower over high-latency links)."""
    from backend.models import BmsShowLog

    t0 = time.monotonic()
    n_batches = (len(rows) + SHOW_LOG_BATCH_SIZE - 1) // SHOW_LOG_BATCH_SIZE
    for i in range(0, len(rows), SHOW_LOG_BATCH_SIZE):
        batch = rows[i : i + SHOW_LOG_BATCH_SIZE]
        records = [_show_log_record(r, show_date) for r in batch]
        stmt = pg_insert(BmsShowLog).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["show_date", "session_key"],
            set_={
                "total_seats": stmt.excluded.total_seats,
                "sold": stmt.excluded.sold,
                "available": stmt.excluded.available,
                "gross": stmt.excluded.gross,
                "occupancy": stmt.excluded.occupancy,
                "mins_left": stmt.excluded.mins_left,
                "last_updated_at": func.now(),
            },
        )
        await db.execute(stmt)
        batch_no = i // SHOW_LOG_BATCH_SIZE + 1
        if batch_no == 1 or batch_no == n_batches or batch_no % 5 == 0:
            logger.info(
                "Show log batch %d/%d (%d rows)",
                batch_no,
                n_batches,
                len(rows),
            )

    await db.commit()
    logger.info(
        "Show log batched upsert complete: %d rows for %s in %.1fs",
        len(rows),
        show_date,
        time.monotonic() - t0,
    )


async def ingest_show_log(
    db: AsyncSession | None, rows: list[dict], show_date: date
) -> None:
    """Upsert all show rows into bms_show_log (COPY path by default)."""
    if not rows:
        return

    if SHOW_LOG_USE_COPY:
        await _ingest_show_log_copy(rows, show_date)
        return

    if db is None:
        raise ValueError("db session required when SHOW_LOG_USE_COPY=0")
    await _ingest_show_log_batched(db, rows, show_date)
