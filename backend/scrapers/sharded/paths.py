"""Shared paths and config for sharded scrapers."""

import os
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
SHARD_COUNT = 8
MAX_RECOVERY_ROUNDS = 5

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def shard_id() -> int:
    return int(os.environ.get("SHARD_ID", "1"))


def venues_path(sid: int | None = None) -> str:
    sid = sid or shard_id()
    return os.path.normpath(os.path.join(_DATA_DIR, f"v{sid}.json"))


def output_dir(mode: str, date_code: str) -> str:
    base = os.environ.get(
        "SHARD_OUTPUT_DIR",
        os.path.join("/tmp/shard_out", mode, date_code),
    )
    os.makedirs(base, exist_ok=True)
    return base


def detailed_path(mode: str, date_code: str, sid: int | None = None) -> str:
    sid = sid or shard_id()
    return os.path.join(output_dir(mode, date_code), f"detailed{sid}.json")


def log_path(mode: str, date_code: str, sid: int | None = None) -> str:
    sid = sid or shard_id()
    log_dir = os.path.join(output_dir(mode, date_code), "logs")
    os.makedirs(log_dir, exist_ok=True)
    prefix = "logd" if mode == "daily" else "loga"
    return os.path.join(log_dir, f"{prefix}{sid}.log")


def advance_day_offset() -> int:
    return int(os.environ.get("ADVANCE_DAY_OFFSET", "1"))


def advance_date_code(day_offset: int | None = None) -> str:
    offset = day_offset if day_offset is not None else advance_day_offset()
    return (datetime.now(IST) + timedelta(days=offset)).strftime("%Y%m%d")


def daily_date_code() -> str:
    return datetime.now(IST).strftime("%Y%m%d")
