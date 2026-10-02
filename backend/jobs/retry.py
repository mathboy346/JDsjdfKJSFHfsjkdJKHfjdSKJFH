"""Retry helper for ingest writes against the managed Postgres.

Ingest jobs run on a CI runner and talk to the database over the public
internet, so an occasional blip is expected: the DB refusing connections for a
moment (`ConnectionRefusedError`), a slow connect (`TimeoutError`), an idle
connection dropped by the server, a deadlock between overlapping ingests. Every
write here is an idempotent upsert, so retrying is safe; without it any one
blip fails the whole cycle's ingest.
"""

import asyncio
import errno
import logging
from typing import Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Seconds to wait before attempt 2, 3, 4, 5 (about 2 minutes in total, enough to
# ride out a short DB restart or maintenance blip).
BACKOFF_SECONDS = (5, 15, 30, 60)

_TRANSIENT_TYPE_NAMES = {
    "ConnectionDoesNotExistError",
    "ConnectionFailureError",
    "CannotConnectNowError",
    "TooManyConnectionsError",
    "InterfaceError",
}
_TRANSIENT_ERRNOS = {
    errno.ECONNREFUSED,
    errno.ECONNRESET,
    errno.ECONNABORTED,
    errno.ETIMEDOUT,
    errno.EHOSTUNREACH,
    errno.ENETUNREACH,
    errno.EPIPE,
}
_TRANSIENT_MESSAGES = (
    "connection was closed",
    "connection reset",
    "connection does not exist",
    "connection not found",
    "server closed the connection",
    "connect call failed",
    "timed out",
    "timeout",
    "could not connect",
    "temporarily unavailable",
    "the database system is starting up",
    "the database system is shutting down",
)


def _causes(exc: BaseException):
    seen = 0
    while exc is not None and seen < 8:
        yield exc
        exc = exc.__cause__ or exc.__context__  # type: ignore[assignment]
        seen += 1


def is_deadlock(exc: BaseException) -> bool:
    for e in _causes(exc):
        if type(e).__name__ == "DeadlockDetectedError":
            return True
        if "deadlock detected" in str(e).lower():
            return True
    return False


def is_transient(exc: BaseException) -> bool:
    """True for connection-level failures worth retrying (see module docstring)."""
    for e in _causes(exc):
        if isinstance(e, (TimeoutError, ConnectionError)):
            return True
        if isinstance(e, OSError) and e.errno in _TRANSIENT_ERRNOS:
            return True
        if type(e).__name__ in _TRANSIENT_TYPE_NAMES:
            return True
        if getattr(e, "connection_invalidated", False):
            return True
        msg = str(e).lower()
        if any(p in msg for p in _TRANSIENT_MESSAGES):
            return True
    return False


async def with_retries(label: str, make_coro: Callable[[], Awaitable[T]]) -> T:
    """Run `make_coro()` (a zero-arg coroutine factory), retrying deadlocks and
    transient connection failures with backoff. Anything else — and the last
    attempt's failure — is raised unchanged."""
    max_attempts = len(BACKOFF_SECONDS) + 1
    for attempt in range(1, max_attempts + 1):
        try:
            return await make_coro()
        except Exception as exc:
            retryable = is_deadlock(exc) or is_transient(exc)
            if not retryable or attempt == max_attempts:
                raise
            delay = BACKOFF_SECONDS[attempt - 1]
            reason = "deadlock" if is_deadlock(exc) else "transient connection error"
            logger.warning(
                "%s during %s (attempt %d/%d): %s: %s — retrying in %ds",
                reason.capitalize(), label, attempt, max_attempts,
                type(exc).__name__, str(exc)[:160], delay,
            )
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover
