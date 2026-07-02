"""HTTP client with per-thread identity rotation."""

import base64
import random
import signal
import threading
from typing import Callable

import cloudscraper

API_TIMEOUT = 12
HARD_TIMEOUT = 15
_HOST = base64.b64decode("aW4uYm9va215c2hvdy5jb20=").decode()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

thread_local = threading.local()
_log_fn: Callable[[str], None] | None = None


class HardTimeoutError(Exception):
    pass


def set_log_fn(fn: Callable[[str], None]) -> None:
    global _log_fn
    _log_fn = fn


def _log(msg: str) -> None:
    if _log_fn:
        _log_fn(msg)


def _timeout_handler(signum, frame) -> None:
    raise HardTimeoutError("Hard timeout hit")


def hard_timeout(seconds: int):
    def deco(fn):
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(seconds)
            try:
                return fn(*args, **kwargs)
            finally:
                signal.alarm(0)

        return wrapper

    return deco


class Identity:
    def __init__(self) -> None:
        self.ua = random.choice(USER_AGENTS)
        self.ip = ".".join(str(random.randint(20, 230)) for _ in range(4))
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )

    def headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-IN,en;q=0.9",
            "Origin": f"https://{_HOST}",
            "Referer": f"https://{_HOST}/",
            "X-Forwarded-For": self.ip,
        }


def get_identity() -> Identity:
    if not hasattr(thread_local, "identity"):
        thread_local.identity = Identity()
        _log("New identity created")
    return thread_local.identity


def reset_identity() -> None:
    if hasattr(thread_local, "identity"):
        del thread_local.identity
    _log("Identity reset")


@hard_timeout(HARD_TIMEOUT)
def fetch_api_raw(venue_code: str, date_code: str) -> dict:
    ident = get_identity()
    url = (
        f"https://{_HOST}/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={date_code}"
    )
    r = ident.scraper.get(url, headers=ident.headers(), timeout=API_TIMEOUT)
    if not r.text.strip().startswith("{"):
        raise RuntimeError("Blocked / HTML")
    return r.json()
