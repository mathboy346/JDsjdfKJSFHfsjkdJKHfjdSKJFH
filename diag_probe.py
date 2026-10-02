"""Temporary diagnostic probe: what do GH runner IPs get back from BMS/District
with different HTTP clients? Prints compact one-line results. Not part of the
pipeline; removed after the investigation."""

import base64
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

IST = timezone(timedelta(hours=5, minutes=30))
N = int(os.environ.get("PROBE_N", "1"))
BMS_HOST = base64.b64decode("aW4uYm9va215c2hvdy5jb20=").decode()
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def snip(text: str, n: int = 160) -> str:
    return re.sub(r"\s+", " ", text or "")[:n]


def show_ip() -> None:
    try:
        j = requests.get("https://ipinfo.io/json", timeout=10).json()
        print(f"RUNNER ip={j.get('ip')} org={j.get('org')} city={j.get('city')} region={j.get('region')}", flush=True)
    except Exception as e:
        print("RUNNER ip lookup failed", type(e).__name__, flush=True)


def clients():
    out = {}
    out["requests"] = lambda url, headers, **kw: requests.get(url, headers=headers, timeout=20, **kw)
    try:
        import cloudscraper

        cs = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "desktop": True})
        out["cloudscraper"] = lambda url, headers, **kw: cs.get(url, headers=headers, timeout=20, **kw)
    except Exception as e:
        print("cloudscraper unavailable", e, flush=True)
    try:
        from curl_cffi import requests as cffi

        for imp in ("chrome124", "chrome131", "safari17_0"):
            out[f"cffi-{imp}"] = (lambda imp: lambda url, headers, **kw: cffi.get(url, headers=headers, timeout=20, impersonate=imp, **kw))(imp)
    except Exception as e:
        print("curl_cffi unavailable", e, flush=True)
    return out


def probe_bms(cl: dict) -> None:
    date_code = datetime.now(IST).strftime("%Y%m%d")
    venues = json.load(open(f"backend/data/v{(N % 8) + 1}.json"))
    codes = list(venues)[:8]
    hdr = {
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-IN,en;q=0.9",
        "Origin": f"https://{BMS_HOST}",
        "Referer": f"https://{BMS_HOST}/",
    }
    for name, fn in cl.items():
        res = []
        for code in codes:
            url = f"https://{BMS_HOST}/api/v2/mobile/showtimes/byvenue?venueCode={code}&dateCode={date_code}"
            t = time.time()
            try:
                r = fn(url, hdr)
                body = r.text if hasattr(r, "text") else ""
                ok = body.strip().startswith("{")
                res.append(("OK" if ok else f"HTML{r.status_code}", round(time.time() - t, 1)))
                if not ok and len([x for x in res if x[0] != "OK"]) == 1:
                    srv = r.headers.get("server")
                    print(f"  BMS[{name}] first non-JSON: status={r.status_code} server={srv} cf-ray={r.headers.get('cf-ray')} body={snip(body)!r}", flush=True)
            except Exception as e:
                res.append((type(e).__name__, round(time.time() - t, 1)))
            time.sleep(0.5)
        ok_n = sum(1 for x in res if x[0] == "OK")
        print(f"BMS[{name}] ok={ok_n}/{len(res)} detail={res}", flush=True)


def probe_district(cl: dict) -> None:
    hdr = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
    }
    for name, fn in cl.items():
        for label, url in (
            ("listing", "https://www.district.in/movies/"),
            ("moviepage", "https://www.district.in/movies/x-movie-tickets-in-gurgaon-MV220590"),
        ):
            t = time.time()
            try:
                r = fn(url, hdr)
                nd = "next_data" if "__NEXT_DATA__" in r.text else "no_next_data"
                extra = ""
                if r.status_code != 200:
                    extra = f" server={r.headers.get('server')} body={snip(r.text, 220)!r}"
                print(f"DISTRICT[{name}] {label}: {r.status_code} len={len(r.text)} {nd} {time.time()-t:.1f}s{extra}", flush=True)
            except Exception as e:
                print(f"DISTRICT[{name}] {label}: EXC {type(e).__name__} {str(e)[:120]}", flush=True)
            time.sleep(0.5)

    wurl, wkey = os.environ.get("DISTRICT_WORKER_URL", ""), os.environ.get("DISTRICT_WORKER_KEY", "")
    if wurl and wkey:
        for label, params in (("discover", {"mode": "discover"}), ("moviepage", {"movie_id": "220590", "city": "gurgaon"})):
            try:
                r = requests.get(wurl, params=params, headers={"x-worker-key": wkey}, timeout=25)
                print(f"DISTRICT[worker] {label}: {r.status_code} len={len(r.text)} body={snip(r.text, 160)!r}", flush=True)
            except Exception as e:
                print(f"DISTRICT[worker] {label}: EXC {type(e).__name__}", flush=True)


if __name__ == "__main__":
    show_ip()
    cl = clients()
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "bms"):
        probe_bms(cl)
    if which in ("all", "district"):
        probe_district(cl)
