"""Temporary diagnostic probe (BMS pass-rate experiments). Removed after use."""
import base64, json, os, re, sys, time, random
from datetime import datetime, timedelta, timezone
import requests

IST = timezone(timedelta(hours=5, minutes=30))
N = int(os.environ.get("PROBE_N", "1"))
HOST = base64.b64decode("aW4uYm9va215c2hvdy5jb20=").decode()
DATE = datetime.now(IST).strftime("%Y%m%d")
venues = json.load(open(f"backend/data/v{(N % 8) + 1}.json"))
CODES = list(venues)[: int(os.environ.get("PROBE_COUNT", "20"))]


def api(code): return f"https://{HOST}/api/v2/mobile/showtimes/byvenue?venueCode={code}&dateCode={DATE}"

MOBILE_ANDROID = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.210 Mobile Safari/537.36",
    "Accept": "application/json, text/plain, */*", "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
    "Origin": f"https://{HOST}", "Referer": f"https://{HOST}/",
    "sec-ch-ua": '"Chromium";v="120","Not_A Brand";v="8"', "sec-ch-ua-mobile": "?1", "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest": "empty", "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin", "x-appcode": "MOBAND2",
}
PLAIN = {"Accept": "application/json, text/plain, */*", "Accept-Language": "en-IN,en;q=0.9", "Origin": f"https://{HOST}", "Referer": f"https://{HOST}/"}


def run(name, getter, pause=0.5):
    res = []
    for c in CODES:
        t = time.time()
        try:
            r = getter(c)
            res.append("O" if r.text.strip().startswith("{") else f"x{r.status_code}")
        except Exception as e:
            res.append("E")
        time.sleep(pause)
    print(f"BMS[{name}] ok={res.count('O')}/{len(res)} seq={''.join(x[0] for x in res)}", flush=True)


def main():
    try:
        j = requests.get("https://ipinfo.io/json", timeout=10).json(); print(f"RUNNER ip={j.get('ip')} org={j.get('org')}", flush=True)
    except Exception: pass
    from curl_cffi import requests as cffi
    import cloudscraper
    # A: baseline per-request fresh identity
    run("A-cloudscraper-fresh", lambda c: cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "desktop": True}).get(api(c), headers=PLAIN, timeout=20))
    run("B-cffi-safari17_0-fresh-plainhdr", lambda c: cffi.get(api(c), headers=PLAIN, impersonate="safari17_0", timeout=20))
    run("C-cffi-chrome131_android-mobilehdr", lambda c: cffi.get(api(c), headers=MOBILE_ANDROID, impersonate="chrome131_android", timeout=20))
    run("D-cffi-safari17_2_ios+xappcode", lambda c: cffi.get(api(c), headers={**PLAIN, "x-appcode": "MOBAND2"}, impersonate="safari17_2_ios", timeout=20))
    run("E-cffi-chrome131-mobilehdr(UA android)", lambda c: cffi.get(api(c), headers=MOBILE_ANDROID, impersonate="chrome131", timeout=20))
    # F: session, warm cookies from home page first
    s = cffi.Session(impersonate="safari17_0")
    try:
        h = s.get(f"https://{HOST}/", timeout=20); print(f"  warm home: {h.status_code} cookies={list(s.cookies.keys())}", flush=True)
    except Exception as e: print("  warm home EXC", type(e).__name__, flush=True)
    run("F-cffi-safari17_0-session-warm", lambda c: s.get(api(c), headers=PLAIN, timeout=20))
    # G: session reused, no warm
    s2 = cffi.Session(impersonate="safari17_0")
    run("G-cffi-safari17_0-session-nowarm", lambda c: s2.get(api(c), headers=PLAIN, timeout=20))
    # H: slower pace
    run("H-cffi-safari17_0-fresh-slow2s", lambda c: cffi.get(api(c), headers=PLAIN, impersonate="safari17_0", timeout=20), pause=2.0)
    # I: HTTP/1.1 forced
    from curl_cffi.const import CurlHttpVersion
    run("I-cffi-safari17_0-http1.1", lambda c: cffi.get(api(c), headers=PLAIN, impersonate="safari17_0", http_version=CurlHttpVersion.V1_1, timeout=20))

main()
