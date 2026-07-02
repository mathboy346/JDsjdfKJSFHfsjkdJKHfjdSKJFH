"""Parse show-listing JSON payload into flat row records."""


def parse_payload(data: dict, date_code: str) -> list[dict]:
    out = []
    sd = data.get("ShowDetails", [])
    if not sd:
        return out

    venue_info = sd[0].get("Venues", {})
    venue_name = venue_info.get("VenueName", "")
    venue_add = venue_info.get("VenueAdd", "")
    chain = venue_info.get("VenueCompName", "Unknown")

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")

        for ch in ev.get("ChildEvents", []):
            dim = (ch.get("EventDimension") or "").strip()
            lang = (ch.get("EventLanguage") or "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                if sh.get("ShowDateCode") != date_code:
                    continue

                total = sold = gross = 0
                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0) or 0)
                    free = int(cat.get("SeatsAvail", 0) or 0)
                    price = float(cat.get("CurPrice", 0) or 0)
                    total += seats
                    sold += seats - free
                    gross += (seats - free) * price

                out.append({
                    "movie": movie,
                    "venue": venue_name,
                    "address": venue_add,
                    "chain": chain,
                    "time": sh.get("ShowTime", ""),
                    "audi": sh.get("Attributes") or "",
                    "session_id": str(sh.get("SessionId", "") or ""),
                    "totalSeats": total,
                    "sold": sold,
                    "available": total - sold,
                    "gross": round(gross, 2),
                    "source": "BMS",
                })
    return out


def dedupe_rows(rows: list[dict]) -> list[dict]:
    """Deduplicate by (venue, time, session_id, audi)."""
    seen, out = set(), []
    for r in rows:
        key = (r["venue"], r["time"], r["session_id"], r["audi"])
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out
