from backend.processors.pic import detect_pic_chain, apply_block_rate
from backend.processors.normalizer import state_to_region


def aggregate_rows(rows: list[dict]) -> dict:
    """
    Convert flat show rows into per-movie summary with city/chain breakdowns.
    """
    summary: dict = {}

    for r in rows:
        movie   = r["movie"]
        city    = r.get("city", "Unknown")
        state   = r.get("state", "Unknown")
        venue   = r["venue"]
        chain   = r["chain"]
        total   = r["totalSeats"]
        sold    = r["sold"]
        gross   = r["gross"]
        occ     = (sold / total * 100) if total else 0.0
        ff      = 1 if 50 <= occ < 98 else 0
        hf      = 1 if occ >= 98 else 0
        region  = state_to_region(state)

        if movie not in summary:
            summary[movie] = {
                "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
                "venues": set(), "cities": set(),
                "fastfilling": 0, "housefull": 0,
                "details": {}, "Chain_details": {},
            }

        m = summary[movie]
        m["shows"]      += 1
        m["gross"]      += gross
        m["sold"]       += sold
        m["totalSeats"] += total
        m["venues"].add(venue)
        m["cities"].add(city)
        m["fastfilling"] += ff
        m["housefull"]   += hf

        # DB unique key is (variant_key, date_for, city) — merge by city name only.
        ck = city
        if ck not in m["details"]:
            m["details"][ck] = {
                "city": city, "state": state, "region": region,
                "venues": set(), "shows": 0, "gross": 0.0,
                "sold": 0, "totalSeats": 0, "fastfilling": 0, "housefull": 0,
                "_state_gross": {},
            }
        d = m["details"][ck]
        d["_state_gross"][state] = d["_state_gross"].get(state, 0.0) + gross
        dominant_state = max(d["_state_gross"], key=d["_state_gross"].get)
        d["state"] = dominant_state
        d["region"] = state_to_region(dominant_state)
        d["venues"].add(venue)
        d["shows"] += 1; d["gross"] += gross
        d["sold"] += sold; d["totalSeats"] += total
        d["fastfilling"] += ff; d["housefull"] += hf

        if chain not in m["Chain_details"]:
            m["Chain_details"][chain] = {
                "chain": chain, "venues": set(), "shows": 0, "gross": 0.0,
                "sold": 0, "totalSeats": 0, "fastfilling": 0, "housefull": 0,
                "is_pic": bool(detect_pic_chain(chain)),
            }
        c = m["Chain_details"][chain]
        c["venues"].add(venue)
        c["shows"] += 1; c["gross"] += gross
        c["sold"] += sold; c["totalSeats"] += total
        c["fastfilling"] += ff; c["housefull"] += hf

    return _finalize(summary)


def _finalize(summary: dict) -> dict:
    final = {}
    for movie, m in summary.items():
        total = m["totalSeats"]
        sold  = m["sold"]
        occ   = round((sold / total * 100), 2) if total else 0.0

        details = []
        for d in m["details"].values():
            dt = d["totalSeats"]; ds = d["sold"]
            d.pop("_state_gross", None)
            details.append({
                "city": d["city"], "state": d["state"], "region": d["region"],
                "venues": len(d["venues"]), "shows": d["shows"],
                "gross": round(d["gross"], 2), "sold": ds, "totalSeats": dt,
                "fastfilling": d["fastfilling"], "housefull": d["housefull"],
                "occupancy": round((ds / dt * 100), 2) if dt else 0.0,
            })

        chains = []
        for c in m["Chain_details"].values():
            ct = c["totalSeats"]; cs = c["sold"]
            pic = detect_pic_chain(c["chain"])
            adj_sold, adj_gross = (
                apply_block_rate(pic, cs, c["gross"], ct) if pic else (cs, round(c["gross"], 2))
            )
            chains.append({
                "chain": c["chain"],
                "venues": len(c["venues"]), "shows": c["shows"],
                "gross": round(c["gross"], 2), "sold": cs, "totalSeats": ct,
                "fastfilling": c["fastfilling"], "housefull": c["housefull"],
                "occupancy": round((cs / ct * 100), 2) if ct else 0.0,
                "is_pic": c["is_pic"],
                "sold_adj": adj_sold, "gross_adj": round(adj_gross, 2),
            })

        final[movie] = {
            "shows": m["shows"], "gross": round(m["gross"], 2),
            "sold": sold, "totalSeats": total,
            "venues": len(m["venues"]), "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"], "housefull": m["housefull"],
            "occupancy": occ, "details": details, "Chain_details": chains,
        }
    return final
