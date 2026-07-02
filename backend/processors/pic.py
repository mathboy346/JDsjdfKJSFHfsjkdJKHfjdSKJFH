# Block rates: estimated corporate/blocked booking fraction of seats.
BLOCK_RATES: dict[str, float] = {
    "PVR":        0.005,
    "INOX":       0.0,
    "CINEPOLIS":  0.0325,
    "PVR INOX":   0.005,
}
PIC_CHAINS = frozenset(BLOCK_RATES.keys())


def detect_pic_chain(chain_name: str) -> str | None:
    c = (chain_name or "").upper()
    if "CINEPOLIS" in c:
        return "CINEPOLIS"
    if "PVR" in c and "INOX" in c:
        return "PVR INOX"
    if "PVR" in c:
        return "PVR"
    if "INOX" in c:
        return "INOX"
    return None


def apply_block_rate(
    chain: str, sold: int, gross: float, seats: int
) -> tuple[int, float]:
    """Subtract estimated blocked seats from sold/gross."""
    rate = BLOCK_RATES.get(chain, 0)
    if sold > 0 and rate > 0 and seats > 0:
        avg_price = gross / sold
        blocked   = seats * rate
        adj_sold  = max(0, round(sold - blocked))
        adj_gross = max(0.0, adj_sold * avg_price)
        return adj_sold, round(adj_gross, 2)
    return sold, round(gross, 2)
