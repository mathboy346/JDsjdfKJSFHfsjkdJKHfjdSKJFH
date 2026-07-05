from decimal import Decimal, ROUND_HALF_UP

from backend.money import GROSS_CR_DP

NETT_MULTIPLIER = 0.88


def round05(v: float) -> float:
    """Round to nearest 0.05 for crore-scale films.

    Sub-₹5-lakh films (v < 0.5 Cr) used to be rounded to 2 dp of a crore, which
    is ₹1-lakh granularity — that collapses small-film nett to 0.0 and renders a
    dash. Keep ₹10 precision below the 0.05-Cr threshold so lakh/thousand nett
    still displays; large films keep their tidy 0.05-Cr steps.
    """
    if v < 0.5:
        return round(v, GROSS_CR_DP)
    return float(
        (Decimal(str(v)) * 20).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        / Decimal("20")
    )


def estimate_nett_cr(gross: float, shows: int) -> float:
    """Estimate nett collection in crores from raw gross (rupees)."""
    if gross <= 0 or shows <= 0:
        return 0.0
    return round05((gross * NETT_MULTIPLIER) / 10_000_000)
