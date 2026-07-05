from decimal import Decimal, ROUND_HALF_UP

from backend.money import GROSS_CR_DP

NETT_MULTIPLIER = 0.88


def round_to_lakh(v: float) -> float:
    """Round to the nearest lakh (0.01 Cr) for crore-scale films.

    Sub-₹1-lakh films (v < 0.5 Cr) keep ₹10 precision (GROSS_CR_DP) instead —
    rounding those to a whole lakh would collapse small-film nett to 0.0 and
    render a dash.
    """
    if v < 0.5:
        return round(v, GROSS_CR_DP)
    return float(
        (Decimal(str(v)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        / Decimal("100")
    )


def estimate_nett_cr(gross: float, shows: int) -> float:
    """Estimate nett collection in crores from raw gross (rupees)."""
    if gross <= 0 or shows <= 0:
        return 0.0
    return round_to_lakh((gross * NETT_MULTIPLIER) / 10_000_000)
