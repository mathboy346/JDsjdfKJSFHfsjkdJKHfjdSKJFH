"""Money precision helpers shared across API serializers.

`current_daily`/period/entity gross figures used to be rounded to 2 decimal
places of crores (`round(rupees / 1e7, 2)`). For sub-crore films that destroys
all precision — e.g. a ₹3,000 period bucket becomes `0.0` Cr and renders as a
dash on the frontend. We keep the value in crores (so every existing consumer
and unit stays unchanged) but retain enough decimals (₹10 granularity) so small
films can still be shown in lakhs / thousands / rupees downstream.
"""

# 6 dp of a crore == ₹10 granularity: lossless enough for K/₹ display while
# staying comfortably below the scrape cadence noise for any real film.
GROSS_CR_DP = 6


def to_cr(rupees: float | int | None) -> float:
    """Convert raw rupees to crores, preserving small-film precision."""
    if not rupees:
        return 0.0
    return round(rupees / 1e7, GROSS_CR_DP)
