from sqlalchemy import (
    Column, String, Integer, Float, Boolean, Date, DateTime, BigInteger,
    Text, ForeignKey, UniqueConstraint, Index, func
)
from sqlalchemy.dialects.postgresql import JSONB
from backend.database import Base


class Movie(Base):
    __tablename__ = "movies"
    slug           = Column(String(300), primary_key=True)
    canonical_name = Column(String(300), nullable=False)
    norm_key       = Column(String(300), unique=True)
    poster_url     = Column(Text)
    genres         = Column(JSONB, default=list)
    languages      = Column(JSONB, default=list)
    formats        = Column(JSONB, default=list)
    release_date   = Column(Date)
    runtime_mins   = Column(Integer)
    description    = Column(Text)
    first_seen_at  = Column(DateTime(timezone=True), server_default=func.now())
    last_updated   = Column(DateTime(timezone=True), server_default=func.now())


class MovieVariant(Base):
    __tablename__ = "movie_variants"
    variant_key = Column(String(400), primary_key=True)
    movie_slug  = Column(String(300), ForeignKey("movies.slug", ondelete="CASCADE"))
    language    = Column(String(50))
    lang_group  = Column(String(10))  # Hindi / South / English / Other
    format      = Column(String(80))
    created_at  = Column(DateTime(timezone=True), server_default=func.now())


class CurrentAdvance(Base):
    __tablename__ = "current_advance"
    # Composite PK (not variant_key alone) — T+1/T+2/T+3 rows for the same
    # movie must coexist, since the advance window now scrapes 3 future dates
    # every cycle instead of overwriting a single "tomorrow" row.
    variant_key = Column(String(400), primary_key=True)
    date_for    = Column(Date, nullable=False, primary_key=True)
    shows       = Column(Integer, default=0)
    gross       = Column(Float, default=0)
    sold        = Column(Integer, default=0)
    total_seats = Column(Integer, default=0)
    venues      = Column(Integer, default=0)
    cities      = Column(Integer, default=0)
    fastfilling = Column(Integer, default=0)
    housefull   = Column(Integer, default=0)
    occupancy   = Column(Float, default=0)
    nett_cr_est = Column(Float, default=0)
    fetched_at  = Column(DateTime(timezone=True), server_default=func.now())


class CurrentDaily(Base):
    __tablename__ = "current_daily"
    variant_key = Column(String(400), primary_key=True)
    date_for    = Column(Date, nullable=False)
    shows       = Column(Integer, default=0)
    gross       = Column(Float, default=0)
    sold        = Column(Integer, default=0)
    total_seats = Column(Integer, default=0)
    venues      = Column(Integer, default=0)
    cities      = Column(Integer, default=0)
    fastfilling = Column(Integer, default=0)
    housefull   = Column(Integer, default=0)
    occupancy   = Column(Float, default=0)
    nett_cr_est = Column(Float, default=0)
    fetched_at  = Column(DateTime(timezone=True), server_default=func.now())


class AdvanceCity(Base):
    __tablename__ = "advance_city"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    city        = Column(String(100))
    state       = Column(String(100))
    region      = Column(String(20))
    venues      = Column(Integer)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    fastfilling = Column(Integer)
    housefull   = Column(Integer)
    occupancy   = Column(Float)
    __table_args__ = (UniqueConstraint("variant_key", "date_for", "city"),)


class DailyCity(Base):
    __tablename__ = "daily_city"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    city        = Column(String(100))
    state       = Column(String(100))
    region      = Column(String(20))
    venues      = Column(Integer)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    fastfilling = Column(Integer)
    housefull   = Column(Integer)
    occupancy   = Column(Float)
    __table_args__ = (UniqueConstraint("variant_key", "date_for", "city"),)


class AdvanceChain(Base):
    __tablename__ = "advance_chain"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    chain       = Column(String(150))
    is_pic      = Column(Boolean, default=False)
    venues      = Column(Integer)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    gross_adj   = Column(Float)
    sold_adj    = Column(Integer)
    fastfilling = Column(Integer)
    housefull   = Column(Integer)
    occupancy   = Column(Float)
    __table_args__ = (UniqueConstraint("variant_key", "date_for", "chain"),)


class DailyChain(Base):
    __tablename__ = "daily_chain"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    chain       = Column(String(150))
    is_pic      = Column(Boolean, default=False)
    venues      = Column(Integer)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    gross_adj   = Column(Float)
    sold_adj    = Column(Integer)
    fastfilling = Column(Integer)
    housefull   = Column(Integer)
    occupancy   = Column(Float)
    __table_args__ = (UniqueConstraint("variant_key", "date_for", "chain"),)


class AdvanceHistory(Base):
    __tablename__ = "advance_history"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    snapshot_at = Column(DateTime(timezone=True), nullable=False)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    occupancy   = Column(Float)
    nett_cr_est = Column(Float)
    __table_args__ = (
        Index("adv_hist_idx", "variant_key", "date_for", "snapshot_at"),
    )


class DailyHistory(Base):
    __tablename__ = "daily_history"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    snapshot_at = Column(DateTime(timezone=True), nullable=False)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    occupancy   = Column(Float)
    nett_cr_est = Column(Float)
    __table_args__ = (
        Index("daily_hist_idx", "variant_key", "date_for", "snapshot_at"),
    )


class BmsShowLog(Base):
    __tablename__ = "bms_show_log"
    show_date       = Column(Date, nullable=False, primary_key=True)
    session_key     = Column(String(600), nullable=False, primary_key=True)
    movie           = Column(String(400))
    variant_key     = Column(String(400))
    venue           = Column(String(300))
    chain           = Column(String(150))
    city            = Column(String(100))
    state           = Column(String(100))
    time            = Column(String(20))
    audi            = Column(String(200))
    session_id      = Column(String(100))
    total_seats     = Column(Integer, default=0)
    sold            = Column(Integer, default=0)
    available       = Column(Integer, default=0)
    gross           = Column(Float, default=0)
    occupancy       = Column(Float, default=0)
    mins_left       = Column(Float)
    first_seen_at   = Column(DateTime(timezone=True), server_default=func.now())
    last_updated_at = Column(DateTime(timezone=True), server_default=func.now())
    __table_args__ = (
        Index("bms_show_log_variant_idx", "variant_key", "show_date"),
        Index("bms_show_log_date_idx", "show_date"),
    )


class Venue(Base):
    __tablename__ = "venues"
    venue_code   = Column(String(20), primary_key=True)
    venue_name   = Column(String(300))
    city         = Column(String(100))
    state        = Column(String(100))
    chain        = Column(String(150))
    latitude     = Column(Float)
    longitude    = Column(Float)
    last_updated = Column(DateTime(timezone=True), server_default=func.now())


# --- District aggregate tables -----------------------------------------
# Mirror the 8 BMS aggregate tables above 1:1 (same columns), kept as
# separate tables rather than a shared `source` column, for the same reason
# district_show_log is separate from bms_show_log (see
# docs/district-integration-plan.md, bh repo, section 2).

class DistrictCurrentAdvance(Base):
    __tablename__ = "district_current_advance"
    variant_key = Column(String(400), primary_key=True)
    date_for    = Column(Date, nullable=False, primary_key=True)
    shows       = Column(Integer, default=0)
    gross       = Column(Float, default=0)
    sold        = Column(Integer, default=0)
    total_seats = Column(Integer, default=0)
    venues      = Column(Integer, default=0)
    cities      = Column(Integer, default=0)
    fastfilling = Column(Integer, default=0)
    housefull   = Column(Integer, default=0)
    occupancy   = Column(Float, default=0)
    nett_cr_est = Column(Float, default=0)
    fetched_at  = Column(DateTime(timezone=True), server_default=func.now())


class DistrictCurrentDaily(Base):
    __tablename__ = "district_current_daily"
    variant_key = Column(String(400), primary_key=True)
    date_for    = Column(Date, nullable=False)
    shows       = Column(Integer, default=0)
    gross       = Column(Float, default=0)
    sold        = Column(Integer, default=0)
    total_seats = Column(Integer, default=0)
    venues      = Column(Integer, default=0)
    cities      = Column(Integer, default=0)
    fastfilling = Column(Integer, default=0)
    housefull   = Column(Integer, default=0)
    occupancy   = Column(Float, default=0)
    nett_cr_est = Column(Float, default=0)
    fetched_at  = Column(DateTime(timezone=True), server_default=func.now())


class DistrictAdvanceCity(Base):
    __tablename__ = "district_advance_city"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    city        = Column(String(100))
    state       = Column(String(100))
    region      = Column(String(20))
    venues      = Column(Integer)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    fastfilling = Column(Integer)
    housefull   = Column(Integer)
    occupancy   = Column(Float)
    __table_args__ = (UniqueConstraint("variant_key", "date_for", "city"),)


class DistrictDailyCity(Base):
    __tablename__ = "district_daily_city"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    city        = Column(String(100))
    state       = Column(String(100))
    region      = Column(String(20))
    venues      = Column(Integer)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    fastfilling = Column(Integer)
    housefull   = Column(Integer)
    occupancy   = Column(Float)
    __table_args__ = (UniqueConstraint("variant_key", "date_for", "city"),)


class DistrictAdvanceChain(Base):
    __tablename__ = "district_advance_chain"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    chain       = Column(String(150))
    is_pic      = Column(Boolean, default=False)
    venues      = Column(Integer)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    gross_adj   = Column(Float)
    sold_adj    = Column(Integer)
    fastfilling = Column(Integer)
    housefull   = Column(Integer)
    occupancy   = Column(Float)
    __table_args__ = (UniqueConstraint("variant_key", "date_for", "chain"),)


class DistrictDailyChain(Base):
    __tablename__ = "district_daily_chain"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    chain       = Column(String(150))
    is_pic      = Column(Boolean, default=False)
    venues      = Column(Integer)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    gross_adj   = Column(Float)
    sold_adj    = Column(Integer)
    fastfilling = Column(Integer)
    housefull   = Column(Integer)
    occupancy   = Column(Float)
    __table_args__ = (UniqueConstraint("variant_key", "date_for", "chain"),)


class DistrictAdvanceHistory(Base):
    __tablename__ = "district_advance_history"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    snapshot_at = Column(DateTime(timezone=True), nullable=False)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    occupancy   = Column(Float)
    nett_cr_est = Column(Float)
    __table_args__ = (
        Index("district_adv_hist_idx", "variant_key", "date_for", "snapshot_at"),
    )


class DistrictDailyHistory(Base):
    __tablename__ = "district_daily_history"
    id          = Column(BigInteger, primary_key=True, autoincrement=True)
    variant_key = Column(String(400))
    date_for    = Column(Date)
    snapshot_at = Column(DateTime(timezone=True), nullable=False)
    shows       = Column(Integer)
    gross       = Column(Float)
    sold        = Column(Integer)
    total_seats = Column(Integer)
    occupancy   = Column(Float)
    nett_cr_est = Column(Float)
    __table_args__ = (
        Index("district_daily_hist_idx", "variant_key", "date_for", "snapshot_at"),
    )
