"""
TITAN PRIME — SQLAlchemy Async Database Models
"""
from datetime import datetime
from typing import Optional, AsyncGenerator

from sqlalchemy import (
    Integer, String, Float, Boolean, DateTime, Text, BigInteger,
    ForeignKey, Index, func
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from core.config import settings


class Base(DeclarativeBase):
    pass


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sym: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    direction: Mapped[str] = mapped_column(String(10), nullable=False)  # LONG/SHORT
    quality: Mapped[str] = mapped_column(String(20), nullable=False)  # A+/A/B+/B/WATCHLIST
    score: Mapped[float] = mapped_column(Float, default=0.0)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    entry: Mapped[float] = mapped_column(Float, nullable=False)
    sl: Mapped[float] = mapped_column(Float, nullable=False)
    tp: Mapped[float] = mapped_column(Float, nullable=False)
    rr_t: Mapped[float] = mapped_column(Float, default=0.0)  # target R:R
    act_rr: Mapped[Optional[float]] = mapped_column(Float, nullable=True)  # actual R:R when closed
    status: Mapped[str] = mapped_column(String(20), default="OPEN", index=True)  # OPEN/TP/SL/EXPIRED/INVALIDATED
    out_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    regime: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    contrarian_score: Mapped[int] = mapped_column(Integer, default=0)
    duration: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # Scalp/Intraday/Swing
    # Feature flags
    f_ema: Mapped[bool] = mapped_column(Boolean, default=False)
    f_rsi: Mapped[bool] = mapped_column(Boolean, default=False)
    f_macd: Mapped[bool] = mapped_column(Boolean, default=False)
    f_sweep: Mapped[bool] = mapped_column(Boolean, default=False)
    f_ob: Mapped[bool] = mapped_column(Boolean, default=False)
    f_fvg: Mapped[bool] = mapped_column(Boolean, default=False)
    f_struct: Mapped[bool] = mapped_column(Boolean, default=False)
    # Analysis text
    narrative: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    chart_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    reasons: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON list
    neg_factors: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON list
    sm_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON list
    trap_warnings: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON list
    consensus_view: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sm_view: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    news_risk: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    sizing: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON dict
    hold_h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), index=True)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    trades: Mapped[list["Trade"]] = relationship("Trade", back_populates="signal", lazy="select")

    __table_args__ = (
        Index("ix_signals_sym_status", "sym", "status"),
        Index("ix_signals_quality", "quality"),
    )


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("signals.id"), nullable=True)
    sym: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    direction: Mapped[str] = mapped_column(String(10), nullable=False)
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    sl: Mapped[float] = mapped_column(Float, nullable=False)
    tp: Mapped[float] = mapped_column(Float, nullable=False)
    margin_gbp: Mapped[float] = mapped_column(Float, default=0.0)
    risk_gbp: Mapped[float] = mapped_column(Float, default=0.0)
    target_profit_gbp: Mapped[float] = mapped_column(Float, default=0.0)
    leverage: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(20), default="OPEN", index=True)  # OPEN/TP/SL/CLOSED
    pnl_gbp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pnl_r: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    signal: Mapped[Optional["Signal"]] = relationship("Signal", back_populates="trades")
    journal_entries: Mapped[list["JournalEntry"]] = relationship("JournalEntry", back_populates="trade", lazy="select")


class JournalEntry(Base):
    __tablename__ = "journal"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    trade_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("trades.id"), nullable=True)
    sym: Mapped[str] = mapped_column(String(20), nullable=False)
    direction: Mapped[str] = mapped_column(String(10), nullable=False)
    entry_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exit_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    lessons: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    chart_path: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())

    trade: Mapped[Optional["Trade"]] = relationship("Trade", back_populates="journal_entries")


class MarketData(Base):
    __tablename__ = "market_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sym: Mapped[str] = mapped_column(String(20), nullable=False, unique=True, index=True)
    price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    change_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    high: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    low: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class NewsItem(Base):
    __tablename__ = "news"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    headline: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    importance: Mapped[int] = mapped_column(Integer, default=0)
    risk_level: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    bias: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    bull_pct: Mapped[int] = mapped_column(Integer, default=0)
    bear_pct: Mapped[int] = mapped_column(Integer, default=0)
    affected_assets: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=func.now())


class AdaptiveWeight(Base):
    __tablename__ = "weights"

    feature: Mapped[str] = mapped_column(String(50), primary_key=True)
    weight: Mapped[float] = mapped_column(Float, default=1.0)
    win_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    sample_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


class AccountState(Base):
    __tablename__ = "account_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    balance_gbp: Mapped[float] = mapped_column(Float, default=50.0)
    peak_gbp: Mapped[float] = mapped_column(Float, default=50.0)
    drawdown_pct: Mapped[float] = mapped_column(Float, default=0.0)
    total_trades: Mapped[int] = mapped_column(Integer, default=0)
    wins: Mapped[int] = mapped_column(Integer, default=0)
    losses: Mapped[int] = mapped_column(Integer, default=0)
    win_rate: Mapped[float] = mapped_column(Float, default=0.0)
    avg_rr: Mapped[float] = mapped_column(Float, default=0.0)
    profit_factor: Mapped[float] = mapped_column(Float, default=0.0)
    sharpe: Mapped[float] = mapped_column(Float, default=0.0)
    sortino: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())


# ── Database engine & session factory ─────────────────────────────────────────
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def init_db():
    """Create all tables."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # Seed default adaptive weights
    async with async_session_factory() as session:
        from sqlalchemy import select
        for feature in ["f_ema", "f_rsi", "f_macd", "f_sweep", "f_ob", "f_fvg", "f_struct"]:
            result = await session.execute(select(AdaptiveWeight).where(AdaptiveWeight.feature == feature))
            if not result.scalar_one_or_none():
                session.add(AdaptiveWeight(feature=feature, weight=1.0, sample_count=0))
        # Seed account state if not exists
        result = await session.execute(select(AccountState))
        if not result.scalar_one_or_none():
            session.add(AccountState())
        await session.commit()


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency injection for FastAPI routes."""
    async with async_session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
