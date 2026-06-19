"""
TITAN PRIME — Market Data API
"""
from fastapi import APIRouter, Depends
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session, NewsItem
from core.config import ALL_SYMBOLS, EQ_SYMBOLS, FOREX, METALS, OIL
from services.market_data import get_price_cache, get_news_cache

router = APIRouter(prefix="/api/market", tags=["market"])


@router.get("/snapshot")
async def get_market_snapshot():
    """Current prices for all symbols."""
    prices = get_price_cache()
    snapshot = {
        "equities": [],
        "forex": [],
        "metals": [],
        "oil": [],
    }

    for sym in EQ_SYMBOLS:
        d = prices.get(sym, {})
        snapshot["equities"].append({
            "sym": sym,
            "price": d.get("price"),
            "change_pct": d.get("change_pct", 0),
            "high": d.get("high"),
            "low": d.get("low"),
            "updated_at": d.get("updated_at"),
        })

    for sym in FOREX:
        d = prices.get(sym, {})
        snapshot["forex"].append({
            "sym": sym,
            "price": d.get("price"),
            "change_pct": d.get("change_pct", 0),
            "high": d.get("high"),
            "low": d.get("low"),
            "updated_at": d.get("updated_at"),
        })

    for sym in METALS:
        d = prices.get(sym, {})
        snapshot["metals"].append({
            "sym": sym,
            "price": d.get("price"),
            "change_pct": d.get("change_pct", 0),
            "high": d.get("high"),
            "low": d.get("low"),
            "updated_at": d.get("updated_at"),
        })

    for sym in OIL:
        d = prices.get(sym, {})
        snapshot["oil"].append({
            "sym": sym,
            "price": d.get("price"),
            "change_pct": d.get("change_pct", 0),
            "high": d.get("high"),
            "low": d.get("low"),
            "updated_at": d.get("updated_at"),
        })

    return snapshot


@router.get("/regime")
async def get_market_regime():
    """
    Determine current market regime from price action and news.
    Returns: Risk-On / Risk-Off / Neutral + session status.
    """
    from datetime import datetime, timezone
    prices = get_price_cache()
    news = get_news_cache()

    # Simple regime from SPY change + news
    spy_chg = prices.get("SPY", {}).get("change_pct", 0) or 0
    gold_chg = prices.get("XAUUSD", {}).get("change_pct", 0) or 0

    # News bias
    bull_news = sum(1 for n in news[:5] if n.get("bias") == "BULLISH")
    bear_news = sum(1 for n in news[:5] if n.get("bias") == "BEARISH")

    if spy_chg > 0.3 and bull_news >= bear_news:
        regime = "Risk-On"
        regime_color = "green"
    elif spy_chg < -0.3 or (gold_chg > 0.5 and bear_news > bull_news):
        regime = "Risk-Off"
        regime_color = "red"
    else:
        regime = "Neutral"
        regime_color = "yellow"

    # Trading session
    now_utc = datetime.now(timezone.utc)
    hour_utc = now_utc.hour
    if 8 <= hour_utc < 16:
        session = "London"
        session_active = True
    elif 13 <= hour_utc < 21:
        session = "New York"
        session_active = True
    elif 0 <= hour_utc < 8:
        session = "Asia"
        session_active = True
    else:
        session = "Closed"
        session_active = False

    # Check overlap
    if 13 <= hour_utc < 16:
        session = "London/New York Overlap"
        session_active = True

    return {
        "regime": regime,
        "regime_color": regime_color,
        "session": session,
        "session_active": session_active,
        "spy_change": spy_chg,
        "gold_change": gold_chg,
        "bull_news_count": bull_news,
        "bear_news_count": bear_news,
        "timestamp": now_utc.isoformat(),
    }


@router.get("/news")
async def get_market_news(
    limit: int = 20,
    min_importance: int = 0,
    session=Depends(get_session),
):
    """Get recent analyzed news items."""
    # First try in-memory cache (fresh)
    cached = get_news_cache()
    if cached:
        filtered = [n for n in cached if n.get("importance", 0) >= min_importance]
        return {"news": filtered[:limit]}

    # Fall back to DB
    q = select(NewsItem).where(
        NewsItem.importance >= min_importance
    ).order_by(desc(NewsItem.created_at)).limit(limit)
    result = await session.execute(q)
    items = result.scalars().all()
    return {
        "news": [
            {
                "id": n.id,
                "headline": n.headline,
                "summary": n.summary,
                "importance": n.importance,
                "risk_level": n.risk_level,
                "bias": n.bias,
                "bull_pct": n.bull_pct,
                "bear_pct": n.bear_pct,
                "published_at": n.published_at.isoformat() if n.published_at else None,
            }
            for n in items
        ]
    }
