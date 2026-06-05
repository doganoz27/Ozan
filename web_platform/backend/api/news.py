"""
TITAN PRIME — News API
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session, NewsItem
from services.market_data import get_news_cache

router = APIRouter(prefix="/api/news", tags=["news"])


@router.get("")
async def list_news(
    limit: int = Query(30, le=100),
    min_importance: int = Query(0),
    bias: str = Query(None),
    session: AsyncSession = Depends(get_session),
):
    """List news items with optional filters."""
    # Prefer in-memory cache
    cached = get_news_cache()
    if cached:
        filtered = cached
        if min_importance:
            filtered = [n for n in filtered if n.get("importance", 0) >= min_importance]
        if bias:
            filtered = [n for n in filtered if n.get("bias", "").upper() == bias.upper()]
        return {"news": filtered[:limit]}

    # Fall back to DB
    q = select(NewsItem).order_by(desc(NewsItem.created_at))
    if min_importance:
        q = q.where(NewsItem.importance >= min_importance)
    if bias:
        q = q.where(NewsItem.bias == bias.upper())
    q = q.limit(limit)
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
                "created_at": n.created_at.isoformat() if n.created_at else None,
            }
            for n in items
        ]
    }


@router.get("/high-impact")
async def get_high_impact_news(session: AsyncSession = Depends(get_session)):
    """Get only high-impact news (importance >= 60)."""
    cached = get_news_cache()
    if cached:
        high = [n for n in cached if n.get("importance", 0) >= 60]
        return {"news": high[:20]}

    q = select(NewsItem).where(NewsItem.importance >= 60).order_by(desc(NewsItem.created_at)).limit(20)
    result = await session.execute(q)
    items = result.scalars().all()
    return {
        "news": [
            {
                "id": n.id,
                "headline": n.headline,
                "importance": n.importance,
                "risk_level": n.risk_level,
                "bias": n.bias,
                "published_at": n.published_at.isoformat() if n.published_at else None,
            }
            for n in items
        ]
    }
