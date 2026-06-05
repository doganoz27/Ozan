"""
TITAN PRIME — Signals API
"""
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session, Signal, Trade
from core.config import settings

router = APIRouter(prefix="/api/signals", tags=["signals"])


def _signal_to_dict(s: Signal) -> dict:
    return {
        "id": s.id,
        "sym": s.sym,
        "direction": s.direction,
        "quality": s.quality,
        "score": s.score,
        "confidence": s.confidence,
        "entry": s.entry,
        "sl": s.sl,
        "tp": s.tp,
        "rr_t": s.rr_t,
        "act_rr": s.act_rr,
        "status": s.status,
        "out_price": s.out_price,
        "regime": s.regime,
        "contrarian_score": s.contrarian_score,
        "duration": s.duration,
        "f_ema": s.f_ema,
        "f_rsi": s.f_rsi,
        "f_macd": s.f_macd,
        "f_sweep": s.f_sweep,
        "f_ob": s.f_ob,
        "f_fvg": s.f_fvg,
        "f_struct": s.f_struct,
        "narrative": s.narrative,
        "chart_path": s.chart_path,
        "reasons": json.loads(s.reasons) if s.reasons else [],
        "neg_factors": json.loads(s.neg_factors) if s.neg_factors else [],
        "sm_notes": json.loads(s.sm_notes) if s.sm_notes else [],
        "trap_warnings": json.loads(s.trap_warnings) if s.trap_warnings else [],
        "consensus_view": s.consensus_view,
        "sm_view": s.sm_view,
        "news_risk": s.news_risk,
        "sizing": json.loads(s.sizing) if s.sizing else {},
        "hold_h": s.hold_h,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "closed_at": s.closed_at.isoformat() if s.closed_at else None,
    }


@router.get("")
async def list_signals(
    status: Optional[str] = Query(None),
    quality: Optional[str] = Query(None),
    sym: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    session: AsyncSession = Depends(get_session),
):
    """List all signals with optional filters."""
    q = select(Signal).order_by(desc(Signal.created_at))
    if status:
        q = q.where(Signal.status == status)
    if quality:
        q = q.where(Signal.quality == quality)
    if sym:
        q = q.where(Signal.sym == sym)
    q = q.offset(offset).limit(limit)
    result = await session.execute(q)
    signals = result.scalars().all()

    # Count total
    count_q = select(func.count()).select_from(Signal)
    if status:
        count_q = count_q.where(Signal.status == status)
    if quality:
        count_q = count_q.where(Signal.quality == quality)
    if sym:
        count_q = count_q.where(Signal.sym == sym)
    total = (await session.execute(count_q)).scalar_one()

    return {
        "signals": [_signal_to_dict(s) for s in signals],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.get("/active")
async def get_active_signals(session: AsyncSession = Depends(get_session)):
    """Get all OPEN signals."""
    q = select(Signal).where(Signal.status == "OPEN").order_by(desc(Signal.created_at))
    result = await session.execute(q)
    signals = result.scalars().all()
    return {"signals": [_signal_to_dict(s) for s in signals]}


@router.get("/stats")
async def get_signal_stats(session: AsyncSession = Depends(get_session)):
    """Aggregate statistics across all signals."""
    # Total counts by status
    q = select(Signal.status, func.count().label("n")).group_by(Signal.status)
    result = await session.execute(q)
    by_status = {row.status: row.n for row in result}

    # By quality
    q2 = select(Signal.quality, func.count().label("n")).group_by(Signal.quality)
    result2 = await session.execute(q2)
    by_quality = {row.quality: row.n for row in result2}

    # Win rate
    total_closed = sum(by_status.get(s, 0) for s in ["TP", "SL"])
    wins = by_status.get("TP", 0)
    win_rate = round(wins / total_closed * 100, 1) if total_closed > 0 else 0

    # Average actual R:R for closed trades
    q3 = select(func.avg(Signal.act_rr)).where(Signal.act_rr.isnot(None))
    avg_rr = (await session.execute(q3)).scalar_one()

    return {
        "by_status": by_status,
        "by_quality": by_quality,
        "total_closed": total_closed,
        "wins": wins,
        "win_rate": win_rate,
        "avg_rr": round(avg_rr or 0, 2),
    }


@router.get("/{signal_id}")
async def get_signal(signal_id: int, session: AsyncSession = Depends(get_session)):
    """Get a single signal by ID."""
    result = await session.execute(select(Signal).where(Signal.id == signal_id))
    s = result.scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="Signal not found")
    return _signal_to_dict(s)


@router.post("/{signal_id}/enter")
async def enter_signal(signal_id: int, session: AsyncSession = Depends(get_session)):
    """Convert a WATCHLIST signal into an active OPEN trade."""
    result = await session.execute(select(Signal).where(Signal.id == signal_id))
    s = result.scalar_one_or_none()
    if not s:
        raise HTTPException(status_code=404, detail="Signal not found")

    if s.status not in ("WATCHLIST", "APPROVED"):
        raise HTTPException(status_code=400, detail=f"Cannot enter signal with status {s.status}")

    # Create a trade
    sizing = json.loads(s.sizing) if s.sizing else {}
    trade = Trade(
        signal_id=s.id,
        sym=s.sym,
        direction=s.direction,
        entry_price=s.entry,
        sl=s.sl,
        tp=s.tp,
        margin_gbp=sizing.get("margin", 0),
        risk_gbp=sizing.get("risk_amt", 0),
        target_profit_gbp=sizing.get("exp_profit", 0),
        leverage=sizing.get("leverage", 1),
        status="OPEN",
        opened_at=datetime.utcnow(),
    )
    session.add(trade)

    # Update signal status
    s.status = "OPEN"
    await session.commit()
    await session.refresh(trade)

    return {
        "trade_id": trade.id,
        "signal_id": signal_id,
        "message": f"Trade opened for {s.sym} {s.direction}",
    }
