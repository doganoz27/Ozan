"""
TITAN PRIME — Trades API
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session, Trade, Signal, AccountState
from services.market_data import get_price_cache

router = APIRouter(prefix="/api/trades", tags=["trades"])


def _trade_to_dict(t: Trade, current_price: Optional[float] = None) -> dict:
    pnl_live = None
    pnl_r_live = None
    progress_pct = None

    if t.status == "OPEN" and current_price and t.entry_price and t.sl and t.tp:
        risk = abs(t.entry_price - t.sl)
        if risk > 0:
            if t.direction == "LONG":
                pnl_pts = current_price - t.entry_price
                progress_pct = round(
                    min(max((current_price - t.entry_price) / (t.tp - t.entry_price) * 100, 0), 100), 1
                ) if t.tp > t.entry_price else 0
            else:
                pnl_pts = t.entry_price - current_price
                progress_pct = round(
                    min(max((t.entry_price - current_price) / (t.entry_price - t.tp) * 100, 0), 100), 1
                ) if t.entry_price > t.tp else 0
            pnl_r_live = round(pnl_pts / risk, 2)
            notional = t.margin_gbp * t.leverage if t.margin_gbp and t.leverage else 0
            pnl_live = round(notional * pnl_pts / t.entry_price, 2) if t.entry_price else 0

    return {
        "id": t.id,
        "signal_id": t.signal_id,
        "sym": t.sym,
        "direction": t.direction,
        "entry_price": t.entry_price,
        "sl": t.sl,
        "tp": t.tp,
        "margin_gbp": t.margin_gbp,
        "risk_gbp": t.risk_gbp,
        "target_profit_gbp": t.target_profit_gbp,
        "leverage": t.leverage,
        "status": t.status,
        "pnl_gbp": t.pnl_gbp if t.status != "OPEN" else pnl_live,
        "pnl_r": t.pnl_r if t.status != "OPEN" else pnl_r_live,
        "current_price": current_price,
        "progress_pct": progress_pct,
        "opened_at": t.opened_at.isoformat() if t.opened_at else None,
        "closed_at": t.closed_at.isoformat() if t.closed_at else None,
    }


@router.get("")
async def list_trades(
    status: Optional[str] = Query(None),
    sym: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
    session: AsyncSession = Depends(get_session),
):
    """List all trades."""
    q = select(Trade).order_by(desc(Trade.opened_at))
    if status:
        q = q.where(Trade.status == status)
    if sym:
        q = q.where(Trade.sym == sym)
    q = q.offset(offset).limit(limit)
    result = await session.execute(q)
    trades = result.scalars().all()
    prices = get_price_cache()
    return {
        "trades": [_trade_to_dict(t, prices.get(t.sym, {}).get("price")) for t in trades]
    }


@router.get("/open")
async def get_open_trades(session: AsyncSession = Depends(get_session)):
    """Get all open trades with live P&L."""
    q = select(Trade).where(Trade.status == "OPEN").order_by(desc(Trade.opened_at))
    result = await session.execute(q)
    trades = result.scalars().all()
    prices = get_price_cache()
    return {
        "trades": [_trade_to_dict(t, prices.get(t.sym, {}).get("price")) for t in trades]
    }


@router.get("/{trade_id}")
async def get_trade(trade_id: int, session: AsyncSession = Depends(get_session)):
    """Get a single trade."""
    result = await session.execute(select(Trade).where(Trade.id == trade_id))
    t = result.scalar_one_or_none()
    if not t:
        raise HTTPException(status_code=404, detail="Trade not found")
    prices = get_price_cache()
    return _trade_to_dict(t, prices.get(t.sym, {}).get("price"))


@router.post("/{trade_id}/close")
async def close_trade(
    trade_id: int,
    session: AsyncSession = Depends(get_session),
):
    """Manually close a trade at current market price."""
    result = await session.execute(select(Trade).where(Trade.id == trade_id))
    t = result.scalar_one_or_none()
    if not t:
        raise HTTPException(status_code=404, detail="Trade not found")
    if t.status != "OPEN":
        raise HTTPException(status_code=400, detail=f"Trade already closed ({t.status})")

    prices = get_price_cache()
    current_price = prices.get(t.sym, {}).get("price")
    if not current_price:
        raise HTTPException(status_code=400, detail="Cannot get current price for manual close")

    # Calculate P&L
    risk = abs(t.entry_price - t.sl) if t.sl and t.entry_price else 1
    if t.direction == "LONG":
        pnl_r = round((current_price - t.entry_price) / risk, 2) if risk else 0
    else:
        pnl_r = round((t.entry_price - current_price) / risk, 2) if risk else 0

    notional = (t.margin_gbp or 0) * (t.leverage or 1)
    pnl_gbp = round(notional * (current_price - t.entry_price) / t.entry_price, 2) if t.entry_price else 0
    if t.direction == "SHORT":
        pnl_gbp = -pnl_gbp

    # Determine outcome label
    if t.tp and t.direction == "LONG" and current_price >= t.tp:
        status = "TP"
    elif t.tp and t.direction == "SHORT" and current_price <= t.tp:
        status = "TP"
    elif t.sl and t.direction == "LONG" and current_price <= t.sl:
        status = "SL"
    elif t.sl and t.direction == "SHORT" and current_price >= t.sl:
        status = "SL"
    else:
        status = "CLOSED"

    t.status = status
    t.pnl_gbp = pnl_gbp
    t.pnl_r = pnl_r
    t.closed_at = datetime.utcnow()

    # Update related signal
    if t.signal_id:
        sig_result = await session.execute(select(Signal).where(Signal.id == t.signal_id))
        sig = sig_result.scalar_one_or_none()
        if sig:
            sig.status = status
            sig.out_price = current_price
            sig.act_rr = pnl_r
            sig.closed_at = datetime.utcnow()

    # Update account state
    acc_result = await session.execute(select(AccountState).order_by(AccountState.id.desc()))
    acc = acc_result.scalar_one_or_none()
    if acc:
        acc.balance_gbp = round(acc.balance_gbp + pnl_gbp, 2)
        acc.total_trades = (acc.total_trades or 0) + 1
        if pnl_r > 0:
            acc.wins = (acc.wins or 0) + 1
        else:
            acc.losses = (acc.losses or 0) + 1
        total = acc.total_trades or 1
        acc.win_rate = round(acc.wins / total * 100, 1) if total > 0 else 0
        acc.updated_at = datetime.utcnow()

    await session.commit()

    # Send Telegram outcome
    try:
        from services.telegram_bot import send_outcome_alert
        import asyncio
        asyncio.create_task(send_outcome_alert(t.sym, t.direction, status, t.entry_price, current_price, pnl_r, t.signal_id))
    except Exception:
        pass

    return {
        "trade_id": trade_id,
        "status": status,
        "pnl_gbp": pnl_gbp,
        "pnl_r": pnl_r,
        "out_price": current_price,
    }
