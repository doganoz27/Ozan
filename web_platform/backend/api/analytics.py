"""
TITAN PRIME — Analytics API
"""
import math
from statistics import stdev as _std
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session, Signal, Trade, AdaptiveWeight, AccountState

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


def _sharpe(returns: list) -> float:
    if len(returns) < 3:
        return 0.0
    try:
        m = sum(returns) / len(returns)
        s = _std(returns)
        return round(m / s * math.sqrt(252), 2) if s else 0.0
    except Exception:
        return 0.0


def _sortino(returns: list) -> float:
    if len(returns) < 3:
        return 0.0
    try:
        m = sum(returns) / len(returns)
        neg = [r for r in returns if r < 0]
        if not neg:
            return 9.99
        ds = _std(neg)
        return round(m / ds * math.sqrt(252), 2) if ds else 0.0
    except Exception:
        return 0.0


def _max_drawdown(equity: list) -> float:
    if len(equity) < 2:
        return 0.0
    peak = equity[0]
    mdd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100 if peak else 0
        if dd > mdd:
            mdd = dd
    return round(mdd, 2)


@router.get("/performance")
async def get_performance(session: AsyncSession = Depends(get_session)):
    """Full performance statistics."""
    # Account state
    acc_result = await session.execute(select(AccountState).order_by(AccountState.id.desc()))
    acc = acc_result.scalar_one_or_none()

    # Closed signals with act_rr
    q = select(Signal).where(Signal.act_rr.isnot(None), Signal.status.in_(["TP", "SL"]))
    result = await session.execute(q.order_by(Signal.created_at))
    signals = result.scalars().all()

    total = len(signals)
    wins = sum(1 for s in signals if s.status == "TP")
    losses = total - wins
    win_rate = round(wins / total * 100, 1) if total > 0 else 0

    rr_values = [s.act_rr for s in signals if s.act_rr is not None]
    avg_rr = round(sum(rr_values) / len(rr_values), 2) if rr_values else 0

    win_rr = [r for r in rr_values if r > 0]
    loss_rr = [abs(r) for r in rr_values if r <= 0]
    avg_win = sum(win_rr) / len(win_rr) if win_rr else 0
    avg_loss = sum(loss_rr) / len(loss_rr) if loss_rr else 1
    profit_factor = round((avg_win * wins) / (avg_loss * losses), 2) if losses > 0 and avg_loss > 0 else 0

    # R returns for Sharpe/Sortino
    returns = rr_values
    sharpe = _sharpe(returns)
    sortino = _sortino(returns)

    # Kelly criterion
    b = avg_win / avg_loss if avg_loss else 1
    p = win_rate / 100
    q_k = 1 - p
    kelly = round(max(0, min((b * p - q_k) / b * 100, 25)), 1) if b else 0

    balance = acc.balance_gbp if acc else 50.0
    peak = acc.peak_gbp if acc else 50.0

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_rr": avg_rr,
        "profit_factor": profit_factor,
        "sharpe": sharpe,
        "sortino": sortino,
        "kelly": kelly,
        "balance_gbp": balance,
        "peak_gbp": peak,
        "drawdown_pct": acc.drawdown_pct if acc else 0,
        "avg_win_r": round(avg_win, 2),
        "avg_loss_r": round(avg_loss, 2),
    }


@router.get("/by_symbol")
async def performance_by_symbol(session: AsyncSession = Depends(get_session)):
    """Per-symbol performance breakdown."""
    q = select(Signal).where(Signal.act_rr.isnot(None), Signal.status.in_(["TP", "SL"]))
    result = await session.execute(q)
    signals = result.scalars().all()

    by_sym: dict = {}
    for s in signals:
        if s.sym not in by_sym:
            by_sym[s.sym] = {"total": 0, "wins": 0, "rr_sum": 0}
        by_sym[s.sym]["total"] += 1
        if s.status == "TP":
            by_sym[s.sym]["wins"] += 1
        by_sym[s.sym]["rr_sum"] += s.act_rr or 0

    result_list = []
    for sym, d in by_sym.items():
        t = d["total"]
        w = d["wins"]
        result_list.append({
            "sym": sym,
            "total": t,
            "wins": w,
            "losses": t - w,
            "win_rate": round(w / t * 100, 1) if t > 0 else 0,
            "avg_rr": round(d["rr_sum"] / t, 2) if t > 0 else 0,
        })
    result_list.sort(key=lambda x: -x["win_rate"])
    return {"by_symbol": result_list}


@router.get("/by_quality")
async def performance_by_quality(session: AsyncSession = Depends(get_session)):
    """Performance breakdown by signal quality grade."""
    q = select(Signal).where(Signal.act_rr.isnot(None), Signal.status.in_(["TP", "SL"]))
    result = await session.execute(q)
    signals = result.scalars().all()

    by_q: dict = {}
    for s in signals:
        grade = s.quality or "?"
        if grade not in by_q:
            by_q[grade] = {"total": 0, "wins": 0, "rr_sum": 0}
        by_q[grade]["total"] += 1
        if s.status == "TP":
            by_q[grade]["wins"] += 1
        by_q[grade]["rr_sum"] += s.act_rr or 0

    result_list = []
    for grade, d in by_q.items():
        t = d["total"]
        w = d["wins"]
        result_list.append({
            "quality": grade,
            "total": t,
            "wins": w,
            "losses": t - w,
            "win_rate": round(w / t * 100, 1) if t > 0 else 0,
            "avg_rr": round(d["rr_sum"] / t, 2) if t > 0 else 0,
        })
    grade_order = {"A+": 0, "A": 1, "B+": 2, "WATCHLIST": 3}
    result_list.sort(key=lambda x: grade_order.get(x["quality"], 99))
    return {"by_quality": result_list}


@router.get("/equity_curve")
async def get_equity_curve(session: AsyncSession = Depends(get_session)):
    """Return equity curve data points."""
    q = select(Signal).where(
        Signal.act_rr.isnot(None),
        Signal.status.in_(["TP", "SL"]),
        Signal.closed_at.isnot(None),
    ).order_by(Signal.closed_at)
    result = await session.execute(q)
    signals = result.scalars().all()

    balance = 50.0
    risk_pct = 0.01
    equity = [{"date": "Start", "balance": balance, "trade": 0}]

    for s in signals:
        rr = s.act_rr or 0
        # Simplified P&L calc: risk 1% per trade * actual R:R
        pnl = round(balance * risk_pct * rr, 2)
        balance = round(balance + pnl, 2)
        equity.append({
            "date": s.closed_at.strftime("%Y-%m-%d %H:%M") if s.closed_at else "",
            "balance": balance,
            "trade": s.id,
            "sym": s.sym,
            "rr": rr,
            "status": s.status,
        })
    return {"equity_curve": equity, "final_balance": balance}


@router.get("/features")
async def get_feature_weights(session: AsyncSession = Depends(get_session)):
    """Return adaptive feature weights."""
    result = await session.execute(select(AdaptiveWeight).order_by(desc(AdaptiveWeight.weight)))
    weights = result.scalars().all()
    return {
        "features": [
            {
                "feature": w.feature,
                "weight": w.weight,
                "win_rate": w.win_rate,
                "sample_count": w.sample_count,
                "updated_at": w.updated_at.isoformat() if w.updated_at else None,
            }
            for w in weights
        ]
    }
