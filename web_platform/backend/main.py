"""
TITAN PRIME — FastAPI Backend
Institutional-grade trading platform API server.
"""
import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

from core.config import settings
from core.database import init_db, async_session_factory, Signal, AdaptiveWeight
from core.redis_client import get_redis, close_redis

from services.market_data import refresh_all_market_data, refresh_news, get_price_cache, get_candles
from services.signal_engine import score_setup, calc_sizing, set_adaptive_weights
from services.telegram_bot import send_signal_alert, send_outcome_alert, send_performance_report
from services.chart_service import generate_signal_chart

from websocket.manager import manager, broadcast_price_update, broadcast_new_signal, broadcast_trade_update

from api.signals import router as signals_router
from api.trades import router as trades_router
from api.journal import router as journal_router
from api.analytics import router as analytics_router
from api.market import router as market_router
from api.news import router as news_router

from sqlalchemy import select, desc

# ── Background task state ─────────────────────────────────────────────────────
_last_analysis: float = 0
_last_news_alert: set = set()


async def _load_adaptive_weights():
    """Load adaptive weights from DB into signal engine."""
    async with async_session_factory() as session:
        result = await session.execute(select(AdaptiveWeight))
        weights = result.scalars().all()
        set_adaptive_weights({w.feature: w.weight for w in weights})


async def _run_signal_analysis():
    """Run signal scoring for all symbols with available candles."""
    global _last_analysis
    from services.market_data import get_price_cache, get_candles, get_news_cache

    prices = get_price_cache()
    news = get_news_cache()

    from core.config import ALL_SYMBOLS
    new_signals = []

    for sym in ALL_SYMBOLS:
        try:
            candles = get_candles(sym)
            price_data = prices.get(sym, {})
            price = price_data.get("price")
            if not price or len(candles) < 40:
                continue

            # Get relevant news
            relevant_news = [n for n in news[:10] if sym in str(n.get("affected_assets", ""))]

            result = score_setup(sym, candles, price, news_items=relevant_news)
            if result is None:
                continue

            # Calculate sizing
            sizing = calc_sizing(sym, result["entry"], result["sl"], result["rr"])
            result["sizing"] = sizing

            # Check dedup — don't re-save if same signal is already open
            async with async_session_factory() as session:
                existing = await session.execute(
                    select(Signal).where(
                        Signal.sym == sym,
                        Signal.direction == result["direction"],
                        Signal.status == "OPEN",
                    ).order_by(desc(Signal.created_at)).limit(1)
                )
                existing_sig = existing.scalar_one_or_none()
                if existing_sig and (datetime.utcnow() - existing_sig.created_at).total_seconds() < 7200:
                    continue

                # Generate chart
                chart_path = None
                try:
                    chart_path = await generate_signal_chart(result, candles)
                except Exception:
                    pass

                # Save to DB
                sig = Signal(
                    sym=sym,
                    direction=result["direction"],
                    quality=result["quality"],
                    score=result["score"],
                    confidence=result["confidence"],
                    entry=result["entry"],
                    sl=result["sl"],
                    tp=result["tp"],
                    rr_t=result["rr"],
                    status=result["status"],
                    regime=result.get("regime"),
                    contrarian_score=result.get("contrarian_score", 0),
                    duration=result.get("duration"),
                    f_ema=result.get("f_ema", False),
                    f_rsi=result.get("f_rsi", False),
                    f_macd=result.get("f_macd", False),
                    f_sweep=result.get("f_sweep", False),
                    f_ob=result.get("f_ob", False),
                    f_fvg=result.get("f_fvg", False),
                    f_struct=result.get("f_struct", False),
                    narrative=result.get("narrative"),
                    chart_path=chart_path,
                    reasons=json.dumps(result.get("reasons", [])),
                    neg_factors=json.dumps(result.get("neg_factors", [])),
                    sm_notes=json.dumps(result.get("sm_notes", [])),
                    trap_warnings=json.dumps(result.get("trap_warnings", [])),
                    consensus_view=result.get("consensus_view"),
                    sm_view=result.get("sm_view"),
                    news_risk=result.get("news_risk"),
                    sizing=json.dumps(sizing),
                    hold_h=result.get("hold_h"),
                    created_at=datetime.utcnow(),
                )
                session.add(sig)
                await session.commit()
                await session.refresh(sig)
                result["id"] = sig.id

            # Broadcast to WebSocket clients
            signal_payload = {
                "id": sig.id,
                "sym": sym,
                "direction": result["direction"],
                "quality": result["quality"],
                "score": result["score"],
                "entry": result["entry"],
                "sl": result["sl"],
                "tp": result["tp"],
                "rr": result["rr"],
                "status": result["status"],
                "regime": result.get("regime"),
                "duration": result.get("duration"),
                "chart_path": chart_path,
            }
            new_signals.append(signal_payload)
            await broadcast_new_signal(signal_payload)

            # Send Telegram for approved signals
            if result["quality"] in ("A+", "A", "B+") and result["status"] == "APPROVED":
                try:
                    await send_signal_alert(result, chart_path)
                except Exception:
                    pass

        except Exception:
            pass

    _last_analysis = time.time()
    return new_signals


async def _monitor_open_positions():
    """Check open signals/trades for TP/SL hit."""
    from services.market_data import get_price_cache

    prices = get_price_cache()
    async with async_session_factory() as session:
        result = await session.execute(
            select(Signal).where(Signal.status == "OPEN")
        )
        open_signals = result.scalars().all()

        for sig in open_signals:
            price_data = prices.get(sig.sym, {})
            cur = price_data.get("price")
            if not cur:
                continue

            ep = sig.entry
            sl = sig.sl
            tp = sig.tp
            risk = abs(ep - sl) if ep and sl else 1

            new_status = None
            out_price = cur

            if sig.direction == "LONG":
                if cur <= sl:
                    new_status = "SL"
                elif cur >= tp:
                    new_status = "TP"
            else:
                if cur >= sl:
                    new_status = "SL"
                elif cur <= tp:
                    new_status = "TP"

            # Auto-expire after 7 days
            if not new_status and sig.created_at:
                age_days = (datetime.utcnow() - sig.created_at).days
                if age_days >= 7:
                    new_status = "EXPIRED"

            if new_status:
                if sig.direction == "LONG":
                    act_rr = round((out_price - ep) / risk, 2) if risk else 0
                else:
                    act_rr = round((ep - out_price) / risk, 2) if risk else 0

                sig.status = new_status
                sig.out_price = out_price
                sig.act_rr = act_rr
                sig.closed_at = datetime.utcnow()
                await session.commit()

                # Telegram outcome
                try:
                    await send_outcome_alert(sig.sym, sig.direction, new_status, ep, out_price, act_rr, sig.id)
                except Exception:
                    pass

                # Broadcast
                await broadcast_trade_update({
                    "signal_id": sig.id,
                    "sym": sig.sym,
                    "direction": sig.direction,
                    "status": new_status,
                    "out_price": out_price,
                    "act_rr": act_rr,
                })


async def _price_broadcast_loop():
    """Broadcast price updates to WebSocket clients every 5s."""
    while True:
        try:
            prices = get_price_cache()
            if prices and manager.connection_count > 0:
                compact = {
                    sym: {
                        "price": d.get("price"),
                        "change_pct": d.get("change_pct", 0),
                    }
                    for sym, d in prices.items()
                    if d.get("price")
                }
                await broadcast_price_update(compact)
        except Exception:
            pass
        await asyncio.sleep(5)


async def _market_refresh_loop():
    """Refresh market data every 30s."""
    while True:
        try:
            await refresh_all_market_data()
        except Exception:
            pass
        await asyncio.sleep(30)


async def _signal_analysis_loop():
    """Run signal analysis every 5 minutes."""
    # Initial delay to let market data load first
    await asyncio.sleep(35)
    while True:
        try:
            await _run_signal_analysis()
        except Exception:
            pass
        await asyncio.sleep(300)


async def _monitor_loop():
    """Monitor TP/SL every 30s."""
    await asyncio.sleep(60)
    while True:
        try:
            await _monitor_open_positions()
        except Exception:
            pass
        await asyncio.sleep(30)


async def _news_refresh_loop():
    """Refresh news every 10 minutes."""
    while True:
        try:
            await refresh_news()
        except Exception:
            pass
        await asyncio.sleep(600)


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    os.makedirs(settings.CHART_DIR, exist_ok=True)

    # Init DB
    await init_db()

    # Load adaptive weights
    await _load_adaptive_weights()

    # Start background tasks
    tasks = [
        asyncio.create_task(_market_refresh_loop()),
        asyncio.create_task(_signal_analysis_loop()),
        asyncio.create_task(_monitor_loop()),
        asyncio.create_task(_news_refresh_loop()),
        asyncio.create_task(_price_broadcast_loop()),
    ]

    # Initial data load
    asyncio.create_task(refresh_all_market_data())
    asyncio.create_task(refresh_news())

    yield

    # Shutdown
    for task in tasks:
        task.cancel()
    await close_redis()


# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Titan Prime Elite",
    description="Institutional-grade trading intelligence platform",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount chart images
os.makedirs(settings.CHART_DIR, exist_ok=True)
app.mount("/charts", StaticFiles(directory=settings.CHART_DIR), name="charts")

# Include routers
app.include_router(signals_router)
app.include_router(trades_router)
app.include_router(journal_router)
app.include_router(analytics_router)
app.include_router(market_router)
app.include_router(news_router)


# ── WebSocket endpoint ────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial snapshot on connect
        prices = get_price_cache()
        if prices:
            compact = {
                sym: {"price": d.get("price"), "change_pct": d.get("change_pct", 0)}
                for sym, d in prices.items()
                if d.get("price")
            }
            await manager.send_personal(websocket, {"type": "price_update", "data": compact})

        while True:
            data = await websocket.receive_text()
            # Echo back for ping/pong
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await manager.send_personal(websocket, {"type": "pong", "ts": time.time()})
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception:
        await manager.disconnect(websocket)


# ── Health endpoints ──────────────────────────────────────────────────────────
@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "service": "titan-prime-backend",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "ws_connections": manager.connection_count,
    }


@app.get("/")
async def root():
    return {
        "service": "Titan Prime Elite API",
        "docs": "/docs",
        "health": "/health",
    }
