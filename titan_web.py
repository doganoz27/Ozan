#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TITAN PRIME ELITE — Institutional Web Dashboard
Tek komutla çalışan kurumsal trading terminali (Docker gerekmez).

ÇALIŞTIRMA:
    python titan_web.py
Sonra tarayıcıda aç:  http://localhost:8000
"""

import sys, os, time, json, threading, sqlite3, subprocess, io, math
from datetime import datetime, timedelta
from collections import defaultdict

# ── Otomatik bağımlılık kurulumu ──────────────────────────────────────────────
def _ensure(pkg, pip_name=None):
    try:
        __import__(pkg)
    except ImportError:
        print(f"[kurulum] {pip_name or pkg} indiriliyor...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pip_name or pkg])

for _p, _pip in [("fastapi", "fastapi"), ("uvicorn", "uvicorn[standard]")]:
    _ensure(_p, _pip)

# Optional chart dependencies
_CHART_OK = False
try:
    import mplfinance as mpf
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    _CHART_OK = True
except ImportError:
    pass

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

import titan_flow as tf

HERE = os.path.dirname(os.path.abspath(__file__))
_START_TIME = time.time()
_last_tick = {"t": 0}
_feed_errors = {"n": 0}
_system_log: list = []  # max 200 entries

def _log(msg: str, level: str = "INFO"):
    entry = {"t": datetime.utcnow().strftime("%H:%M:%S"), "level": level, "msg": msg}
    _system_log.append(entry)
    if len(_system_log) > 200:
        _system_log.pop(0)

# ══════════════════════════════════════════════════════════════════════════════
# ARKA PLAN MOTORU
# ══════════════════════════════════════════════════════════════════════════════
_engine_started = False

def _analysis_loop():
    while True:
        try:
            tf.run_analysis()
            _last_tick["t"] = time.time()
            _log(f"Sinyal analizi tamamlandı — {len(tf.setups)} setup")
        except Exception as e:
            _feed_errors["n"] += 1
            _log(f"Analiz hatası: {e}", "ERROR")
        time.sleep(20)

def start_engine():
    global _engine_started
    if _engine_started:
        return
    _engine_started = True
    threads = [
        tf.background_loop,
        tf.cot_loop,
        tf.monitor_loop,
        tf.stats_loop,
        tf.portfolio_loop,
        tf.performance_report_loop,
        _analysis_loop,
    ]
    for fn in threads:
        threading.Thread(target=fn, daemon=True).start()
    try:
        tf._check_open()
    except Exception:
        pass
    try:
        tf.compute_stats()
    except Exception:
        pass
    _log("Titan Prime Elite başlatıldı")

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _fp(v):
    if v is None:
        return None
    try:
        return round(float(v), 5)
    except Exception:
        return v

def _session_now() -> str:
    h = datetime.utcnow().hour
    if h < 7:
        return "ASYA"
    elif h < 12:
        return "LONDRA"
    elif h < 16:
        return "LONDRA+NY"
    elif h < 21:
        return "NEW YORK"
    return "ASYA"

def _grade_from_score(score: float) -> str:
    if score >= 95:
        return "A+"
    elif score >= 90:
        return "A"
    elif score >= 80:
        return "B+"
    elif score >= 70:
        return "B"
    elif score >= 60:
        return "WATCH"
    return "REJECT"

def _derive_ai_scores(s: dict) -> dict:
    """Derive individual AI component scores (0-100) from signal dict."""
    reasons  = " ".join(s.get("reasons", []) or []).upper()
    sm_notes = " ".join(s.get("sm_notes", []) or []).upper()
    neg      = " ".join(s.get("neg_factors", []) or []).upper()
    base     = (s.get("score") or 50) / 100

    def kw(*words): return any(w in reasons or w in sm_notes for w in words)

    trend     = min(100, max(0, round(50 + (base - 0.5) * 80
                + (15 if kw("EMA","TREND","YÜKSEL","DÜŞÜŞ") else 0)
                + (-10 if "KARŞI" in neg else 0))))
    structure = min(100, max(0, round(45 + (base - 0.5) * 60
                + (20 if kw("BOS","CHOCH","KIRILIM","YAPI") else 0)
                + (10 if kw("DESTEK","DİRENÇ","KIRILDI") else 0))))
    liquidity = min(100, max(0, round(40 + (base - 0.5) * 70
                + (20 if kw("LİKİDİTE","SWEEP","OB","BLOK","FVG") else 0))))
    volume    = min(100, max(0, round(50 + (base - 0.5) * 50
                + (15 if kw("HACİM","VOLUME") else 0))))
    momentum  = min(100, max(0, round(45 + (base - 0.5) * 70
                + (15 if kw("RSI","MACD","MOMENTUM") else 0)
                + (-10 if "AŞIRI ALIM" in neg or "AŞIRI SATIM" in neg else 0))))
    session_s = _session_now()
    sess_score = {"LONDRA+NY": 85, "NEW YORK": 80, "LONDRA": 75, "ASYA": 55}.get(session_s, 65)
    news_imp   = min(100, max(0, round(50 + (1 - (s.get("news_risk") or 0.5)) * 50)))
    risk_score = min(100, max(0, round(100 - (s.get("contrarian_score") or 50) * 0.6)))

    return {
        "Trend":     trend,
        "Yapı":      structure,
        "Likidite":  liquidity,
        "Hacim":     volume,
        "Momentum":  momentum,
        "Seans":     sess_score,
        "Haber":     news_imp,
        "Risk":      risk_score,
    }

# ══════════════════════════════════════════════════════════════════════════════
# DATA FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════
def market_snapshot():
    out = []
    groups = [
        ("FOREX MAJORS",  ["EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD","USD/CAD","NZD/USD"]),
        ("FOREX CROSSES", ["EUR/GBP","EUR/JPY","GBP/JPY","EUR/CHF","AUD/JPY","GBP/CHF"]),
        ("METALS & OIL",  ["XAU/USD","XAG/USD","WTI","BRENT"]),
        ("EQUITIES",      tf.EQ_SYMBOLS),
    ]
    with tf.lock:
        snap = dict(tf.market)
    for label, syms in groups:
        for sym in syms:
            md = snap.get(sym)
            if not md or md.price is None:
                continue
            out.append({
                "group": label, "sym": sym,
                "price": _fp(md.price), "chg": round(md.chg, 3),
                "high": _fp(md.high), "low": _fp(md.low),
                "updated": md.updated,
            })
    return out

def heatmap_data():
    """Return heatmap grid — change% sorted by absolute movement."""
    with tf.lock:
        snap = dict(tf.market)
    cells = []
    for sym, md in snap.items():
        if md and md.price is not None:
            chg = md.chg or 0
            intensity = min(1.0, abs(chg) / 3.0)  # 3% = max intensity
            cells.append({"sym": sym, "chg": round(chg, 3), "price": _fp(md.price), "intensity": round(intensity, 3)})
    cells.sort(key=lambda x: -abs(x["chg"]))
    return cells

def _signal_to_dict(s):
    sz = s.get("sizing", {}) or {}
    try:
        ai = tf.ai_decision_scores(s)
    except Exception:
        ai = _derive_ai_scores(s)
    # session from signal time or now
    st_time = s.get("time", "")
    try:
        hour = int(str(st_time).split(":")[0]) if st_time and ":" in str(st_time) else datetime.utcnow().hour
        if hour < 7: sig_sess = "ASYA"
        elif hour < 12: sig_sess = "LONDRA"
        elif hour < 16: sig_sess = "LONDRA+NY"
        elif hour < 21: sig_sess = "NEW YORK"
        else: sig_sess = "ASYA"
    except Exception:
        sig_sess = _session_now()

    score = s.get("score") or 0
    # probability = weighted from score + confidence
    conf = (s.get("confidence") or 0)
    prob = round(min(99, max(1, score * 0.6 + conf * 0.4)), 0)

    # strategy used (derived from regime + reasons text)
    reasons_txt = " ".join(s.get("reasons", []) or []).upper()
    if "ICT" in reasons_txt or "OB" in reasons_txt or "FVG" in reasons_txt:
        strategy = "ICT/SMC"
    elif "TREND" in reasons_txt and "EMA" in reasons_txt:
        strategy = "Trend Takip"
    elif "KONTRARİAN" in reasons_txt or (s.get("contrarian_score", 0) or 0) > 70:
        strategy = "Kontrarian"
    else:
        strategy = "Yapı Kırılımı"

    # liquidity target
    flags = s.get("flags", {}) or {}
    liq_target = flags.get("liq_target") or flags.get("liquidity_target") or None

    return {
        "sym": s.get("sym"), "quality": s.get("quality"), "direction": s.get("direction"),
        "status": s.get("status"), "score": s.get("score"), "confidence": s.get("confidence"),
        "price": _fp(s.get("price")), "el": _fp(s.get("el")), "eh": _fp(s.get("eh")),
        "sl": _fp(s.get("sl")), "tp": _fp(s.get("tp")), "rr": s.get("rr"),
        "regime": s.get("regime"), "duration": s.get("duration"),
        "contrarian_score": s.get("contrarian_score"), "contrarian_label": s.get("contrarian_label"),
        "news_risk": s.get("news_risk"), "hold_h": s.get("hold_h"),
        "consensus_view": s.get("consensus_view"), "sm_view": s.get("sm_view"),
        "reasons": s.get("reasons", [])[:8], "neg_factors": s.get("neg_factors", [])[:4],
        "sm_notes": s.get("sm_notes", [])[:6], "trap_warnings": s.get("trap_warnings", [])[:3],
        "narrative": s.get("narrative"), "time": s.get("time"),
        "margin": sz.get("margin"), "exp_loss": sz.get("exp_loss"),
        "exp_profit": sz.get("exp_profit"), "leverage": sz.get("leverage"),
        "risk_pct": sz.get("risk_pct"), "notional": sz.get("notional"),
        "ai_scores": ai,
        "session": sig_sess,
        "probability": prob,
        "strategy": strategy,
        "liq_target": _fp(liq_target),
    }

def live_signals():
    with tf.lock:
        sts = list(tf.setups)
    return [_signal_to_dict(s) for s in sts]

def active_trades_live():
    out = []
    with tf.lock:
        at = dict(tf.active_trades)
        snap = dict(tf.market)
    for k, t in at.items():
        sym = t.get("sym"); ep = t.get("_trade_entry_price") or t.get("price")
        sl = t.get("sl"); tp = t.get("tp"); direction = t.get("direction")
        md = snap.get(sym); cp = md.price if md else ep
        risk = abs(ep - sl) if (ep and sl) else 1
        pnl_pts = (cp - ep) if direction == "LONG" else (ep - cp)
        rr_live = round(pnl_pts / risk, 2) if risk else 0
        sz = t.get("sizing", {}) or {}
        exp_loss = sz.get("exp_loss", 0) or 0
        pnl_gbp = round(rr_live * exp_loss, 2)
        tp_dist = abs(tp - ep) if (tp and ep) else 1
        prog = max(0, min(100, round(pnl_pts / tp_dist * 100, 1))) if tp_dist else 0
        out.append({
            "sym": sym, "direction": direction, "entry": _fp(ep), "current": _fp(cp),
            "sl": _fp(sl), "tp": _fp(tp), "rr_live": rr_live, "pnl_gbp": pnl_gbp,
            "progress": prog, "margin": sz.get("margin"), "exp_loss": exp_loss,
            "id": t.get("db_id", 0), "score": t.get("score", 0),
            "quality": t.get("quality", ""),
        })
    return out

def journal_data():
    try:
        with tf.db() as c:
            tp = c.execute("SELECT id,sym,quality,direction,entry,out_price,act_rr,rr_t,created,out_at FROM signals WHERE status='TP' ORDER BY id DESC LIMIT 40").fetchall()
            sl = c.execute("SELECT id,sym,quality,direction,entry,out_price,act_rr,rr_t,created,out_at FROM signals WHERE status='SL' ORDER BY id DESC LIMIT 40").fetchall()
            op = c.execute("SELECT id,sym,quality,direction,entry,sl,tp,rr_t,score,created FROM signals WHERE status='OPEN' ORDER BY id DESC LIMIT 30").fetchall()
    except Exception:
        tp = sl = op = []
    def row(r): return {k: r[k] for k in r.keys()}
    return {"tp": [row(r) for r in tp], "sl": [row(r) for r in sl], "open": [row(r) for r in op]}

def analytics_data():
    with tf.lock:
        st = dict(tf.stats_cache)
        weights = dict(tf.adap_weights)
        ps = dict(tf.portfolio_state)
    best = st.get("best_syms", [])
    best_out = [{"sym": s, "wr": d.get("wr", 0), "avg_rr": d.get("avg_rr", 0), "t": d.get("t", 0)} for s, d in best]
    by_q = {}
    for q, d in (st.get("by_q", {}) or {}).items():
        by_q[q] = {"wr": d.get("wr", 0), "w": d.get("w", 0), "t": d.get("t", 0)}
    sess = {}
    for s, d in (st.get("sess_stats", {}) or {}).items():
        t2 = d.get("t", 0); w = d.get("w", 0)
        sess[s] = {"wr": round(w / t2 * 100, 1) if t2 else 0, "t": t2}
    return {
        "total": st.get("total", 0), "wins": st.get("wins", 0), "losses": st.get("losses", 0),
        "wr": st.get("wr", 0), "avg_rr": st.get("avg_rr", 0), "pf": st.get("pf", 0),
        "sharpe": st.get("sharpe", 0), "sortino": st.get("sortino", 0), "calmar": st.get("calmar"),
        "mdd": st.get("mdd", 0), "kelly": st.get("kelly", 0), "var95": st.get("var95", 0),
        "avg_win": st.get("avg_win", 0), "avg_loss": st.get("avg_loss", 0),
        "streak": st.get("streak", 0), "streak_type": st.get("streak_type", ""),
        "expired": st.get("expired", 0),
        "equity_curve": st.get("equity_curve", []),
        "best_syms": best_out, "by_quality": by_q, "sessions": sess,
        "weights": {k: round(v, 3) for k, v in weights.items()},
        "balance": round(ps.get("shadow_balance", tf.ACCOUNT["balance"]), 2),
        "start_balance": tf.ACCOUNT["balance"],
        "heat": ps.get("heat", 0), "inst_risk": ps.get("inst_risk_score", 100),
        "mc": st.get("mc"),
    }

def performance_data(period: str = "daily"):
    """Pull aggregated performance from DB by day/week/month."""
    try:
        with tf.db() as c:
            rows = c.execute(
                "SELECT date(out_at) as d, status, act_rr FROM signals "
                "WHERE status IN ('TP','SL') AND out_at IS NOT NULL "
                "ORDER BY out_at DESC LIMIT 500"
            ).fetchall()
    except Exception:
        rows = []

    buckets: dict = defaultdict(lambda: {"wins": 0, "losses": 0, "rr_sum": 0.0})
    today = datetime.utcnow().date()

    for r in rows:
        raw_d = r[0]
        if not raw_d:
            continue
        try:
            d = datetime.strptime(raw_d, "%Y-%m-%d").date()
        except Exception:
            continue

        if period == "daily":
            key = str(d)
        elif period == "weekly":
            # ISO week start (Monday)
            key = str(d - timedelta(days=d.weekday()))
        else:  # monthly
            key = f"{d.year}-{d.month:02d}"

        status = r[1]; rr = r[2] or 0
        if status == "TP":
            buckets[key]["wins"] += 1
        else:
            buckets[key]["losses"] += 1
        buckets[key]["rr_sum"] += rr

    out = []
    for k in sorted(buckets.keys(), reverse=True)[:30]:
        b = buckets[k]
        t = b["wins"] + b["losses"]
        out.append({
            "period": k,
            "wins": b["wins"], "losses": b["losses"], "total": t,
            "wr": round(b["wins"] / t * 100, 1) if t else 0,
            "rr_sum": round(b["rr_sum"], 2),
        })
    return out

def news_data():
    with tf.lock:
        arts = list(tf.analyzed_news)
    out = []
    for a in arts[:25]:
        m = a.get("macro", {}) or {}
        impacts = a.get("asset_impacts", {}) or {}
        ne_anlamaliyiz = a.get("ne_anlamaliyiz", "") or ""
        out.append({
            "headline": a.get("headline"), "importance": a.get("importance", 0),
            "risk_level": a.get("risk_level"), "bias": m.get("bias_tr"),
            "bull_pct": m.get("bull_pct", 0), "bear_pct": m.get("bear_pct", 0),
            "neut_pct": m.get("neut_pct", 0), "conf": m.get("conf", 0),
            "dur": m.get("dur"), "caution": m.get("caution"),
            "datetime": a.get("datetime", 0),
            "impacts": impacts,
            "ne_anlamaliyiz": ne_anlamaliyiz,
        })
    out.sort(key=lambda x: -x.get("importance", 0))
    return out

def regime_now():
    sts = live_signals()
    counts = {}
    for s in sts:
        r = s.get("regime") or "Nötr"
        counts[r] = counts.get(r, 0) + 1
    regime = max(counts, key=counts.get) if counts else "Nötr"
    h = datetime.utcnow().hour
    if h < 7: sess = "ASYA"
    elif h < 12: sess = "LONDRA"
    elif h < 16: sess = "LONDRA + NEW YORK"
    elif h < 21: sess = "NEW YORK"
    else: sess = "ASYA"
    return {"regime": regime, "session": sess, "counts": counts}

def health_data():
    uptime_s = int(time.time() - _START_TIME)
    h, m = divmod(uptime_s // 60, 60)
    last_s = time.time() - _last_tick["t"] if _last_tick["t"] else 9999
    db_ok = False
    try:
        with tf.db() as c:
            c.execute("SELECT 1").fetchone()
        db_ok = True
    except Exception:
        pass
    with tf.lock:
        mkt_count = sum(1 for md in tf.market.values() if md and md.price)
        sig_count  = len(tf.setups)
        trade_count = len(tf.active_trades)

    return {
        "uptime": f"{h}s {m}d",
        "db": db_ok,
        "market_feeds": mkt_count,
        "signals": sig_count,
        "active_trades": trade_count,
        "last_analysis_ago": round(last_s, 0),
        "feed_errors": _feed_errors["n"],
        "chart_engine": _CHART_OK,
        "log": _system_log[-20:],
    }

# ══════════════════════════════════════════════════════════════════════════════
# CHART GENERATION (mplfinance — optional)
# ══════════════════════════════════════════════════════════════════════════════
def generate_chart_png(sym: str) -> bytes | None:
    if not _CHART_OK:
        return None
    try:
        import yfinance as yf
        import pandas as pd
        ticker_map = {
            "EUR/USD": "EURUSD=X", "GBP/USD": "GBPUSD=X", "USD/JPY": "JPY=X",
            "XAU/USD": "GC=F", "XAG/USD": "SI=F", "WTI": "CL=F", "BRENT": "BZ=F",
        }
        ticker = ticker_map.get(sym, sym.replace("/", "") + "=X")
        df = yf.Ticker(ticker).history(period="5d", interval="15m")
        if df.empty:
            return None
        df.index = df.index.tz_localize(None) if df.index.tzinfo else df.index
        mc = mpf.make_marketcolors(up="#00ff88", down="#ff3b5c", wick="inherit",
                                    edge="inherit", volume="#3b82f6")
        s = mpf.make_mpf_style(marketcolors=mc, facecolor="#0a0b0e", edgecolor="#1e2028",
                                figcolor="#0a0b0e", gridcolor="#1e2028",
                                rc={"axes.labelcolor": "#6b7280", "xtick.color": "#6b7280",
                                    "ytick.color": "#6b7280"})
        buf = io.BytesIO()
        mpf.plot(df.tail(60), type="candle", style=s, title=f" {sym}",
                 volume=True, savefig=dict(fname=buf, dpi=120, bbox_inches="tight"))
        buf.seek(0)
        return buf.read()
    except Exception as e:
        _log(f"Chart hatası {sym}: {e}", "WARN")
        return None

# ══════════════════════════════════════════════════════════════════════════════
# FASTAPI
# ══════════════════════════════════════════════════════════════════════════════
app = FastAPI(title="Titan Prime Elite")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def _startup():
    start_engine()

@app.get("/", response_class=HTMLResponse)
def index():
    path = os.path.join(HERE, "titan_dashboard.html")
    with open(path, encoding="utf-8") as f:
        return f.read()

@app.get("/health")
def api_health():
    return JSONResponse(health_data())

@app.get("/api/snapshot")
def api_snapshot():
    return JSONResponse({
        "market": market_snapshot(),
        "regime": regime_now(),
        "analytics": analytics_data(),
        "active_trades": active_trades_live(),
        "heatmap": heatmap_data(),
    })

@app.get("/api/signals")
def api_signals():
    return JSONResponse(live_signals())

@app.get("/api/trades")
def api_trades():
    return JSONResponse(active_trades_live())

@app.get("/api/journal")
def api_journal():
    return JSONResponse(journal_data())

@app.get("/api/analytics")
def api_analytics():
    return JSONResponse(analytics_data())

@app.get("/api/performance")
def api_performance(p: str = "daily"):
    if p not in ("daily", "weekly", "monthly"):
        p = "daily"
    return JSONResponse(performance_data(p))

@app.get("/api/news")
def api_news():
    return JSONResponse(news_data())

@app.get("/api/heatmap")
def api_heatmap():
    return JSONResponse(heatmap_data())

@app.get("/api/health")
def api_health2():
    return JSONResponse(health_data())

@app.get("/api/chart/{sym:path}")
def api_chart(sym: str):
    png = generate_chart_png(sym)
    if png:
        return Response(content=png, media_type="image/png")
    return JSONResponse({"error": "chart üretilemedi"}, status_code=503)

@app.post("/api/enter/{key}")
def api_enter(key: str):
    try:
        ok = tf.enter_trade(key)
        return {"ok": bool(ok)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ── WebSocket ─────────────────────────────────────────────────────────────────
class WSManager:
    def __init__(self):
        self.active = []
    async def connect(self, ws):
        await ws.accept(); self.active.append(ws)
    def disconnect(self, ws):
        if ws in self.active: self.active.remove(ws)

manager = WSManager()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    import asyncio
    await manager.connect(ws)
    try:
        while True:
            payload = {
                "type": "tick",
                "market": market_snapshot(),
                "regime": regime_now(),
                "signals": live_signals(),
                "active_trades": active_trades_live(),
                "analytics": analytics_data(),
                "heatmap": heatmap_data(),
                "news": news_data()[:10],
                "health": {
                    "last_analysis_ago": round(time.time() - _last_tick["t"], 0) if _last_tick["t"] else None,
                    "feed_errors": _feed_errors["n"],
                    "db": True,
                },
            }
            await ws.send_text(json.dumps(payload, default=str))
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)

# ══════════════════════════════════════════════════════════════════════════════
def main():
    start_engine()
    print("\n" + "=" * 60)
    print("  TITAN PRIME ELITE — Institutional Trading Terminal")
    print("=" * 60)
    print("  Tarayicida ac:  http://localhost:8000")
    print("  Health check:   http://localhost:8000/health")
    print(f"  Chart engine:   {'✓ mplfinance' if _CHART_OK else '✗ pip install mplfinance'}")
    print("  Kapatmak icin:  CTRL + C")
    print("=" * 60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")

if __name__ == "__main__":
    main()
