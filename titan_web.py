#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TITAN PRIME ELITE — Web Dashboard
Tek komutla çalışan kurumsal trading terminali (Docker gerekmez).

ÇALIŞTIRMA:
    python titan_web.py
Sonra tarayıcıda aç:  http://localhost:8000

Mevcut titan_flow.py motorunu kullanır — aynı SQLite DB, aynı sinyal motoru,
aynı Telegram entegrasyonu. Sadece arayüz CLI yerine modern web panelidir.
"""

import sys, os, time, json, threading, sqlite3, subprocess
from datetime import datetime

# ── Otomatik bağımlılık kurulumu (fastapi + uvicorn) ──────────────────────────
def _ensure(pkg, pip_name=None):
    try:
        __import__(pkg)
    except ImportError:
        print(f"[kurulum] {pip_name or pkg} indiriliyor...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pip_name or pkg])

for _p, _pip in [("fastapi", "fastapi"), ("uvicorn", "uvicorn[standard]")]:
    _ensure(_p, _pip)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ── Trading motorunu import et ────────────────────────────────────────────────
import titan_flow as tf

HERE = os.path.dirname(os.path.abspath(__file__))

# ══════════════════════════════════════════════════════════════════════════════
# ARKA PLAN MOTORU — titan_flow döngülerini başlat
# ══════════════════════════════════════════════════════════════════════════════
_engine_started = False

def _analysis_loop():
    """Periyodik sinyal analizi (CLI'daki render() yerine)."""
    while True:
        try:
            tf.run_analysis()
        except Exception:
            pass
        time.sleep(20)

def start_engine():
    global _engine_started
    if _engine_started:
        return
    _engine_started = True
    threads = [
        tf.background_loop,         # piyasa verisi (yfinance + finnhub + haber)
        tf.cot_loop,                # COT verisi
        tf.monitor_loop,            # açık işlem TP/SL takibi
        tf.stats_loop,              # istatistik + adaptif öğrenme
        tf.portfolio_loop,          # portföy durumu
        tf.performance_report_loop, # günlük/haftalık Telegram raporu
        _analysis_loop,             # sinyal üretimi
    ]
    for fn in threads:
        threading.Thread(target=fn, daemon=True).start()
    # Açılışta hızlı kontrol
    try: tf._check_open()
    except Exception: pass
    try: tf.compute_stats()
    except Exception: pass

# ══════════════════════════════════════════════════════════════════════════════
# YARDIMCI — engine state → JSON
# ══════════════════════════════════════════════════════════════════════════════
def _fp(v):
    if v is None: return None
    try: return round(float(v), 5)
    except: return v

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

def _signal_to_dict(s):
    sz = s.get("sizing", {}) or {}
    return {
        "sym": s.get("sym"), "quality": s.get("quality"), "direction": s.get("direction"),
        "status": s.get("status"), "score": s.get("score"), "confidence": s.get("confidence"),
        "price": _fp(s.get("price")), "el": _fp(s.get("el")), "eh": _fp(s.get("eh")),
        "sl": _fp(s.get("sl")), "tp": _fp(s.get("tp")), "rr": s.get("rr"),
        "regime": s.get("regime"), "duration": s.get("duration"),
        "contrarian_score": s.get("contrarian_score"), "contrarian_label": s.get("contrarian_label"),
        "news_risk": s.get("news_risk"), "hold_h": s.get("hold_h"),
        "consensus_view": s.get("consensus_view"), "sm_view": s.get("sm_view"),
        "reasons": s.get("reasons", [])[:6], "neg_factors": s.get("neg_factors", [])[:4],
        "sm_notes": s.get("sm_notes", [])[:4], "trap_warnings": s.get("trap_warnings", [])[:3],
        "narrative": s.get("narrative"), "time": s.get("time"),
        "margin": sz.get("margin"), "exp_loss": sz.get("exp_loss"),
        "exp_profit": sz.get("exp_profit"), "leverage": sz.get("leverage"),
        "risk_pct": sz.get("risk_pct"),
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
        # ilerleme: girişten TP'ye % kaç gidildi
        tp_dist = abs(tp - ep) if (tp and ep) else 1
        prog = max(0, min(100, round(pnl_pts / tp_dist * 100, 1))) if tp_dist else 0
        out.append({
            "sym": sym, "direction": direction, "entry": _fp(ep), "current": _fp(cp),
            "sl": _fp(sl), "tp": _fp(tp), "rr_live": rr_live, "pnl_gbp": pnl_gbp,
            "progress": prog, "margin": sz.get("margin"), "exp_loss": exp_loss,
            "id": t.get("db_id", 0),
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
        t = d.get("t", 0); w = d.get("w", 0)
        sess[s] = {"wr": round(w / t * 100, 1) if t else 0, "t": t}
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

def news_data():
    with tf.lock:
        arts = list(tf.analyzed_news)
    out = []
    for a in arts[:25]:
        m = a.get("macro", {}) or {}
        out.append({
            "headline": a.get("headline"), "importance": a.get("importance", 0),
            "risk_level": a.get("risk_level"), "bias": m.get("bias_tr"),
            "bull_pct": m.get("bull_pct", 0), "bear_pct": m.get("bear_pct", 0),
            "neut_pct": m.get("neut_pct", 0), "conf": m.get("conf", 0),
            "dur": m.get("dur"), "caution": m.get("caution"),
            "datetime": a.get("datetime", 0),
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
    # seans
    h = datetime.utcnow().hour
    if h < 7: sess = "ASYA"
    elif h < 12: sess = "LONDRA"
    elif h < 16: sess = "LONDRA + NEW YORK"
    elif h < 21: sess = "NEW YORK"
    else: sess = "ASYA"
    return {"regime": regime, "session": sess, "counts": counts}

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

@app.get("/api/snapshot")
def api_snapshot():
    return JSONResponse({
        "market": market_snapshot(),
        "regime": regime_now(),
        "analytics": analytics_data(),
        "active_trades": active_trades_live(),
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

@app.get("/api/news")
def api_news():
    return JSONResponse(news_data())

@app.post("/api/enter/{key}")
def api_enter(key: str):
    try:
        ok = tf.enter_trade(key)
        return {"ok": bool(ok)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ── WebSocket — canlı yayın ───────────────────────────────────────────────────
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
    print("  TITAN PRIME ELITE — Web Dashboard")
    print("=" * 60)
    print("  Tarayicida ac:  http://localhost:8000")
    print("  Kapatmak icin:  CTRL + C")
    print("=" * 60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")

if __name__ == "__main__":
    main()
