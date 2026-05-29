#!/usr/bin/env python3
"""
TITAN FLOW — Institutional-Grade Live Trading Intelligence Terminal
WebSocket streaming · Multi-timeframe TA · News Sentiment · ICT/SMC Patterns
"""

import websocket
import requests
import json
import threading
import time
import sys
import math
import re
import sqlite3
import os
from datetime import datetime, timezone
from collections import deque

# ── dependency check ──────────────────────────────────────────────────────────
MISSING = []
try:
    import numpy as np
except ImportError:
    MISSING.append("numpy")
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich.live import Live
    from rich import box
    from rich.align import Align
except ImportError:
    MISSING.append("rich")

if MISSING:
    print(f"[!] Missing: pip install {' '.join(MISSING)}")
    sys.exit(1)

# ── config ────────────────────────────────────────────────────────────────────
API_KEY  = "d8ce4jpr01qidic7ibt0d8ce4jpr01qidic7ibtg"
BASE_URL = "https://finnhub.io/api/v1"
WS_URL   = f"wss://ws.finnhub.io?token={API_KEY}"

CRYPTO_SYMBOLS = [
    "BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT",
    "BINANCE:BNBUSDT",  "BINANCE:XRPUSDT", "BINANCE:ADAUSDT",
]
FOREX_SYMBOLS = [
    "OANDA:EUR_USD","OANDA:GBP_USD","OANDA:USD_JPY","OANDA:USD_CHF",
    "OANDA:AUD_USD","OANDA:USD_CAD","OANDA:NZD_USD",
    "OANDA:EUR_GBP","OANDA:EUR_JPY","OANDA:GBP_JPY",
    "OANDA:EUR_CHF","OANDA:AUD_JPY","OANDA:GBP_CHF",
    "OANDA:XAU_USD","OANDA:XAG_USD",
    "OANDA:BCO_USD","OANDA:WTICO_USD",
]
EQUITY_SYMBOLS = ["NVDA","AAPL","SPY","QQQ","MSFT","TSLA","AMD","META"]

ALL_SUBSCRIBE  = CRYPTO_SYMBOLS + FOREX_SYMBOLS
CANDLE_EQUITY  = EQUITY_SYMBOLS
CANDLE_CRYPTO  = ["BINANCE:BTCUSDT","BINANCE:ETHUSDT","BINANCE:SOLUSDT","BINANCE:XRPUSDT"]
CANDLE_HISTORY    = 200
REFRESH_RATE      = 1.5
ANALYSIS_INTERVAL = 30
DB_PATH           = os.path.join(os.path.dirname(os.path.abspath(__file__)), "titan_journal.db")

# Minimum samples before adaptive weights kick in
ADAPTIVE_MIN_TRADES = 20
# Signal dedup window: don't re-log same symbol+direction within N seconds
SIGNAL_DEDUP_SECS = 7200   # 2 hours

console = Console()

# ── COT (Commitment of Traders) config ───────────────────────────────────────
# CFTC Socrata public API — no key required
CFTC_API = "https://publicreporting.cftc.gov/resource/jun7-fc8e.json"

# Maps our symbols to CFTC market/contract names (legacy COT report)
COT_MARKET_MAP = {
    "BINANCE:BTCUSDT":  "BITCOIN - CHICAGO MERCANTILE EXCHANGE",
    "OANDA:EUR_USD":    "EURO FX - CHICAGO MERCANTILE EXCHANGE",
    "OANDA:GBP_USD":    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE",
    "OANDA:USD_JPY":    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    "OANDA:USD_CHF":    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
    "OANDA:AUD_USD":    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "OANDA:USD_CAD":    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "OANDA:NZD_USD":    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "OANDA:XAU_USD":    "GOLD - COMMODITY EXCHANGE INC.",
    "OANDA:XAG_USD":    "SILVER - COMMODITY EXCHANGE INC.",
    "OANDA:WTICO_USD":  "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
    "OANDA:BCO_USD":    "BRENT LAST DAY FINANCIAL - ICE FUTURES EUROPE",
    "SPY":              "S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
    "QQQ":              "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE",
}

# Global COT cache: symbol → latest COT snapshot dict
cot_cache: dict[str, dict] = {}

# ── news sentiment keywords ───────────────────────────────────────────────────
BULLISH_WORDS = [
    "surge","rally","soar","gain","jump","rise","breakout","bullish","upside",
    "higher","strong","beat","exceed","upgrade","buy","inflow","record","growth",
    "approval","partnership","deal","expansion","positive","recovery","rebound",
    "all-time high","ath","accumulate","institutional","etf approved","rate cut",
    "fed pivot","dovish","stimulus","earnings beat","revenue growth","profit",
]
BEARISH_WORDS = [
    "drop","fall","crash","plunge","decline","selloff","bearish","downside",
    "lower","weak","miss","downgrade","sell","outflow","ban","hack","exploit",
    "lawsuit","regulation","fine","investigation","rate hike","hawkish",
    "inflation","recession","layoff","bankruptcy","default","war","sanctions",
    "tariff","debt","yield spike","risk-off","panic","liquidation","loss",
]

# symbol → keywords for news matching
SYMBOL_KEYWORDS = {
    "BINANCE:BTCUSDT":  ["bitcoin","btc","crypto","digital asset","satoshi","lightning","etf bitcoin","spot btc"],
    "BINANCE:ETHUSDT":  ["ethereum","eth","ether","defi","eip","layer2","l2"],
    "BINANCE:SOLUSDT":  ["solana","sol","solana network"],
    "BINANCE:BNBUSDT":  ["binance","bnb","bnb chain"],
    "BINANCE:XRPUSDT":  ["ripple","xrp","sec ripple"],
    "BINANCE:ADAUSDT":  ["cardano","ada","hoskinson"],
    "OANDA:XAU_USD":    ["gold","xau","bullion","fed","inflation","safe haven","dxy"],
    "OANDA:XAG_USD":    ["silver","xag","precious metal"],
    "OANDA:BCO_USD":    ["brent","crude oil","opec","oil price","energy"],
    "OANDA:WTICO_USD":  ["wti","crude","oil","opec","petroleum","barrel"],
    "OANDA:EUR_USD":    ["euro","eur","ecb","eurozone","european central bank","lagarde"],
    "OANDA:GBP_USD":    ["pound","gbp","boe","bank of england","uk inflation","sterling"],
    "OANDA:USD_JPY":    ["yen","jpy","boj","bank of japan","ueda","japan"],
    "OANDA:USD_CHF":    ["franc","chf","snb","swiss","switzerland"],
    "OANDA:AUD_USD":    ["aussie","aud","rba","australia","reserve bank of australia"],
    "OANDA:USD_CAD":    ["loonie","cad","boc","canada","bank of canada"],
    "OANDA:NZD_USD":    ["kiwi","nzd","rbnz","new zealand"],
    "OANDA:EUR_GBP":    ["euro","pound","ecb","boe"],
    "OANDA:EUR_JPY":    ["euro","yen","ecb","boj"],
    "OANDA:GBP_JPY":    ["pound","yen","boe","boj"],
    "OANDA:EUR_CHF":    ["euro","franc","ecb","snb"],
    "OANDA:AUD_JPY":    ["aussie","yen","rba","boj","risk appetite","risk-on","risk-off"],
    "OANDA:GBP_CHF":    ["pound","franc","boe","snb"],
    "NVDA":  ["nvidia","nvda","gpu","ai chip","cuda","blackwell","jensen huang","data center"],
    "AAPL":  ["apple","aapl","iphone","ios","tim cook","app store","mac"],
    "SPY":   ["s&p 500","sp500","spx","equity market","stock market","fed","interest rate"],
    "QQQ":   ["nasdaq","qqq","tech stocks","technology","big tech"],
    "MSFT":  ["microsoft","msft","azure","openai","copilot","teams","windows"],
    "TSLA":  ["tesla","tsla","elon","musk","electric vehicle","ev","cybertruck"],
    "AMD":   ["amd","ryzen","epyc","radeon","lisa su","advanced micro"],
    "META":  ["meta","facebook","instagram","zuckerberg","whatsapp","threads","reality labs"],
}

# ── data store ────────────────────────────────────────────────────────────────
class MarketData:
    def __init__(self, symbol):
        self.symbol  = symbol
        self.price   = None
        self.prev    = None
        self.high    = None
        self.low     = None
        self.open_   = None
        self.volume  = 0.0
        self.trades  = 0
        self.ticks   = deque(maxlen=500)
        self.candles = []
        self.last_ws = None

    @property
    def change_pct(self):
        if self.price and self.prev and self.prev != 0:
            return (self.price - self.prev) / self.prev * 100
        return 0.0

    @property
    def short_name(self):
        s = self.symbol
        if "BINANCE:" in s: return s.replace("BINANCE:","")
        if "OANDA:"   in s: return s.replace("OANDA:","").replace("_","/")
        return s

# Global state
market_data: dict[str, MarketData] = {}
news_cache       = []
categorised_news: dict[str, list] = {}
setup_alerts     = []
lock             = threading.Lock()
ws_connected     = False
last_analysis_time = 0

# Journal / adaptive learning state
journal_stats    = {}   # computed stats dict (refreshed periodically)
adaptive_weights = {}   # feature → weight multiplier (1.0 = neutral)
last_stats_time  = 0
STATS_INTERVAL   = 300  # recompute stats every 5 min

for sym in ALL_SUBSCRIBE + CANDLE_EQUITY:
    market_data[sym] = MarketData(sym)

# ── news engine ───────────────────────────────────────────────────────────────
def sentiment_score(text: str) -> tuple[int, str]:
    """
    Returns (score, label).
    score > 0  → BULLISH
    score < 0  → BEARISH
    score == 0 → NEUTRAL
    """
    t = text.lower()
    bull = sum(1 for w in BULLISH_WORDS if w in t)
    bear = sum(1 for w in BEARISH_WORDS if w in t)
    net  = bull - bear
    if net >= 2:  label = "BULLISH"
    elif net <= -2: label = "BEARISH"
    else:           label = "NEUTRAL"
    return net, label

def match_news_to_symbol(symbol: str, news_items: list) -> list:
    """Filter and score news items relevant to a symbol."""
    keywords = SYMBOL_KEYWORDS.get(symbol, [])
    if not keywords:
        return []
    results = []
    for item in news_items:
        text = (item.get("headline","") + " " + item.get("summary","")).lower()
        if any(kw in text for kw in keywords):
            score, label = sentiment_score(text)
            ts = item.get("datetime", 0)
            age_h = (time.time() - ts) / 3600 if ts else 999
            results.append({
                "headline": item.get("headline","")[:110],
                "source":   item.get("source",""),
                "label":    label,
                "score":    score,
                "age_h":    round(age_h, 1),
                "url":      item.get("url",""),
            })
    # Sort by recency, then sentiment strength
    results.sort(key=lambda x: (x["age_h"], -abs(x["score"])))
    return results[:4]

def aggregate_news_sentiment(news_items: list) -> tuple[int, str]:
    """Overall sentiment across a list of matched news items."""
    if not news_items:
        return 0, "NEUTRAL"
    total = sum(n["score"] for n in news_items)
    if total >= 3:   return total, "BULLISH"
    elif total <= -3: return total, "BEARISH"
    else:             return total, "NEUTRAL"

# ═══════════════════════════════════════════════════════════════════════════════
# TRADE JOURNAL — SQLite database, signal tracking, stats, adaptive learning
# ═══════════════════════════════════════════════════════════════════════════════

def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def db_init():
    """Create tables if they don't exist."""
    with db_connect() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT    NOT NULL,
            quality         TEXT    NOT NULL,
            direction       TEXT    NOT NULL,
            entry_price     REAL    NOT NULL,
            entry_low       REAL    NOT NULL,
            entry_high      REAL    NOT NULL,
            sl              REAL    NOT NULL,
            tp1             REAL    NOT NULL,
            tp2             REAL    NOT NULL,
            tp3             REAL    NOT NULL,
            rr_target       REAL    NOT NULL,
            score           INTEGER NOT NULL,
            news_sentiment  TEXT    DEFAULT 'NEUTRAL',
            cot_bias        TEXT    DEFAULT '',
            cot_pct_rank    REAL    DEFAULT 50,
            -- pattern flags (1=present, 0=absent)
            f_ema_aligned   INTEGER DEFAULT 0,
            f_rsi_extreme   INTEGER DEFAULT 0,
            f_macd_cross    INTEGER DEFAULT 0,
            f_vwap_side     INTEGER DEFAULT 0,
            f_liq_sweep     INTEGER DEFAULT 0,
            f_order_block   INTEGER DEFAULT 0,
            f_fvg           INTEGER DEFAULT 0,
            f_struct        INTEGER DEFAULT 0,
            f_vol_spike     INTEGER DEFAULT 0,
            f_cot_signal    INTEGER DEFAULT 0,
            f_news_cat      INTEGER DEFAULT 0,
            -- outcome
            status          TEXT    DEFAULT 'OPEN',
            outcome_price   REAL,
            outcome_at      TEXT,
            actual_rr       REAL,
            max_price       REAL,   -- peak favourable excursion
            created_at      TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS score_weights (
            feature         TEXT    PRIMARY KEY,
            base_weight     REAL    NOT NULL DEFAULT 1.0,
            learned_mult    REAL    NOT NULL DEFAULT 1.0,
            win_rate        REAL,
            sample_count    INTEGER DEFAULT 0,
            updated_at      TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_signals_status  ON signals(status);
        CREATE INDEX IF NOT EXISTS idx_signals_symbol  ON signals(symbol);
        CREATE INDEX IF NOT EXISTS idx_signals_created ON signals(created_at);
        """)
        # Seed default weights for all features
        features = [
            "f_ema_aligned","f_rsi_extreme","f_macd_cross","f_vwap_side",
            "f_liq_sweep","f_order_block","f_fvg","f_struct",
            "f_vol_spike","f_cot_signal","f_news_cat",
        ]
        for f in features:
            conn.execute("""
                INSERT OR IGNORE INTO score_weights(feature, base_weight, learned_mult)
                VALUES (?, 1.0, 1.0)
            """, (f,))
        conn.commit()

db_init()   # run on import

# ── signal logging ────────────────────────────────────────────────────────────
_last_logged: dict[str, float] = {}   # symbol+dir → timestamp

def extract_flags(tech_reasons: list, cot_snap: dict) -> dict:
    """Convert reason strings to binary feature flags for the DB."""
    joined = " ".join(tech_reasons).lower()
    return {
        "f_ema_aligned":  1 if "ema" in joined and ("stack" in joined or "aligned" in joined) else 0,
        "f_rsi_extreme":  1 if "oversold" in joined or "overbought" in joined else 0,
        "f_macd_cross":   1 if "macd" in joined and ("crossover" in joined or "histogram" in joined) else 0,
        "f_vwap_side":    1 if "vwap" in joined else 0,
        "f_liq_sweep":    1 if "sweep" in joined or "stop hunt" in joined else 0,
        "f_order_block":  1 if "order block" in joined else 0,
        "f_fvg":          1 if "fvg" in joined or "fair value" in joined else 0,
        "f_struct":       1 if "higher high" in joined or "lower low" in joined else 0,
        "f_vol_spike":    1 if "volume spike" in joined else 0,
        "f_cot_signal":   1 if cot_snap and cot_snap.get("cot_score",0) != 0 else 0,
        "f_news_cat":     0,  # filled in from news_sentiment
    }

def log_signal(setup: dict):
    """Insert a new signal into the journal. Deduplicates by symbol+direction."""
    key = f"{setup['symbol']}_{setup['direction']}"
    now = time.time()
    if now - _last_logged.get(key, 0) < SIGNAL_DEDUP_SECS:
        return   # too soon — same signal still potentially open
    _last_logged[key] = now

    cot_snap = setup.get("cot") or {}
    flags    = extract_flags(setup.get("tech_reasons",[]), cot_snap)
    flags["f_news_cat"] = 1 if setup.get("news_sentiment") in ("BULLISH","BEARISH") else 0

    el, eh = setup["entry"]
    with db_connect() as conn:
        conn.execute("""
            INSERT INTO signals (
                symbol, quality, direction,
                entry_price, entry_low, entry_high,
                sl, tp1, tp2, tp3, rr_target, score,
                news_sentiment, cot_bias, cot_pct_rank,
                f_ema_aligned, f_rsi_extreme, f_macd_cross, f_vwap_side,
                f_liq_sweep, f_order_block, f_fvg, f_struct,
                f_vol_spike, f_cot_signal, f_news_cat,
                status, created_at
            ) VALUES (
                ?,?,?,  ?,?,?,  ?,?,?,?,?,?,
                ?,?,?,
                ?,?,?,?,  ?,?,?,?,  ?,?,?,
                'OPEN', ?
            )
        """, (
            setup["symbol"], setup["quality"], setup["direction"],
            setup["price"], el, eh,
            setup["sl"], setup["tp1"], setup["tp2"], setup["tp3"],
            setup["rr"], setup["score"],
            setup.get("news_sentiment","NEUTRAL"),
            cot_snap.get("bias",""), cot_snap.get("pct_rank",50),
            flags["f_ema_aligned"], flags["f_rsi_extreme"], flags["f_macd_cross"], flags["f_vwap_side"],
            flags["f_liq_sweep"],   flags["f_order_block"], flags["f_fvg"],        flags["f_struct"],
            flags["f_vol_spike"],   flags["f_cot_signal"],  flags["f_news_cat"],
            datetime.utcnow().isoformat(timespec="seconds"),
        ))
        conn.commit()

# ── signal outcome monitor ────────────────────────────────────────────────────
def monitor_open_signals():
    """
    Background thread: every 60s checks all OPEN signals against live prices.
    Updates status to TP1/TP2/TP3/SL/EXPIRED and records actual R:R.
    """
    while True:
        try:
            _check_signals()
        except Exception:
            pass
        time.sleep(60)

def _check_signals():
    with db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM signals WHERE status='OPEN'"
        ).fetchall()
        now_str = datetime.utcnow().isoformat(timespec="seconds")

        for row in rows:
            sym    = row["symbol"]
            dirn   = row["direction"]
            ep     = row["entry_price"]
            sl     = row["sl"]
            tp1    = row["tp1"]
            tp2    = row["tp2"]
            tp3    = row["tp3"]
            rr_t   = row["rr_target"]
            created = row["created_at"]

            # Get current price
            with lock:
                md = market_data.get(sym)
                cur = md.price if md else None

            if cur is None:
                continue

            risk   = abs(ep - sl)
            if risk == 0:
                continue

            # Update max favourable excursion
            if dirn == "LONG":
                mfe = cur if (row["max_price"] is None or cur > row["max_price"]) else row["max_price"]
            else:
                mfe = cur if (row["max_price"] is None or cur < row["max_price"]) else row["max_price"]

            conn.execute("UPDATE signals SET max_price=? WHERE id=?", (mfe, row["id"]))

            # Check outcome
            new_status   = None
            outcome_price = cur
            actual_rr    = None

            if dirn == "LONG":
                if cur <= sl:
                    new_status = "SL"
                    actual_rr  = round(-1.0, 2)
                elif cur >= tp3:
                    new_status = "TP3"
                    actual_rr  = round(rr_t, 2)
                elif cur >= tp2:
                    new_status = "TP2"
                    actual_rr  = round(rr_t * 0.6, 2)
                elif cur >= tp1:
                    new_status = "TP1"
                    actual_rr  = round(rr_t * 0.3, 2)
            else:  # SHORT
                if cur >= sl:
                    new_status = "SL"
                    actual_rr  = round(-1.0, 2)
                elif cur <= tp3:
                    new_status = "TP3"
                    actual_rr  = round(rr_t, 2)
                elif cur <= tp2:
                    new_status = "TP2"
                    actual_rr  = round(rr_t * 0.6, 2)
                elif cur <= tp1:
                    new_status = "TP1"
                    actual_rr  = round(rr_t * 0.3, 2)

            # Expire signals older than 7 days
            try:
                age_days = (datetime.utcnow() - datetime.fromisoformat(created)).days
                if age_days >= 7 and new_status is None:
                    new_status    = "EXPIRED"
                    outcome_price = cur
                    actual_rr     = round((cur - ep) / risk, 2) if dirn == "LONG" else round((ep - cur) / risk, 2)
            except Exception:
                pass

            if new_status:
                conn.execute("""
                    UPDATE signals
                    SET status=?, outcome_price=?, outcome_at=?, actual_rr=?
                    WHERE id=?
                """, (new_status, outcome_price, now_str, actual_rr, row["id"]))

        conn.commit()

# ── statistics engine ─────────────────────────────────────────────────────────
STAT_FEATURES = [
    ("f_ema_aligned", "EMA Aligned"),
    ("f_rsi_extreme", "RSI Extreme"),
    ("f_macd_cross",  "MACD Cross"),
    ("f_vwap_side",   "VWAP Side"),
    ("f_liq_sweep",   "Liq Sweep"),
    ("f_order_block", "Order Block"),
    ("f_fvg",         "FVG"),
    ("f_struct",      "Structure"),
    ("f_vol_spike",   "Vol Spike"),
    ("f_cot_signal",  "COT Signal"),
    ("f_news_cat",    "News Cat"),
]

def compute_stats() -> dict:
    """
    Compute comprehensive journal statistics from closed trades.
    Returns a stats dict used for display and adaptive learning.
    """
    with db_connect() as conn:
        closed = conn.execute("""
            SELECT * FROM signals
            WHERE status IN ('TP1','TP2','TP3','SL','EXPIRED')
        """).fetchall()

        if not closed:
            return {"total": 0}

        total    = len(closed)
        wins     = [r for r in closed if r["status"] in ("TP1","TP2","TP3")]
        losses   = [r for r in closed if r["status"] == "SL"]
        expired  = [r for r in closed if r["status"] == "EXPIRED"]
        tp3_hits = [r for r in closed if r["status"] == "TP3"]
        tp2_hits = [r for r in closed if r["status"] == "TP2"]
        tp1_hits = [r for r in closed if r["status"] == "TP1"]

        win_rate = len(wins) / total * 100 if total else 0

        rr_vals  = [r["actual_rr"] for r in closed if r["actual_rr"] is not None]
        avg_rr   = sum(rr_vals) / len(rr_vals) if rr_vals else 0
        best_rr  = max(rr_vals) if rr_vals else 0
        worst_rr = min(rr_vals) if rr_vals else 0

        # Profit factor
        gross_win  = sum(r for r in rr_vals if r > 0)
        gross_loss = abs(sum(r for r in rr_vals if r < 0))
        pf         = round(gross_win / gross_loss, 2) if gross_loss else 999.0

        # By quality
        by_quality = {}
        for q in ("A+","A","B+"):
            qrows = [r for r in closed if r["quality"]==q]
            qwins = [r for r in qrows if r["status"] in ("TP1","TP2","TP3")]
            by_quality[q] = {
                "total": len(qrows),
                "wins":  len(qwins),
                "wr":    round(len(qwins)/len(qrows)*100,1) if qrows else 0,
                "avg_rr": round(sum(r["actual_rr"] for r in qrows if r["actual_rr"])/len(qrows),2) if qrows else 0,
            }

        # By direction
        longs  = [r for r in closed if r["direction"]=="LONG"]
        shorts = [r for r in closed if r["direction"]=="SHORT"]
        lw     = [r for r in longs  if r["status"] in ("TP1","TP2","TP3")]
        sw     = [r for r in shorts if r["status"] in ("TP1","TP2","TP3")]

        # By symbol (top 5 by trade count)
        sym_counts: dict = {}
        for r in closed:
            s = r["symbol"].replace("BINANCE:","").replace("OANDA:","").replace("_","/")
            if s not in sym_counts: sym_counts[s] = {"t":0,"w":0,"rr":[]}
            sym_counts[s]["t"] += 1
            if r["status"] in ("TP1","TP2","TP3"): sym_counts[s]["w"] += 1
            if r["actual_rr"]: sym_counts[s]["rr"].append(r["actual_rr"])
        top_syms = sorted(sym_counts.items(), key=lambda x: x[1]["t"], reverse=True)[:8]

        # Per-feature win rates
        feature_stats = {}
        for col, label in STAT_FEATURES:
            f_rows = [r for r in closed if r[col]==1]
            f_wins = [r for r in f_rows if r["status"] in ("TP1","TP2","TP3")]
            wr     = len(f_wins)/len(f_rows)*100 if f_rows else None
            feature_stats[col] = {
                "label":   label,
                "total":   len(f_rows),
                "wins":    len(f_wins),
                "wr":      round(wr,1) if wr is not None else None,
            }

        # Recent 10 signals
        recent = conn.execute("""
            SELECT symbol, quality, direction, status, actual_rr, rr_target,
                   entry_price, created_at
            FROM signals ORDER BY id DESC LIMIT 10
        """).fetchall()

        return {
            "total":      total,
            "wins":       len(wins),
            "losses":     len(losses),
            "expired":    len(expired),
            "tp3_hits":   len(tp3_hits),
            "tp2_hits":   len(tp2_hits),
            "tp1_hits":   len(tp1_hits),
            "win_rate":   round(win_rate, 1),
            "avg_rr":     round(avg_rr, 2),
            "best_rr":    round(best_rr, 2),
            "worst_rr":   round(worst_rr, 2),
            "profit_factor": pf,
            "by_quality": by_quality,
            "by_dir": {
                "LONG":  {"total":len(longs),  "wins":len(lw),  "wr":round(len(lw)/len(longs)*100,1)  if longs  else 0},
                "SHORT": {"total":len(shorts), "wins":len(sw),  "wr":round(len(sw)/len(shorts)*100,1) if shorts else 0},
            },
            "top_symbols":   top_syms,
            "feature_stats": feature_stats,
            "recent":        [dict(r) for r in recent],
        }

# ── adaptive weight engine ────────────────────────────────────────────────────
def update_adaptive_weights(stats: dict):
    """
    Recalculate learned weight multipliers from feature win rates.
    Features with WR > 60% → multiplier increases (max 1.5x)
    Features with WR < 40% → multiplier decreases (min 0.5x)
    """
    global adaptive_weights
    if stats.get("total", 0) < ADAPTIVE_MIN_TRADES:
        return  # not enough data yet

    feature_stats = stats.get("feature_stats", {})
    new_weights   = {}
    now_str       = datetime.utcnow().isoformat(timespec="seconds")

    with db_connect() as conn:
        for col, fdata in feature_stats.items():
            if fdata["total"] < 10:
                new_weights[col] = 1.0
                continue
            wr = fdata["wr"] or 50.0
            # Sigmoid-like scaling: WR 70% → ~1.4x, WR 30% → ~0.6x
            mult = max(0.5, min(1.5, 0.5 + wr / 50.0))
            new_weights[col] = round(mult, 3)
            conn.execute("""
                UPDATE score_weights
                SET learned_mult=?, win_rate=?, sample_count=?, updated_at=?
                WHERE feature=?
            """, (mult, wr, fdata["total"], now_str, col))
        conn.commit()

    adaptive_weights = new_weights

def get_weight(feature: str) -> float:
    """Return the current adaptive multiplier for a feature (default 1.0)."""
    return adaptive_weights.get(feature, 1.0)

def apply_adaptive_weights(score: int, flags: dict) -> float:
    """Apply learned multipliers to a setup's raw score."""
    if not adaptive_weights:
        return float(score)
    boost = sum(
        (get_weight(col) - 1.0)   # delta from neutral
        for col, val in flags.items() if val == 1
    )
    return round(score + boost, 2)

# ── stats refresh thread ──────────────────────────────────────────────────────
def stats_refresh_loop():
    global journal_stats, last_stats_time
    while True:
        try:
            stats = compute_stats()
            update_adaptive_weights(stats)
            with lock:
                journal_stats = stats
        except Exception:
            pass
        time.sleep(STATS_INTERVAL)

# ── COT engine ───────────────────────────────────────────────────────────────
def fetch_cot_report(market_name: str, num_weeks: int = 12) -> list[dict]:
    """
    Fetch COT legacy report rows for a given CFTC market name.
    Returns list of weekly rows (newest first), each with positioning data.
    """
    try:
        params = {
            "$where": f"upper(market_and_exchange_names) like upper('%{market_name.split(' - ')[0][:30]}%')",
            "$order": "report_date_as_yyyy_mm_dd DESC",
            "$limit": str(num_weeks),
        }
        r = requests.get(CFTC_API, params=params, timeout=10)
        data = r.json()
        if not isinstance(data, list) or not data:
            return []
        rows = []
        for d in data:
            try:
                rows.append({
                    "date":          d.get("report_date_as_yyyy_mm_dd","")[:10],
                    "market":        d.get("market_and_exchange_names",""),
                    # Large Speculators (Non-Commercial)
                    "spec_long":     int(d.get("noncomm_positions_long_all", 0)),
                    "spec_short":    int(d.get("noncomm_positions_short_all", 0)),
                    # Commercial Hedgers
                    "comm_long":     int(d.get("comm_positions_long_all", 0)),
                    "comm_short":    int(d.get("comm_positions_short_all", 0)),
                    # Small Speculators (Non-Reportable)
                    "small_long":    int(d.get("nonrept_positions_long_all", 0)),
                    "small_short":   int(d.get("nonrept_positions_short_all", 0)),
                    # Open Interest
                    "open_interest": int(d.get("open_interest_all", 0)),
                    # Week-over-week changes
                    "spec_long_chg":  int(d.get("change_in_noncomm_long_all", 0)),
                    "spec_short_chg": int(d.get("change_in_noncomm_short_all", 0)),
                    "comm_long_chg":  int(d.get("change_in_comm_long_all", 0)),
                    "comm_short_chg": int(d.get("change_in_comm_short_all", 0)),
                    "oi_chg":         int(d.get("change_in_open_interest_all", 0)),
                })
            except (ValueError, TypeError):
                continue
        return rows
    except Exception:
        return []

def analyse_cot(rows: list[dict]) -> dict:
    """
    Derive institutional signals from COT rows.
    Returns a dict with positioning stats and a bias signal.
    """
    if not rows:
        return {}

    latest = rows[0]
    spec_net   = latest["spec_long"]  - latest["spec_short"]
    comm_net   = latest["comm_long"]  - latest["comm_short"]
    small_net  = latest["small_long"] - latest["small_short"]

    # Net positioning change this week
    spec_net_chg = latest["spec_long_chg"] - latest["spec_short_chg"]
    comm_net_chg = latest["comm_long_chg"] - latest["comm_short_chg"]

    # Historical net range for extreme detection (last 12 weeks)
    spec_nets = [r["spec_long"]-r["spec_short"] for r in rows]
    mn, mx = min(spec_nets), max(spec_nets)
    rng = mx - mn if mx != mn else 1
    pct_rank = round((spec_net - mn) / rng * 100, 1)  # 0=most bearish, 100=most bullish

    # Bias
    if pct_rank >= 75:   bias = "EXTREME LONG"
    elif pct_rank >= 55: bias = "BULLISH"
    elif pct_rank <= 25: bias = "EXTREME SHORT"
    elif pct_rank <= 45: bias = "BEARISH"
    else:                bias = "NEUTRAL"

    # Contrarian signal for extreme readings
    contrarian = None
    if pct_rank >= 85:
        contrarian = ("SHORT", "Specs at EXTREME LONG — contrarian SHORT signal")
    elif pct_rank <= 15:
        contrarian = ("LONG",  "Specs at EXTREME SHORT — contrarian LONG signal")

    # Commercial vs Spec divergence (smart money vs dumb money)
    comm_divergence = None
    if comm_net > 0 and spec_net < 0:
        comm_divergence = "BULLISH — Commercials net LONG while specs net SHORT (contrarian long setup)"
    elif comm_net < 0 and spec_net > 0:
        comm_divergence = "BEARISH — Commercials net SHORT while specs net LONG (contrarian short setup)"

    # COT score contribution to setup scoring (+2/-2)
    cot_score = 0
    cot_reasons = []
    if pct_rank <= 25:
        cot_score = 2
        cot_reasons.append(f"COT: Spec net positioning at {pct_rank:.0f}th percentile — historically bullish territory")
    elif pct_rank >= 75:
        cot_score = -2
        cot_reasons.append(f"COT: Spec net positioning at {pct_rank:.0f}th percentile — historically bearish territory")
    if spec_net_chg > 0:
        cot_score += 1
        cot_reasons.append(f"COT: Specs added {spec_net_chg:+,} net longs this week — momentum buying")
    elif spec_net_chg < 0:
        cot_score -= 1
        cot_reasons.append(f"COT: Specs cut {spec_net_chg:+,} net longs this week — distribution signal")
    if comm_divergence:
        if "BULLISH" in comm_divergence: cot_score += 1
        else: cot_score -= 1
        cot_reasons.append(f"COT: {comm_divergence}")

    return {
        "date":           latest["date"],
        "spec_long":      latest["spec_long"],
        "spec_short":     latest["spec_short"],
        "spec_net":       spec_net,
        "spec_net_chg":   spec_net_chg,
        "comm_long":      latest["comm_long"],
        "comm_short":     latest["comm_short"],
        "comm_net":       comm_net,
        "comm_net_chg":   comm_net_chg,
        "small_net":      small_net,
        "open_interest":  latest["open_interest"],
        "oi_chg":         latest["oi_chg"],
        "pct_rank":       pct_rank,
        "bias":           bias,
        "contrarian":     contrarian,
        "comm_divergence":comm_divergence,
        "cot_score":      cot_score,
        "cot_reasons":    cot_reasons,
        "weeks_tracked":  len(rows),
    }

def fetch_cot_all():
    """Fetch & analyse COT for all mapped symbols. Updates cot_cache."""
    global cot_cache
    new_cache = {}
    for sym, market_name in COT_MARKET_MAP.items():
        try:
            rows    = fetch_cot_report(market_name, num_weeks=12)
            analysis = analyse_cot(rows)
            if analysis:
                new_cache[sym] = analysis
        except Exception:
            pass
        time.sleep(0.3)  # polite rate limiting
    with lock:
        cot_cache = new_cache

def fetch_all_news():
    """Fetch general + crypto + forex news from Finnhub."""
    items = []
    categories = ["general","crypto","forex","merger"]
    for cat in categories:
        try:
            r = requests.get(f"{BASE_URL}/news",
                             params=dict(category=cat, token=API_KEY), timeout=5)
            data = r.json()
            if isinstance(data, list):
                items.extend(data[:15])
        except Exception:
            pass
    # Deduplicate by headline
    seen = set()
    unique = []
    for item in items:
        h = item.get("headline","")
        if h and h not in seen:
            seen.add(h)
            unique.append(item)
    return unique

def fetch_company_news(symbol: str) -> list:
    """Fetch company-specific news for equity symbols."""
    try:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        # 7 days back
        from_date = datetime.utcfromtimestamp(time.time() - 7*86400).strftime("%Y-%m-%d")
        r = requests.get(f"{BASE_URL}/company-news",
                         params=dict(symbol=symbol, from_=from_date, to=today,
                                     token=API_KEY), timeout=5)
        # Finnhub uses 'from' not 'from_'
        r = requests.get(f"{BASE_URL}/company-news",
                         params={"symbol": symbol, "from": from_date,
                                 "to": today, "token": API_KEY}, timeout=5)
        data = r.json()
        if isinstance(data, list):
            return data[:20]
    except Exception:
        pass
    return []

# ── REST helpers ──────────────────────────────────────────────────────────────
def resolution_to_seconds(res):
    return {"1":60,"5":300,"15":900,"30":1800,"60":3600,"D":86400}.get(str(res),3600)

def fetch_candles(symbol, resolution="60", count=CANDLE_HISTORY):
    now   = int(time.time())
    from_ = now - count * resolution_to_seconds(resolution)
    p     = {"symbol": symbol, "resolution": resolution,
             "from": from_, "to": now, "token": API_KEY}
    for endpoint in [f"{BASE_URL}/stock/candle", f"{BASE_URL}/crypto/candle",
                     f"{BASE_URL}/forex/candle"]:
        try:
            r    = requests.get(endpoint, params=p, timeout=6)
            data = r.json()
            if data.get("s") == "ok":
                return list(zip(data["t"],data["o"],data["h"],data["l"],data["c"],data["v"]))
        except Exception:
            pass
    return []

def fetch_quote(symbol):
    try:
        r = requests.get(f"{BASE_URL}/quote",
                         params=dict(symbol=symbol, token=API_KEY), timeout=4)
        return r.json()
    except Exception:
        return {}

# ── technical indicators ──────────────────────────────────────────────────────
def ema(prices, period):
    if len(prices) < period: return []
    k = 2 / (period + 1)
    r = [sum(prices[:period]) / period]
    for p in prices[period:]:
        r.append(p * k + r[-1] * (1 - k))
    return r

def sma(prices, period):
    return [sum(prices[i:i+period])/period for i in range(len(prices)-period+1)]

def rsi(prices, period=14):
    if len(prices) < period + 1: return None
    d  = [prices[i+1]-prices[i] for i in range(len(prices)-1)]
    ag = sum(max(x,0) for x in d[:period]) / period
    al = sum(max(-x,0) for x in d[:period]) / period
    for x in d[period:]:
        ag = (ag*(period-1)+max(x,0))  / period
        al = (al*(period-1)+max(-x,0)) / period
    if al == 0: return 100.0
    return round(100 - 100/(1+ag/al), 2)

def macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow+signal: return None,None,None
    ef = ema(prices,fast); es = ema(prices,slow)
    n  = min(len(ef),len(es))
    ml = [ef[-n+i]-es[-n+i] for i in range(n)]
    sl = ema(ml,signal)
    if not sl: return None,None,None
    h  = ml[-1]-sl[-1]
    return round(ml[-1],6), round(sl[-1],6), round(h,6)

def atr(candles, period=14):
    if len(candles) < period+1: return None
    trs = [max(candles[i][2]-candles[i][3],
               abs(candles[i][2]-candles[i-1][4]),
               abs(candles[i][3]-candles[i-1][4]))
           for i in range(1,len(candles))]
    if len(trs) < period: return None
    return round(sum(trs[-period:])/period, 8)

def vwap_calc(candles):
    if not candles: return None
    tv = sum(((c[2]+c[3]+c[4])/3)*c[5] for c in candles)
    v  = sum(c[5] for c in candles)
    return tv/v if v else None

def bollinger(prices, period=20, mult=2.0):
    if len(prices) < period: return None,None,None
    mid = sma(prices,period)[-1]
    std = math.sqrt(sum((p-mid)**2 for p in prices[-period:])/period)
    return round(mid-mult*std,8), round(mid,8), round(mid+mult*std,8)

def detect_order_block(candles):
    if len(candles) < 10: return None,None
    rec = candles[-20:]; bull = bear = None
    for i in range(len(rec)-3):
        c = rec[i]
        nxt = rec[i+1:i+4]
        if c[4] < c[1] and any(n[4]>c[2] for n in nxt):
            bull = (c[3],c[2])
        if c[4] > c[1] and any(n[4]<c[3] for n in nxt):
            bear = (c[3],c[2])
    return bull, bear

def detect_fvg(candles):
    if len(candles) < 3: return None,None
    fb = fb2 = None
    for i in range(len(candles)-2):
        c1,_,c3 = candles[i],candles[i+1],candles[i+2]
        if c1[2] < c3[3]: fb  = (c1[2],c3[3])
        if c1[3] > c3[2]: fb2 = (c1[3],c3[2])
    return fb, fb2

def detect_liquidity_sweep(candles, lookback=20):
    if len(candles) < lookback+2: return None
    rec  = candles[-lookback-2:-2]
    last = candles[-1]
    sh   = max(c[2] for c in rec)
    sl_  = min(c[3] for c in rec)
    if last[2]>sh and last[4]<sh: return ("BEARISH_SWEEP", sh,  last[4])
    if last[3]<sl_ and last[4]>sl_: return ("BULLISH_SWEEP",  sl_, last[4])
    return None

def pivot_structure(highs, lows, n=10):
    if len(highs) < n: return None
    h = highs[-n:]; l = lows[-n:]
    if h[-1]>h[-2]>h[-3] and l[-1]>l[-2]>l[-3]: return "BULLISH"
    if h[-1]<h[-2]<h[-3] and l[-1]<l[-2]<l[-3]: return "BEARISH"
    return "RANGING"

# ── setup scoring engine ──────────────────────────────────────────────────────
def score_setup(symbol, candles, price, news_items=None, cot_data=None):
    """
    Returns dict with quality, direction, entry, sl, tp1-3, rr,
    technical_reasons, news_reasons, entry_narrative.
    Returns None if no valid setup.
    """
    if len(candles) < 50: return None

    closes = [c[4] for c in candles]
    highs  = [c[2] for c in candles]
    lows   = [c[3] for c in candles]
    vols   = [c[5] for c in candles]

    e9   = ema(closes,9)
    e20  = ema(closes,20)
    e50  = ema(closes,50)
    e200 = ema(closes,200) if len(closes)>=200 else []

    rsi_v            = rsi(closes[-50:],14)
    macd_l,macd_s,mh = macd(closes)
    atr_v            = atr(candles)
    vwap_v           = vwap_calc(candles[-24:])
    bb_l,bb_m,bb_h   = bollinger(closes)
    sweep            = detect_liquidity_sweep(candles)
    bull_ob,bear_ob  = detect_order_block(candles)
    fvg_b,fvg_br     = detect_fvg(candles[-10:])
    struct           = pivot_structure(highs,lows)

    avg_vol  = sum(vols[-20:])/20 if vols else 1
    vol_spk  = vols[-1]>avg_vol*1.5 if vols else False

    sl_pts = sl_sr = 0
    rl_pts = rl_sr = 0
    tech_long  = []
    tech_short = []

    # EMA stack
    if e9 and e20 and e50:
        if e9[-1]>e20[-1]>e50[-1]:
            sl_pts+=2; tech_long.append(f"EMA stack bullish (9>{e9[-1]:.4f} > 20>{e20[-1]:.4f} > 50>{e50[-1]:.4f})")
        elif e9[-1]<e20[-1]<e50[-1]:
            rl_pts+=2; tech_short.append(f"EMA stack bearish (9<20<50)")

    if e200:
        if price>e200[-1]:
            sl_pts+=1; tech_long.append(f"Above EMA200 ({e200[-1]:.4f}) — macro bullish")
        else:
            rl_pts+=1; tech_short.append(f"Below EMA200 ({e200[-1]:.4f}) — macro bearish")

    # RSI
    if rsi_v is not None:
        if rsi_v<30:
            sl_pts+=3; tech_long.append(f"RSI extreme oversold ({rsi_v}) — reversal fuel loaded")
        elif rsi_v<40:
            sl_pts+=2; tech_long.append(f"RSI oversold ({rsi_v}) — bullish reversion setup")
        elif 40<=rsi_v<=60:
            sl_pts+=1; rl_pts+=1
            tech_long.append(f"RSI neutral ({rsi_v}) — room to run both ways")
            tech_short.append(f"RSI neutral ({rsi_v}) — room to run both ways")
        elif rsi_v>70:
            rl_pts+=3; tech_short.append(f"RSI extreme overbought ({rsi_v}) — reversal fuel loaded")
        elif rsi_v>60:
            rl_pts+=2; tech_short.append(f"RSI overbought ({rsi_v}) — bearish reversion setup")

    # MACD
    if mh is not None:
        if mh>0 and macd_l>macd_s:
            sl_pts+=2; tech_long.append(f"MACD bullish crossover — histogram expanding ({mh:+.5f})")
        elif mh<0 and macd_l<macd_s:
            rl_pts+=2; tech_short.append(f"MACD bearish crossover — histogram expanding ({mh:+.5f})")
        elif mh>0:
            sl_pts+=1; tech_long.append(f"MACD histogram positive ({mh:+.5f})")
        elif mh<0:
            rl_pts+=1; tech_short.append(f"MACD histogram negative ({mh:+.5f})")

    # VWAP
    if vwap_v:
        dev = (price-vwap_v)/vwap_v*100
        if price>vwap_v*1.001:
            sl_pts+=1; tech_long.append(f"Price {dev:+.2f}% above VWAP ({vwap_v:.4f}) — institutional bid")
        elif price<vwap_v*0.999:
            rl_pts+=1; tech_short.append(f"Price {dev:+.2f}% below VWAP ({vwap_v:.4f}) — institutional pressure")

    # Bollinger
    if bb_l and bb_h:
        bb_pos = (price-bb_l)/(bb_h-bb_l)*100 if bb_h!=bb_l else 50
        if price<=bb_l*1.002:
            sl_pts+=2; tech_long.append(f"Price at/below lower Bollinger ({bb_l:.4f}) — BB mean reversion long")
        elif price>=bb_h*0.998:
            rl_pts+=2; tech_short.append(f"Price at/above upper Bollinger ({bb_h:.4f}) — BB mean reversion short")

    # Liquidity sweep
    if sweep:
        st,lvl,_ = sweep
        if st=="BULLISH_SWEEP":
            sl_pts+=3; tech_long.append(f"BULLISH LIQUIDITY SWEEP — stops taken below {lvl:.4f}, smart money absorbed")
        elif st=="BEARISH_SWEEP":
            rl_pts+=3; tech_short.append(f"BEARISH LIQUIDITY SWEEP — stops taken above {lvl:.4f}, distribution complete")

    # Order blocks
    if bull_ob:
        ob_mid = (bull_ob[0]+bull_ob[1])/2
        if abs(price-ob_mid)/ob_mid<0.015:
            sl_pts+=2; tech_long.append(f"Price at BULLISH ORDER BLOCK {bull_ob[0]:.4f}–{bull_ob[1]:.4f}")
    if bear_ob:
        ob_mid = (bear_ob[0]+bear_ob[1])/2
        if abs(price-ob_mid)/ob_mid<0.015:
            rl_pts+=2; tech_short.append(f"Price at BEARISH ORDER BLOCK {bear_ob[0]:.4f}–{bear_ob[1]:.4f}")

    # FVG
    if fvg_b and fvg_b[0]<=price<=fvg_b[1]:
        sl_pts+=2; tech_long.append(f"Price filling BULLISH FVG {fvg_b[0]:.4f}–{fvg_b[1]:.4f}")
    if fvg_br and fvg_br[1]<=price<=fvg_br[0]:
        rl_pts+=2; tech_short.append(f"Price filling BEARISH FVG {fvg_br[1]:.4f}–{fvg_br[0]:.4f}")

    # Market structure
    if struct=="BULLISH":
        sl_pts+=2; tech_long.append("Higher highs + higher lows — bullish market structure confirmed")
    elif struct=="BEARISH":
        rl_pts+=2; tech_short.append("Lower highs + lower lows — bearish market structure confirmed")

    # Volume spike
    if vol_spk:
        sl_pts+=1; rl_pts+=1
        tech_long.append(f"Volume spike ({vols[-1]/avg_vol:.1f}x avg) — institutional activity detected")
        tech_short.append(f"Volume spike ({vols[-1]/avg_vol:.1f}x avg) — institutional activity detected")

    # ── News sentiment scoring ────────────────────────────────────────────────
    news_reasons_long  = []
    news_reasons_short = []
    news_score_total   = 0

    if news_items:
        net_sentiment, label = aggregate_news_sentiment(news_items)
        news_score_total = net_sentiment
        bullish_news = [n for n in news_items if n["label"]=="BULLISH"]
        bearish_news = [n for n in news_items if n["label"]=="BEARISH"]

        for n in bullish_news[:2]:
            age_str = f"{n['age_h']:.0f}h ago" if n['age_h']<24 else f"{n['age_h']/24:.0f}d ago"
            news_reasons_long.append(
                f"[BULLISH NEWS {age_str}] {n['headline'][:80]} ({n['source']})"
            )
        for n in bearish_news[:2]:
            age_str = f"{n['age_h']:.0f}h ago" if n['age_h']<24 else f"{n['age_h']/24:.0f}d ago"
            news_reasons_short.append(
                f"[BEARISH NEWS {age_str}] {n['headline'][:80]} ({n['source']})"
            )

        if net_sentiment >= 2:
            sl_pts += min(net_sentiment, 3)
        elif net_sentiment <= -2:
            rl_pts += min(abs(net_sentiment), 3)

    # ── COT positioning score ─────────────────────────────────────────────────
    cot_reasons_long  = []
    cot_reasons_short = []
    cot_snap          = cot_data or cot_cache.get(symbol, {})
    if cot_snap:
        cs = cot_snap.get("cot_score", 0)
        if cs > 0:
            sl_pts += cs
            cot_reasons_long.extend(cot_snap.get("cot_reasons", []))
        elif cs < 0:
            rl_pts += abs(cs)
            cot_reasons_short.extend(cot_snap.get("cot_reasons", []))
        # Contrarian override signal
        ct = cot_snap.get("contrarian")
        if ct:
            if ct[0] == "LONG":
                sl_pts += 2
                cot_reasons_long.append(f"COT CONTRARIAN: {ct[1]}")
            elif ct[0] == "SHORT":
                rl_pts += 2
                cot_reasons_short.append(f"COT CONTRARIAN: {ct[1]}")

    # ── Direction decision ────────────────────────────────────────────────────
    if sl_pts >= rl_pts:
        direction  = "LONG"
        score      = sl_pts
        tech_rsns  = tech_long
        news_rsns  = news_reasons_long
        cot_rsns   = cot_reasons_long
    else:
        direction  = "SHORT"
        score      = rl_pts
        tech_rsns  = tech_short
        news_rsns  = news_reasons_short
        cot_rsns   = cot_reasons_short

    if score >= 10:   quality = "A+"
    elif score >= 8:  quality = "A"
    elif score >= 6:  quality = "B+"
    elif score >= 4:  quality = "B"
    else: return None

    if not atr_v or atr_v == 0: return None

    # ── Entry / SL / TP ───────────────────────────────────────────────────────
    if direction == "LONG":
        entry_l = price - atr_v * 0.3
        entry_h = price + atr_v * 0.15
        sl      = price - atr_v * 1.8
        tp1     = price + atr_v * 1.5
        tp2     = price + atr_v * 3.0
        tp3     = price + atr_v * 5.0
    else:
        entry_l = price - atr_v * 0.15
        entry_h = price + atr_v * 0.3
        sl      = price + atr_v * 1.8
        tp1     = price - atr_v * 1.5
        tp2     = price - atr_v * 3.0
        tp3     = price - atr_v * 5.0

    risk   = abs(price - sl)
    reward = abs(tp3  - price)
    rr     = round(reward / risk, 2) if risk > 0 else 0
    if rr < 2.5: return None

    # ── Entry narrative ───────────────────────────────────────────────────────
    short_name = symbol.replace("BINANCE:","").replace("OANDA:","").replace("_","/")
    narrative  = _build_entry_narrative(
        short_name, direction, quality, price, entry_l, entry_h,
        sl, tp1, tp2, tp3, rr, atr_v, rsi_v, mh, struct, sweep,
        bull_ob, bear_ob, fvg_b, fvg_br, tech_rsns, news_rsns, news_score_total,
        cot_rsns, cot_snap
    )

    return {
        "symbol":      symbol,
        "quality":     quality,
        "direction":   direction,
        "price":       price,
        "entry":       (entry_l, entry_h),
        "sl":          sl,
        "tp1":         tp1,
        "tp2":         tp2,
        "tp3":         tp3,
        "rr":          rr,
        "score":       score,
        "tech_reasons":tech_rsns,
        "news_reasons":news_rsns,
        "cot_reasons": cot_rsns,
        "cot":         cot_snap,
        "narrative":   narrative,
        "time":        datetime.now().strftime("%H:%M:%S"),
        "rsi":         rsi_v,
        "atr":         atr_v,
        "news_count":  len(news_items) if news_items else 0,
        "news_sentiment": "BULLISH" if news_score_total>1 else
                          "BEARISH" if news_score_total<-1 else "NEUTRAL",
    }

def _build_entry_narrative(name, direction, quality, price, el, eh,
                           sl, tp1, tp2, tp3, rr, atr_v, rsi_v, macd_h,
                           struct, sweep, bull_ob, bear_ob, fvg_b, fvg_br,
                           tech_rsns, news_rsns, news_score,
                           cot_rsns=None, cot_snap=None):
    """Build a human-readable institutional entry narrative."""
    lines = []
    dir_word = "LONG" if direction=="LONG" else "SHORT"
    opp_word = "upside" if direction=="LONG" else "downside"
    lines.append(f"WHY ENTER {dir_word} on {name} RIGHT NOW:")
    lines.append("")

    # Structure
    if struct == "BULLISH" and direction=="LONG":
        lines.append("► Market structure is printing HH+HL — bulls in full control. Price is trending.")
    elif struct == "BEARISH" and direction=="SHORT":
        lines.append("► Market structure is printing LH+LL — bears in full control. Trend continuation.")
    elif struct == "RANGING":
        lines.append("► Price is ranging. Mean-reversion edge at extremes — play the boundary.")

    # Liquidity sweep
    if sweep:
        st,lvl,_ = sweep
        if st=="BULLISH_SWEEP" and direction=="LONG":
            lines.append(f"► Liquidity sweep just completed below {lvl:.4f}. Retail stops triggered, smart money absorbed all sells. This is the institutional entry signal.")
        elif st=="BEARISH_SWEEP" and direction=="SHORT":
            lines.append(f"► Liquidity sweep just completed above {lvl:.4f}. Retail longs stopped out, institutions distributed into the spike. Price should drop.")

    # Order blocks
    if bull_ob and direction=="LONG":
        lines.append(f"► Price has returned to a bullish order block ({bull_ob[0]:.4f}–{bull_ob[1]:.4f}). This is where institutions placed buy orders previously. Expect defence of this zone.")
    if bear_ob and direction=="SHORT":
        lines.append(f"► Price has returned to a bearish order block ({bear_ob[0]:.4f}–{bear_ob[1]:.4f}). Institutions distributed here before. Expect renewed selling pressure.")

    # FVG
    if fvg_b and direction=="LONG":
        lines.append(f"► Bullish FVG at {fvg_b[0]:.4f}–{fvg_b[1]:.4f} acting as a magnet. Price entering the imbalance — institutions will fill this gap.")
    if fvg_br and direction=="SHORT":
        lines.append(f"► Bearish FVG at {fvg_br[1]:.4f}–{fvg_br[0]:.4f} overhead. Price rejected into the imbalance — distribution zone active.")

    # RSI
    if rsi_v is not None:
        if rsi_v<30 and direction=="LONG":
            lines.append(f"► RSI at {rsi_v} — extreme oversold. Statistical edge for reversal is highest at these levels. Fuel for a bounce is loaded.")
        elif rsi_v>70 and direction=="SHORT":
            lines.append(f"► RSI at {rsi_v} — extreme overbought. Institutional desk would be fading this move aggressively. Mean reversion imminent.")
        elif 40<=rsi_v<=60:
            lines.append(f"► RSI at {rsi_v} — neutral, momentum has room to extend in the {opp_word} direction without hitting extreme levels.")

    # MACD
    if macd_h is not None:
        if macd_h>0 and direction=="LONG":
            lines.append(f"► MACD histogram positive and expanding — momentum is accelerating {opp_word}. Trend riders will add here.")
        elif macd_h<0 and direction=="SHORT":
            lines.append(f"► MACD histogram negative and expanding — momentum is accelerating {opp_word}. Sellers are in control.")

    # ATR context
    lines.append(f"► ATR ({atr_v:.5f}) defines the risk envelope. SL placed 1.8× ATR away for breathing room without excessive risk.")

    # News
    if news_rsns:
        lines.append("")
        lines.append("MACRO / NEWS CATALYST:")
        for nr in news_rsns[:3]:
            lines.append(f"  {nr}")
    elif news_score == 0:
        lines.append("")
        lines.append("NEWS: No strong directional catalyst — pure technical / liquidity setup.")

    # COT Positioning
    if cot_snap:
        lines.append("")
        lines.append("COMMITMENT OF TRADERS (CFTC — Weekly):")
        pr   = cot_snap.get("pct_rank", 50)
        bias = cot_snap.get("bias","—")
        sn   = cot_snap.get("spec_net", 0)
        snc  = cot_snap.get("spec_net_chg", 0)
        cn   = cot_snap.get("comm_net", 0)
        oi   = cot_snap.get("open_interest", 0)
        date = cot_snap.get("date","—")
        lines.append(f"  Report date       : {date}")
        lines.append(f"  Spec net position : {sn:+,}  (WoW: {snc:+,})")
        lines.append(f"  Commercial net    : {cn:+,}")
        lines.append(f"  Open interest     : {oi:,}")
        lines.append(f"  12-week rank      : {pr:.0f}th percentile  →  {bias}")
        cd = cot_snap.get("comm_divergence")
        if cd:
            lines.append(f"  Comm vs Spec      : {cd}")
        ct = cot_snap.get("contrarian")
        if ct:
            lines.append(f"  ⚠ CONTRARIAN SIGNAL: {ct[1]}")
        if cot_rsns:
            for cr in cot_rsns:
                lines.append(f"  ► {cr}")
    else:
        lines.append("")
        lines.append("COT: No CFTC positioning data available for this instrument.")

    # Risk
    lines.append("")
    lines.append(f"RISK MANAGEMENT:")
    lines.append(f"  Entry zone : {el:.5f} – {eh:.5f}")
    lines.append(f"  Stop loss  : {sl:.5f}  (invalidation — exit immediately on close beyond)")
    lines.append(f"  TP1 (25%)  : {tp1:.5f}  — partial exit, move SL to breakeven")
    lines.append(f"  TP2 (50%)  : {tp2:.5f}  — reduce position")
    lines.append(f"  TP3 (25%)  : {tp3:.5f}  — final runner")
    lines.append(f"  R:R        : 1:{rr}  |  Max risk: 1-2% of capital on this trade")

    return "\n".join(lines)

# ── COT background loader ─────────────────────────────────────────────────────
def cot_loader():
    """Fetch COT data on startup and every 12 hours (published weekly by CFTC)."""
    while True:
        try:
            fetch_cot_all()
        except Exception:
            pass
        time.sleep(12 * 3600)

# ── background data & news loader ─────────────────────────────────────────────
def background_loader():
    global news_cache, categorised_news
    first_run = True
    while True:
        try:
            # ── equity quotes ──
            for sym in EQUITY_SYMBOLS:
                q = fetch_quote(sym)
                if q and q.get("c"):
                    with lock:
                        md = market_data[sym]
                        md.price = q["c"]; md.prev = q["pc"]
                        md.high  = q["h"]; md.low  = q["l"]; md.open_ = q["o"]

            # ── candles ──
            for sym in CANDLE_EQUITY + CANDLE_CRYPTO:
                candles = fetch_candles(sym, resolution="60", count=CANDLE_HISTORY)
                if candles:
                    with lock:
                        if sym not in market_data:
                            market_data[sym] = MarketData(sym)
                        market_data[sym].candles = candles
                        market_data[sym].price   = candles[-1][4]

            # ── general + category news ──
            all_news = fetch_all_news()

            # ── company news for equities ──
            for sym in EQUITY_SYMBOLS:
                cn = fetch_company_news(sym)
                all_news.extend(cn)

            with lock:
                news_cache = all_news[:30]
                # Categorise per symbol
                new_cat = {}
                for sym in list(market_data.keys()):
                    matched = match_news_to_symbol(sym, all_news)
                    if matched:
                        new_cat[sym] = matched
                categorised_news = new_cat

        except Exception:
            pass

        time.sleep(60 if first_run else 90)
        first_run = False

# ── analysis engine ───────────────────────────────────────────────────────────
def run_analysis():
    global last_analysis_time, setup_alerts
    if time.time() - last_analysis_time < ANALYSIS_INTERVAL:
        return
    last_analysis_time = time.time()

    results = []
    with lock:
        snap_md   = {k: v for k,v in market_data.items()}
        snap_news = dict(categorised_news)
        snap_cot  = dict(cot_cache)

    for sym, md in snap_md.items():
        if not md.price: continue
        candles = list(md.candles) if md.candles else []
        if not candles and len(md.ticks) >= 50:
            candles = _ticks_to_candles(list(md.ticks), 60)
        if len(candles) < 30: continue
        try:
            result = score_setup(
                sym, candles, md.price,
                news_items=snap_news.get(sym, []),
                cot_data=snap_cot.get(sym),
            )
            if result and result["quality"] in ("A+","A","B+"):
                results.append(result)
                # Auto-log to journal (dedup inside log_signal)
                try:
                    log_signal(result)
                except Exception:
                    pass
        except Exception:
            pass

    order = {"A+":0,"A":1,"B+":2}
    results.sort(key=lambda x: (order.get(x["quality"],9), -x["rr"]))
    setup_alerts = results

def _ticks_to_candles(ticks, interval_sec):
    if not ticks: return []
    candles=[]; o=h=l=c=None; v=0
    bucket = ticks[0][0]-(ticks[0][0]%interval_sec)
    for ts,price,vol in ticks:
        b = ts-(ts%interval_sec)
        if b!=bucket and o is not None:
            candles.append((bucket,o,h,l,c,v))
            o=h=l=c=None; v=0; bucket=b
        if o is None: o=h=l=price
        h=max(h,price); l=min(l,price); c=price; v+=vol
    if o is not None: candles.append((bucket,o,h,l,c,v))
    return candles

# ── WebSocket ──────────────────────────────────────────────────────────────────
def on_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("type")=="trade":
            for t in data["data"]:
                sym=t["s"]; price=t["p"]; vol=t.get("v",0)
                ts=t.get("t",int(time.time()*1000))//1000
                with lock:
                    if sym not in market_data: market_data[sym]=MarketData(sym)
                    md=market_data[sym]
                    if md.price is None: md.open_=price; md.prev=price
                    if md.high is None or price>md.high: md.high=price
                    if md.low  is None or price<md.low:  md.low=price
                    md.prev=md.price or price; md.price=price
                    md.volume+=vol; md.trades+=1
                    md.ticks.append((ts,price,vol))
                    md.last_ws=datetime.now().strftime("%H:%M:%S")
    except Exception: pass

def on_error(ws,e):
    global ws_connected; ws_connected=False

def on_close(ws,c,m):
    global ws_connected; ws_connected=False

def on_open(ws):
    global ws_connected; ws_connected=True
    for s in ALL_SUBSCRIBE:
        ws.send(json.dumps({"type":"subscribe","symbol":s}))

def start_websocket():
    while True:
        try:
            websocket.WebSocketApp(WS_URL,
                on_message=on_message,on_error=on_error,
                on_close=on_close,on_open=on_open
            ).run_forever(ping_interval=20,ping_timeout=10)
        except Exception: pass
        time.sleep(5)

# ── display helpers ────────────────────────────────────────────────────────────
def fp(p):
    if p is None: return "[dim]N/A[/dim]"
    if abs(p)>10000: return f"{p:,.1f}"
    if abs(p)>100:   return f"{p:,.2f}"
    if abs(p)>1:     return f"{p:.4f}"
    return f"{p:.6f}"

def cpct(v):
    if v>0: return f"[bright_green]+{v:.2f}%[/bright_green]"
    if v<0: return f"[bright_red]{v:.2f}%[/bright_red]"
    return f"[dim]0.00%[/dim]"

def qcolor(q):
    c={"A+":"bold bright_yellow","A":"bold green","B+":"bold cyan"}.get(q,"white")
    return f"[{c}]{q}[/{c}]"

def dcolor(d):
    return f"[bright_green]▲ LONG[/bright_green]" if d=="LONG" else f"[bright_red]▼ SHORT[/bright_red]"

def sent_color(s):
    if s=="BULLISH": return "[bright_green]● BULLISH[/bright_green]"
    if s=="BEARISH": return "[bright_red]● BEARISH[/bright_red]"
    return "[dim]● NEUTRAL[/dim]"

# ── panels ─────────────────────────────────────────────────────────────────────
def build_header():
    now    = datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M:%S UTC")
    status = "[bright_green]● WS LIVE[/bright_green]" if ws_connected else "[bright_red]● WS OFFLINE[/bright_red]"
    n_setups = len(setup_alerts)
    badge  = f"[bright_yellow]{n_setups} SETUP{'S' if n_setups!=1 else ''}[/bright_yellow]" if n_setups else "[dim]NO SETUPS[/dim]"
    return Panel(
        Align.center(
            f"[bold bright_yellow]TITAN FLOW[/bold bright_yellow]  [dim]│[/dim]  "
            f"[bold]INSTITUTIONAL INTELLIGENCE TERMINAL[/bold]  [dim]│[/dim]  "
            f"{status}  [dim]│[/dim]  {badge}  [dim]│[/dim]  [dim]{now}[/dim]"
        ),
        box=box.HEAVY, border_style="bright_yellow",
    )

def build_market_table():
    t = Table(title="[bold]● LIVE MARKET FEED[/bold]", box=box.SIMPLE_HEAVY,
              border_style="bright_blue", header_style="bold bright_blue", show_lines=True)
    t.add_column("SYMBOL",     style="bold white", width=14)
    t.add_column("PRICE",      width=13, justify="right")
    t.add_column("CHG",        width=9,  justify="right")
    t.add_column("HIGH",       width=12, justify="right", style="green")
    t.add_column("LOW",        width=12, justify="right", style="red")
    t.add_column("VOL",        width=10, justify="right", style="dim")
    t.add_column("NEWS",       width=9,  justify="center")
    t.add_column("TICK",       width=9,  style="dim")

    groups = [
        ("CRYPTO",        CRYPTO_SYMBOLS),
        ("FOREX MAJORS",  ["OANDA:EUR_USD","OANDA:GBP_USD","OANDA:USD_JPY","OANDA:USD_CHF",
                            "OANDA:AUD_USD","OANDA:USD_CAD","OANDA:NZD_USD"]),
        ("FOREX CROSSES", ["OANDA:EUR_GBP","OANDA:EUR_JPY","OANDA:GBP_JPY",
                            "OANDA:EUR_CHF","OANDA:AUD_JPY","OANDA:GBP_CHF"]),
        ("METALS & OIL",  ["OANDA:XAU_USD","OANDA:XAG_USD","OANDA:BCO_USD","OANDA:WTICO_USD"]),
        ("EQUITIES",      EQUITY_SYMBOLS),
    ]

    with lock:
        cat_snap = dict(categorised_news)

    for label, syms in groups:
        t.add_row(f"[dim bold]── {label} ──[/dim bold]","","","","","","","")
        for sym in syms:
            md = market_data.get(sym)
            if not md: continue
            news_items = cat_snap.get(sym,[])
            if news_items:
                ns, nl = aggregate_news_sentiment(news_items)
                news_str = sent_color(nl).replace("● ","")[:7]
            else:
                news_str = "[dim]—[/dim]"
            t.add_row(
                md.short_name,
                f"[bright_white]{fp(md.price)}[/bright_white]",
                cpct(md.change_pct),
                fp(md.high) if md.high else "—",
                fp(md.low)  if md.low  else "—",
                f"{md.volume:,.0f}" if md.volume else "—",
                news_str,
                md.last_ws or "—",
            )
    return Panel(t, border_style="bright_blue", box=box.ROUNDED)

def build_setup_table():
    alerts = list(setup_alerts)
    if not alerts:
        return Panel(
            Align.center(
                "[bold bright_yellow]SCANNING FOR INSTITUTIONAL SETUPS...[/bold bright_yellow]\n"
                "[dim]Minimum score 6/15 · Minimum R:R 1:2.5 · A+ A B+ only[/dim]"
            ),
            title="[bold bright_yellow]● ACTIVE SETUPS[/bold bright_yellow]",
            border_style="bright_yellow", box=box.HEAVY,
        )

    t = Table(title="[bold bright_yellow]● ACTIVE SETUPS — A+ / A / B+[/bold bright_yellow]",
              box=box.SIMPLE_HEAVY, border_style="bright_yellow",
              header_style="bold bright_yellow", show_lines=True)
    t.add_column("GRADE",  width=6,  justify="center")
    t.add_column("SYMBOL", width=14, style="bold white")
    t.add_column("DIR",    width=9,  justify="center")
    t.add_column("PRICE",  width=12, justify="right")
    t.add_column("ENTRY",  width=22, justify="right")
    t.add_column("SL",     width=12, justify="right", style="bright_red")
    t.add_column("TP1",    width=12, justify="right", style="green")
    t.add_column("TP2",    width=12, justify="right", style="bright_green")
    t.add_column("TP3",    width=12, justify="right", style="bright_yellow")
    t.add_column("R:R",    width=7,  justify="center")
    t.add_column("RSI",    width=6,  justify="center")
    t.add_column("NEWS",   width=9,  justify="center")
    t.add_column("SCORE",  width=7,  justify="center")
    t.add_column("TIME",   width=8)

    for s in alerts:
        rsi_v = s.get("rsi")
        rc    = "bright_green" if rsi_v and rsi_v<40 else "bright_red" if rsi_v and rsi_v>65 else "white"
        t.add_row(
            qcolor(s["quality"]),
            s["symbol"].replace("BINANCE:","").replace("OANDA:","").replace("_","/"),
            dcolor(s["direction"]),
            f"[bright_white]{fp(s['price'])}[/bright_white]",
            f"{fp(s['entry'][0])} – {fp(s['entry'][1])}",
            fp(s["sl"]),
            fp(s["tp1"]),
            fp(s["tp2"]),
            fp(s["tp3"]),
            f"[bold]1:{s['rr']}[/bold]",
            f"[{rc}]{rsi_v:.0f}[/{rc}]" if rsi_v else "—",
            sent_color(s["news_sentiment"]).replace("[bright_green]","[green]").replace("[bright_red]","[red]").replace("● ","")[:7],
            f"[dim]{s['score']}[/dim]",
            s["time"],
        )
    return Panel(t, border_style="bright_yellow", box=box.HEAVY)

def build_detail_panels():
    """Full institutional detail for top 3 setups."""
    alerts = list(setup_alerts)[:3]
    panels = []

    for s in alerts:
        sym_name = s["symbol"].replace("BINANCE:","").replace("OANDA:","").replace("_","/")

        # Technical reasons
        tech_text = ""
        for i,r in enumerate(s["tech_reasons"],1):
            tech_text += f"  [bright_green]{i:02d}.[/bright_green] {r}\n"

        # News reasons
        news_text = ""
        if s["news_reasons"]:
            for r in s["news_reasons"]:
                lc = "bright_green" if "BULLISH" in r else "bright_red"
                news_text += f"  [{lc}]▸[/{lc}] {r}\n"
        else:
            news_text = "  [dim]No relevant news catalyst — pure technical setup[/dim]\n"

        # COT reasons
        cot_text = ""
        cot_snap = s.get("cot", {})
        if cot_snap:
            pr   = cot_snap.get("pct_rank",50)
            bias = cot_snap.get("bias","—")
            sn   = cot_snap.get("spec_net",0)
            snc  = cot_snap.get("spec_net_chg",0)
            cot_text += f"  [bright_magenta]●[/bright_magenta] Report: {cot_snap.get('date','—')}  |  Bias: {bias_color(bias)}  |  Rank: {pr:.0f}th pct\n"
            cot_text += f"  [bright_magenta]●[/bright_magenta] Spec net: {sn:+,}  (WoW: {snc:+,})  |  Commercial net: {cot_snap.get('comm_net',0):+,}\n"
            ct = cot_snap.get("contrarian")
            if ct:
                cot_text += f"  [bold bright_yellow]⚠ CONTRARIAN SIGNAL: {ct[1]}[/bold bright_yellow]\n"
            cd = cot_snap.get("comm_divergence")
            if cd:
                lc = "bright_green" if "BULLISH" in cd else "bright_red"
                cot_text += f"  [{lc}]▸[/{lc}] {cd}\n"
            for cr in s.get("cot_reasons",[]):
                cot_text += f"  [bright_magenta]►[/bright_magenta] {cr}\n"
        else:
            cot_text = "  [dim]No CFTC COT data for this instrument[/dim]\n"

        # Narrative
        narrative_lines = s["narrative"].split("\n")
        narrative_text  = "\n".join(f"  {l}" for l in narrative_lines)

        content = (
            f"{qcolor(s['quality'])}  {dcolor(s['direction'])}  "
            f"[bold white]{sym_name}[/bold white]  "
            f"[dim]Score: {s['score']} | RSI: {s.get('rsi','—')} | "
            f"ATR: {fp(s.get('atr'))} | News: {s['news_count']} items[/dim]\n"
            f"\n[bold dim]━━ TECHNICAL CONFLUENCE ━━[/bold dim]\n"
            f"{tech_text}"
            f"\n[bold dim]━━ NEWS & MACRO CATALYST ━━[/bold dim]\n"
            f"{news_text}"
            f"\n[bold dim]━━ COT POSITIONING (CFTC) ━━[/bold dim]\n"
            f"{cot_text}"
            f"\n[bold dim]━━ ENTRY REASONING ━━[/bold dim]\n"
            f"{narrative_text}"
        )
        panels.append(Panel(
            content,
            title=f"[bold bright_yellow]SETUP DETAIL — {sym_name} {s['time']}[/bold bright_yellow]",
            border_style="bright_yellow", box=box.ROUNDED,
        ))

    if not panels:
        panels.append(Panel(
            "[dim]No A+/A/B+ setups yet. Analysis every 30s.[/dim]",
            border_style="dim", box=box.ROUNDED,
            title="[dim]SETUP DETAIL[/dim]"
        ))
    return panels

def bias_color(bias: str) -> str:
    mapping = {
        "EXTREME LONG":  "[bold bright_green]EXT LONG[/bold bright_green]",
        "BULLISH":       "[bright_green]BULLISH[/bright_green]",
        "NEUTRAL":       "[dim]NEUTRAL[/dim]",
        "BEARISH":       "[bright_red]BEARISH[/bright_red]",
        "EXTREME SHORT": "[bold bright_red]EXT SHORT[/bold bright_red]",
    }
    return mapping.get(bias, f"[dim]{bias}[/dim]")

def pct_bar(pct: float, width: int = 12) -> str:
    """Render a mini progress bar for the 0-100 percentile rank."""
    filled = int(pct / 100 * width)
    bar    = "█" * filled + "░" * (width - filled)
    if pct >= 75:   color = "bright_green"
    elif pct <= 25: color = "bright_red"
    else:           color = "dim"
    return f"[{color}]{bar}[/{color}]"

def build_cot_panel():
    with lock:
        snap = dict(cot_cache)

    if not snap:
        return Panel(
            Align.center(
                "[dim]Fetching COT data from CFTC...\n"
                "Published weekly (Fridays). First load may take ~30s.[/dim]"
            ),
            title="[bold bright_magenta]● COMMITMENT OF TRADERS — CFTC[/bold bright_magenta]",
            border_style="bright_magenta", box=box.ROUNDED,
        )

    t = Table(box=box.SIMPLE_HEAVY, border_style="bright_magenta",
              header_style="bold bright_magenta", show_lines=True,
              title="[bold bright_magenta]● COMMITMENT OF TRADERS — CFTC Weekly[/bold bright_magenta]")
    t.add_column("SYMBOL",       width=13, style="bold white")
    t.add_column("DATE",         width=11, style="dim")
    t.add_column("SPEC NET",     width=12, justify="right")
    t.add_column("WoW ΔSpec",    width=11, justify="right")
    t.add_column("COMM NET",     width=12, justify="right")
    t.add_column("OPEN INT",     width=11, justify="right", style="dim")
    t.add_column("12W RANK",     width=14, justify="center")
    t.add_column("BIAS",         width=12, justify="center")
    t.add_column("CONTRARIAN",   width=10, justify="center")

    # Sort: extreme readings first
    items = sorted(snap.items(),
                   key=lambda x: abs(x[1].get("pct_rank",50)-50), reverse=True)

    for sym, d in items:
        sn    = d.get("spec_net", 0)
        snc   = d.get("spec_net_chg", 0)
        cn    = d.get("comm_net", 0)
        oi    = d.get("open_interest", 0)
        pr    = d.get("pct_rank", 50)
        bias  = d.get("bias","—")
        ct    = d.get("contrarian")
        date  = d.get("date","—")

        name  = sym.replace("BINANCE:","").replace("OANDA:","").replace("_","/")
        sn_s  = f"[bright_green]{sn:+,}[/bright_green]" if sn>0 else f"[bright_red]{sn:+,}[/bright_red]"
        snc_s = f"[bright_green]{snc:+,}[/bright_green]" if snc>0 else f"[bright_red]{snc:+,}[/bright_red]" if snc<0 else f"[dim]{snc:+,}[/dim]"
        cn_s  = f"[bright_green]{cn:+,}[/bright_green]" if cn>0 else f"[bright_red]{cn:+,}[/bright_red]"
        ct_s  = f"[bold bright_yellow]⚠ {ct[0]}[/bold bright_yellow]" if ct else "[dim]—[/dim]"

        t.add_row(
            name, date,
            sn_s, snc_s, cn_s,
            f"{oi:,}",
            f"{pct_bar(pr)} {pr:.0f}%",
            bias_color(bias),
            ct_s,
        )

    return Panel(t, border_style="bright_magenta", box=box.ROUNDED)

def build_news_wall():
    """Full news wall with sentiment labels."""
    with lock:
        items = list(news_cache)

    t = Table(box=box.SIMPLE, show_header=True, header_style="bold bright_blue",
              padding=(0,1))
    t.add_column("SENT",     width=8,  justify="center")
    t.add_column("AGE",      width=6,  justify="right", style="dim")
    t.add_column("SOURCE",   width=12, style="dim")
    t.add_column("HEADLINE", style="white")

    for item in items[:12]:
        text  = (item.get("headline","")+" "+item.get("summary",""))
        score, label = sentiment_score(text)
        ts    = item.get("datetime",0)
        age_h = (time.time()-ts)/3600 if ts else 0
        age_s = f"{age_h:.0f}h" if age_h<24 else f"{age_h/24:.0f}d"
        lc    = "bright_green" if label=="BULLISH" else "bright_red" if label=="BEARISH" else "dim"
        t.add_row(
            f"[{lc}]{label[:4]}[/{lc}]",
            age_s,
            item.get("source","")[:12],
            item.get("headline","")[:100],
        )

    if not items:
        t.add_row("[dim]—[/dim]","—","—","[dim]Fetching news...[/dim]")

    return Panel(t, title="[bold]● LIVE NEWS & SENTIMENT WALL[/bold]",
                 border_style="bright_blue", box=box.ROUNDED)

def outcome_color(status: str) -> str:
    c = {"TP3":"bold bright_green","TP2":"bright_green","TP1":"green",
         "SL":"bold bright_red","OPEN":"bright_yellow","EXPIRED":"dim"}
    s = c.get(status,"white")
    return f"[{s}]{status}[/{s}]"

def build_journal_panel():
    """Recent signals table — last 12 entries."""
    with lock:
        stats = dict(journal_stats)

    recent = stats.get("recent", [])

    # Also pull fresh OPEN signals count from DB
    try:
        with db_connect() as conn:
            open_count = conn.execute(
                "SELECT COUNT(*) FROM signals WHERE status='OPEN'"
            ).fetchone()[0]
    except Exception:
        open_count = 0

    t = Table(
        title=f"[bold bright_cyan]● TRADE JOURNAL  [dim](Open: {open_count})[/dim][/bold bright_cyan]",
        box=box.SIMPLE_HEAVY, border_style="bright_cyan",
        header_style="bold bright_cyan", show_lines=True,
    )
    t.add_column("TIME",    width=8,  style="dim")
    t.add_column("SYMBOL",  width=13, style="bold white")
    t.add_column("GRADE",   width=5,  justify="center")
    t.add_column("DIR",     width=7,  justify="center")
    t.add_column("ENTRY",   width=12, justify="right", style="dim")
    t.add_column("STATUS",  width=8,  justify="center")
    t.add_column("ACT R:R", width=9,  justify="right")
    t.add_column("TGT R:R", width=9,  justify="right", style="dim")

    if not recent:
        t.add_row("—","—","—","—","—","[dim]No trades yet[/dim]","—","—")
    else:
        for r in recent:
            sym    = r["symbol"].replace("BINANCE:","").replace("OANDA:","").replace("_","/")
            status = r["status"]
            arr    = r.get("actual_rr")
            arr_s  = (f"[bright_green]1:{arr:.2f}[/bright_green]" if arr and arr>0
                      else f"[bright_red]{arr:.2f}[/bright_red]"  if arr and arr<0
                      else "[dim]—[/dim]")
            tgt    = r.get("rr_target")
            tgt_s  = f"1:{tgt:.2f}" if tgt else "—"
            ts     = r.get("created_at","")[-8:-3] if r.get("created_at") else "—"
            t.add_row(
                ts, sym,
                qcolor(r["quality"]),
                dcolor(r["direction"]),
                fp(r.get("entry_price")),
                outcome_color(status),
                arr_s, tgt_s,
            )

    return Panel(t, border_style="bright_cyan", box=box.ROUNDED)

def build_stats_panel():
    """Performance stats, feature leaderboard, adaptive weights, top symbols."""
    with lock:
        stats = dict(journal_stats)
    total = stats.get("total", 0)

    if total == 0:
        return Panel(
            Align.center(
                "[dim]No closed trades yet.\n"
                "Stats appear after signals hit TP or SL.\n"
                f"DB: {DB_PATH}[/dim]"
            ),
            title="[bold bright_cyan]● PERFORMANCE STATS & ADAPTIVE LEARNING[/bold bright_cyan]",
            border_style="bright_cyan", box=box.ROUNDED,
        )

    wr    = stats.get("win_rate", 0)
    avg_r = stats.get("avg_rr", 0)
    pf    = stats.get("profit_factor", 0)
    bq    = stats.get("by_quality", {})
    bdir  = stats.get("by_dir", {})
    fstas = stats.get("feature_stats", {})
    top_s = stats.get("top_symbols", [])
    adap  = len(adaptive_weights) > 0 and total >= ADAPTIVE_MIN_TRADES

    wr_c  = "bright_green" if wr>=55 else "bright_red" if wr<45 else "yellow"

    lines = []

    # ── KPIs ──────────────────────────────────────────────────────────────────
    lines.append(f"[bold]OVERALL[/bold]  [dim]{total} closed[/dim]  "
                 f"[{wr_c}]{wr:.1f}% WR[/{wr_c}]  "
                 f"[bold]PF {pf:.2f}[/bold]  "
                 f"Avg R:R [bold]{avg_r:+.2f}[/bold]  "
                 f"Best [bright_green]{stats.get('best_rr',0):+.2f}[/bright_green]  "
                 f"Worst [bright_red]{stats.get('worst_rr',0):+.2f}[/bright_red]")
    lines.append(
        f"TP3 [bright_green]{stats.get('tp3_hits',0)}[/bright_green]  "
        f"TP2 [green]{stats.get('tp2_hits',0)}[/green]  "
        f"TP1 {stats.get('tp1_hits',0)}  "
        f"SL [bright_red]{stats.get('losses',0)}[/bright_red]  "
        f"Expired [dim]{stats.get('expired',0)}[/dim]  "
        f"[dim]│[/dim]  Adaptive: "
        + ("[bright_green]ACTIVE[/bright_green]" if adap
           else f"[dim]pending {max(0,ADAPTIVE_MIN_TRADES-total)} trades[/dim]")
    )

    # ── Quality + Direction side by side ──────────────────────────────────────
    lines.append("")
    row_q = "[bold dim]BY GRADE:[/bold dim]  "
    for q in ("A+","A","B+"):
        d = bq.get(q, {"total":0,"wins":0,"wr":0,"avg_rr":0})
        if d["total"]:
            wrc = "bright_green" if d["wr"]>=55 else "bright_red" if d["wr"]<45 else "yellow"
            row_q += f"{qcolor(q)} [{wrc}]{d['wr']:.0f}%[/{wrc}] {d['wins']}/{d['total']} (R:{d['avg_rr']:+.2f})   "
    lines.append(row_q)

    row_d = "[bold dim]BY DIR  :[/bold dim]  "
    for dirn in ("LONG","SHORT"):
        d = bdir.get(dirn, {"total":0,"wins":0,"wr":0})
        if d["total"]:
            wrc = "bright_green" if d["wr"]>=55 else "bright_red" if d["wr"]<45 else "yellow"
            row_d += f"{dcolor(dirn)} [{wrc}]{d['wr']:.0f}%[/{wrc}] {d['wins']}/{d['total']}   "
    lines.append(row_d)

    # ── Feature leaderboard ────────────────────────────────────────────────────
    lines.append("")
    lines.append("[bold dim]FEATURE LEADERBOARD  (win rate → adaptive weight multiplier)[/bold dim]")
    sorted_feats = sorted(
        fstas.items(),
        key=lambda x: (x[1]["wr"] or 0) if x[1]["total"] >= 3 else -1,
        reverse=True,
    )
    feat_row = ""
    for i, (col, fd) in enumerate(sorted_feats):
        n   = fd["total"]
        wr_ = fd["wr"]
        wt  = adaptive_weights.get(col, 1.0)
        if n < 3:
            bar = "[dim]—[/dim]"
            wrc = "dim"
        else:
            wrc = "bright_green" if (wr_ or 0)>=60 else "bright_red" if (wr_ or 0)<40 else "yellow"
            bar = f"[{wrc}]{wr_:.0f}%[/{wrc}]"
        wt_s = (f"[bright_green]↑×{wt:.2f}[/bright_green]" if wt > 1.05
                else f"[bright_red]↓×{wt:.2f}[/bright_red]" if wt < 0.95
                else f"[dim]×{wt:.2f}[/dim]")
        lines.append(f"  [dim]{fd['label']:<14}[/dim] {bar} [dim]({fd['wins']}/{n})[/dim]  {wt_s}")

    # ── Top symbols ────────────────────────────────────────────────────────────
    if top_s:
        lines.append("")
        lines.append("[bold dim]TOP SYMBOLS[/bold dim]")
        sym_row = ""
        for sym, d in top_s[:6]:
            wr_s = d["w"]/d["t"]*100 if d["t"] else 0
            wrc  = "bright_green" if wr_s>=55 else "bright_red" if wr_s<45 else "yellow"
            avg_ = round(sum(d["rr"])/len(d["rr"]),2) if d["rr"] else 0
            sym_row += (f"[white]{sym}[/white] [{wrc}]{wr_s:.0f}%[/{wrc}] "
                        f"[dim]{d['w']}/{d['t']} avg{avg_:+.2f}[/dim]   ")
        lines.append("  " + sym_row)

    return Panel(
        "\n".join(lines),
        title="[bold bright_cyan]● PERFORMANCE STATS & ADAPTIVE LEARNING[/bold bright_cyan]",
        border_style="bright_cyan", box=box.ROUNDED,
        subtitle=f"[dim]{DB_PATH}[/dim]",
    )

def build_risk_panel():
    with lock:
        n_sym    = sum(1 for md in market_data.values() if md.price)
        n_news   = len(news_cache)
        n_setups = len(setup_alerts)
    content = (
        "[bold bright_red]RISK PROTOCOL[/bold bright_red]\n\n"
        f"[dim]Symbols live   :[/dim] [bold]{n_sym}[/bold]\n"
        f"[dim]News tracked   :[/dim] [bold]{n_news}[/bold]\n"
        f"[dim]Active setups  :[/dim] [bold bright_yellow]{n_setups}[/bold bright_yellow]\n\n"
        "[dim]Max risk/trade  :[/dim] [bold]1–2% capital[/bold]\n"
        "[dim]Min R:R         :[/dim] [bold]1:2.5[/bold]\n"
        "[dim]Filter          :[/dim] [bold]A+ · A · B+[/bold]\n"
        "[dim]Analysis cycle  :[/dim] [bold]30s[/bold]\n"
        "[dim]News cycle      :[/dim] [bold]90s[/bold]\n\n"
        "[dim italic]Stop = final.\nNo averaging down.\nCapital first.[/dim italic]"
    )
    return Panel(content, border_style="bright_red", box=box.ROUNDED,
                 title="[bold bright_red]RISK[/bold bright_red]")

# ── render ─────────────────────────────────────────────────────────────────────
def render():
    run_analysis()

    layout = Layout()
    layout.split_column(
        Layout(name="header",   size=3),
        Layout(name="markets",  size=20),
        Layout(name="setups",   size=17),
        Layout(name="details",  size=30),
        Layout(name="cot_row",  size=16),
        Layout(name="journal",  size=15),
        Layout(name="stats",    size=18),
        Layout(name="bottom",   size=14),
    )

    layout["header"].update(build_header())
    layout["markets"].update(build_market_table())
    layout["setups"].update(build_setup_table())

    detail_panels = build_detail_panels()
    if len(detail_panels) == 3:
        layout["details"].split_row(
            Layout(detail_panels[0]),
            Layout(detail_panels[1]),
            Layout(detail_panels[2]),
        )
    elif len(detail_panels) == 2:
        layout["details"].split_row(
            Layout(detail_panels[0]),
            Layout(detail_panels[1]),
        )
    else:
        layout["details"].update(detail_panels[0])

    layout["cot_row"].update(build_cot_panel())
    layout["journal"].update(build_journal_panel())
    layout["stats"].update(build_stats_panel())

    layout["bottom"].split_row(
        Layout(build_news_wall(), ratio=3),
        Layout(build_risk_panel(), ratio=1),
    )
    return layout

# ── main ───────────────────────────────────────────────────────────────────────
def main():
    console.print(Panel.fit(
        "[bold bright_yellow]TITAN FLOW[/bold bright_yellow] — Initialising...\n"
        "[dim]WebSocket · REST candles · News sentiment engine · ICT/SMC detector[/dim]",
        border_style="bright_yellow"
    ))
    threading.Thread(target=start_websocket,    daemon=True).start()
    threading.Thread(target=background_loader,  daemon=True).start()
    threading.Thread(target=cot_loader,         daemon=True).start()
    threading.Thread(target=monitor_open_signals, daemon=True).start()
    threading.Thread(target=stats_refresh_loop,   daemon=True).start()
    time.sleep(4)
    try:
        with Live(render(), refresh_per_second=1/REFRESH_RATE,
                  screen=True, console=console) as live:
            while True:
                live.update(render())
                time.sleep(REFRESH_RATE)
    except KeyboardInterrupt:
        console.print("\n[bold bright_yellow]TITAN FLOW — Session ended. Capital protected.[/bold bright_yellow]")

if __name__ == "__main__":
    main()
