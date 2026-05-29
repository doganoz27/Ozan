#!/usr/bin/env python3
"""
TITAN FLOW — Institutional-Grade Live Trading Intelligence Terminal
WebSocket real-time streaming + multi-timeframe technical analysis
"""

import websocket
import requests
import json
import threading
import time
import os
import sys
import math
from datetime import datetime, timezone
from collections import deque, defaultdict

# ── dependency check ────────────────────────────────────────────────────────
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
    from rich.text import Text
    from rich.columns import Columns
    from rich import box
    from rich.align import Align
    from rich.rule import Rule
    from rich.style import Style
except ImportError:
    MISSING.append("rich")

if MISSING:
    print(f"[!] Missing packages: {', '.join(MISSING)}")
    print(f"    Run: pip install {' '.join(MISSING)}")
    sys.exit(1)

# ── config ───────────────────────────────────────────────────────────────────
API_KEY  = "d8ce4jpr01qidic7ibt0d8ce4jpr01qidic7ibtg"
BASE_URL = "https://finnhub.io/api/v1"
WS_URL   = f"wss://ws.finnhub.io?token={API_KEY}"

CRYPTO_SYMBOLS  = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT", "BINANCE:BNBUSDT"]
FOREX_SYMBOLS   = ["OANDA:EUR_USD", "OANDA:GBP_USD", "OANDA:USD_JPY", "OANDA:XAU_USD"]
EQUITY_SYMBOLS  = ["NVDA", "AAPL", "SPY", "QQQ"]

ALL_SUBSCRIBE   = CRYPTO_SYMBOLS + FOREX_SYMBOLS
CANDLE_EQUITY   = EQUITY_SYMBOLS
CANDLE_CRYPTO   = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"]

CANDLE_HISTORY  = 200   # bars for indicator calculation
SHORT_HISTORY   = 50    # bars for short-term signals
REFRESH_RATE    = 1.5   # seconds between display updates

console = Console()

# ── data store ───────────────────────────────────────────────────────────────
class MarketData:
    def __init__(self, symbol):
        self.symbol   = symbol
        self.price    = None
        self.prev     = None
        self.high     = None
        self.low      = None
        self.open_    = None
        self.volume   = 0.0
        self.trades   = 0
        self.ticks    = deque(maxlen=500)   # (timestamp, price, volume)
        self.candles  = []                  # OHLCV from REST
        self.last_ws  = None

    @property
    def change_pct(self):
        if self.price and self.prev and self.prev != 0:
            return (self.price - self.prev) / self.prev * 100
        return 0.0

    @property
    def short_name(self):
        s = self.symbol
        if "BINANCE:" in s: return s.replace("BINANCE:", "")
        if "OANDA:"   in s: return s.replace("OANDA:", "").replace("_", "/")
        return s

# Global state
market_data: dict[str, MarketData] = {}
news_cache   = []
setup_alerts = []           # active A+/B+ setups
lock         = threading.Lock()
ws_conn      = None
ws_connected = False
last_analysis_time = 0
analysis_interval  = 30     # seconds between full re-analysis

for sym in ALL_SUBSCRIBE + CANDLE_EQUITY:
    market_data[sym] = MarketData(sym)

# ── REST helpers ─────────────────────────────────────────────────────────────
def fetch_candles(symbol, resolution="60", count=CANDLE_HISTORY):
    """Fetch OHLCV candles from Finnhub REST API."""
    now   = int(time.time())
    from_ = now - count * resolution_to_seconds(resolution)
    params = dict(symbol=symbol, resolution=resolution,
                  from_=from_, to=now, token=API_KEY)
    # Finnhub uses 'from' not 'from_'
    params["from"] = params.pop("from_")
    try:
        r = requests.get(f"{BASE_URL}/stock/candle", params=params, timeout=6)
        data = r.json()
        if data.get("s") == "ok":
            return list(zip(data["t"], data["o"], data["h"], data["l"], data["c"], data["v"]))
    except Exception:
        pass
    # Try crypto candle endpoint
    try:
        r = requests.get(f"{BASE_URL}/crypto/candle", params=params, timeout=6)
        data = r.json()
        if data.get("s") == "ok":
            return list(zip(data["t"], data["o"], data["h"], data["l"], data["c"], data["v"]))
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

def fetch_news():
    try:
        r = requests.get(f"{BASE_URL}/news",
                         params=dict(category="general", token=API_KEY), timeout=5)
        return r.json()[:8]
    except Exception:
        return []

def fetch_forex_rates():
    try:
        r = requests.get(f"{BASE_URL}/forex/rates",
                         params=dict(base="USD", token=API_KEY), timeout=4)
        return r.json()
    except Exception:
        return {}

def resolution_to_seconds(res):
    mapping = {"1": 60, "5": 300, "15": 900, "30": 1800,
               "60": 3600, "D": 86400, "W": 604800}
    return mapping.get(str(res), 3600)

# ── technical indicators ──────────────────────────────────────────────────────
def ema(prices, period):
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(prices[:period]) / period]
    for p in prices[period:]:
        result.append(p * k + result[-1] * (1 - k))
    return result

def sma(prices, period):
    return [sum(prices[i:i+period]) / period
            for i in range(len(prices) - period + 1)]

def rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    deltas = [prices[i+1] - prices[i] for i in range(len(prices)-1)]
    gains  = [max(d, 0) for d in deltas]
    losses = [max(-d, 0) for d in deltas]
    avg_g  = sum(gains[:period])  / period
    avg_l  = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period-1) + gains[i])  / period
        avg_l = (avg_l * (period-1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return round(100 - 100 / (1 + rs), 2)

def macd(prices, fast=12, slow=26, signal=9):
    if len(prices) < slow + signal:
        return None, None, None
    e_fast   = ema(prices, fast)
    e_slow   = ema(prices, slow)
    min_len  = min(len(e_fast), len(e_slow))
    macd_line = [e_fast[-min_len+i] - e_slow[-min_len+i] for i in range(min_len)]
    sig_line  = ema(macd_line, signal)
    if not sig_line:
        return None, None, None
    hist = macd_line[-1] - sig_line[-1]
    return round(macd_line[-1], 6), round(sig_line[-1], 6), round(hist, 6)

def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h = candles[i][2]; l = candles[i][3]; pc = candles[i-1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    return round(sum(trs[-period:]) / period, 6)

def vwap(candles):
    if not candles:
        return None
    cum_tp_v = sum(((c[2]+c[3]+c[4])/3) * c[5] for c in candles)
    cum_v    = sum(c[5] for c in candles)
    if cum_v == 0:
        return None
    return cum_tp_v / cum_v

def bollinger(prices, period=20, std_mult=2.0):
    if len(prices) < period:
        return None, None, None
    s   = sma(prices, period)
    mid = s[-1]
    window = prices[-period:]
    std = math.sqrt(sum((p - mid)**2 for p in window) / period)
    return round(mid - std_mult*std, 6), round(mid, 6), round(mid + std_mult*std, 6)

def stoch_rsi(prices, rsi_period=14, stoch_period=14, smooth_k=3, smooth_d=3):
    """Stochastic RSI"""
    if len(prices) < rsi_period + stoch_period + smooth_k + smooth_d:
        return None, None
    rsi_vals = []
    for i in range(rsi_period, len(prices)+1):
        r = rsi(prices[i-rsi_period-1:i], rsi_period)
        if r is not None:
            rsi_vals.append(r)
    if len(rsi_vals) < stoch_period:
        return None, None
    window = rsi_vals[-stoch_period:]
    mn, mx = min(window), max(window)
    if mx == mn:
        return 50.0, 50.0
    raw_k = (rsi_vals[-1] - mn) / (mx - mn) * 100
    k_vals = [raw_k]
    d = sum(k_vals[-smooth_d:]) / min(smooth_d, len(k_vals))
    return round(raw_k, 2), round(d, 2)

def detect_order_block(candles):
    """Detect bullish/bearish order blocks (last 20 candles)."""
    if len(candles) < 10:
        return None, None
    recent = candles[-20:]
    bull_ob = None
    bear_ob = None
    for i in range(len(recent)-3):
        c = recent[i]
        # Bullish OB: bearish candle followed by strong bullish displacement
        if c[4] < c[1]:  # bearish candle
            next_move = [recent[j] for j in range(i+1, min(i+4, len(recent)))]
            if any(n[4] > c[2] for n in next_move):  # displacement above
                bull_ob = (c[3], c[2])  # low, high of OB candle
        # Bearish OB: bullish candle followed by strong bearish displacement
        if c[4] > c[1]:  # bullish candle
            next_move = [recent[j] for j in range(i+1, min(i+4, len(recent)))]
            if any(n[4] < c[3] for n in next_move):  # displacement below
                bear_ob = (c[3], c[2])
    return bull_ob, bear_ob

def detect_fvg(candles):
    """Fair Value Gap detection."""
    if len(candles) < 3:
        return None, None
    fvg_bull = None
    fvg_bear = None
    for i in range(len(candles)-2):
        c1, c2, c3 = candles[i], candles[i+1], candles[i+2]
        # Bullish FVG: c1 high < c3 low
        if c1[2] < c3[3]:
            fvg_bull = (c1[2], c3[3])
        # Bearish FVG: c1 low > c3 high
        if c1[3] > c3[2]:
            fvg_bear = (c1[3], c3[2])
    return fvg_bull, fvg_bear

def detect_liquidity_sweep(candles, lookback=20):
    """Detect recent stop hunt / liquidity sweep."""
    if len(candles) < lookback + 2:
        return None
    recent   = candles[-lookback-2:-2]
    last_two = candles[-2:]
    prev_highs = [c[2] for c in recent]
    prev_lows  = [c[3] for c in recent]
    swing_high = max(prev_highs)
    swing_low  = min(prev_lows)
    last = candles[-1]
    # Bearish sweep: wicked above swing high then closed below
    if last[2] > swing_high and last[4] < swing_high:
        return ("BEARISH_SWEEP", swing_high, last[4])
    # Bullish sweep: wicked below swing low then closed above
    if last[3] < swing_low and last[4] > swing_low:
        return ("BULLISH_SWEEP", swing_low, last[4])
    return None

# ── setup scoring engine ──────────────────────────────────────────────────────
def score_setup(symbol, candles, current_price):
    """
    Returns (quality, direction, entry_zone, sl, tp1, tp2, tp3, rr, reasons)
    quality: 'A+', 'A', 'B+', 'B', None
    """
    if len(candles) < 50:
        return None, None, None, None, None, None, None, None, []

    closes  = [c[4] for c in candles]
    highs   = [c[2] for c in candles]
    lows    = [c[3] for c in candles]
    vols    = [c[5] for c in candles]

    # --- Indicators ---
    e9   = ema(closes, 9)
    e20  = ema(closes, 20)
    e50  = ema(closes, 50)
    e200 = ema(closes, 200) if len(closes) >= 200 else []

    rsi_val  = rsi(closes[-50:], 14)
    macd_l, macd_s, macd_h = macd(closes)
    atr_val  = atr(candles)
    vwap_val = vwap(candles[-24:])  # ~1 day of hourly
    bb_low, bb_mid, bb_high = bollinger(closes)
    sweep    = detect_liquidity_sweep(candles)
    bull_ob, bear_ob = detect_order_block(candles)
    fvg_bull, fvg_bear = detect_fvg(candles[-10:])

    # Volume momentum
    avg_vol  = sum(vols[-20:]) / 20 if vols else 1
    last_vol = vols[-1] if vols else 0
    vol_spike = last_vol > avg_vol * 1.5

    score_long  = 0
    score_short = 0
    reasons_long  = []
    reasons_short = []

    # ── EMA alignment ─────────────────────────
    if e9 and e20 and e50:
        if e9[-1] > e20[-1] > e50[-1]:
            score_long += 2
            reasons_long.append("EMA 9>20>50 bullish stack")
        elif e9[-1] < e20[-1] < e50[-1]:
            score_short += 2
            reasons_short.append("EMA 9<20<50 bearish stack")

    if e200:
        if current_price > e200[-1]:
            score_long  += 1
            reasons_long.append("Price above EMA200")
        else:
            score_short += 1
            reasons_short.append("Price below EMA200")

    # ── RSI ───────────────────────────────────
    if rsi_val is not None:
        if 40 <= rsi_val <= 60:
            score_long  += 1; score_short += 1
            reasons_long.append(f"RSI neutral {rsi_val} — momentum room")
            reasons_short.append(f"RSI neutral {rsi_val} — momentum room")
        elif rsi_val < 35:
            score_long += 2
            reasons_long.append(f"RSI oversold {rsi_val} — reversal fuel")
        elif rsi_val > 65:
            score_short += 2
            reasons_short.append(f"RSI overbought {rsi_val} — reversal fuel")

    # ── MACD ──────────────────────────────────
    if macd_h is not None:
        if macd_h > 0 and macd_l > macd_s:
            score_long += 2
            reasons_long.append("MACD bullish crossover / positive histogram")
        elif macd_h < 0 and macd_l < macd_s:
            score_short += 2
            reasons_short.append("MACD bearish crossover / negative histogram")

    # ── VWAP ──────────────────────────────────
    if vwap_val:
        if current_price > vwap_val * 1.001:
            score_long  += 1
            reasons_long.append("Price above VWAP — institutional bid")
        elif current_price < vwap_val * 0.999:
            score_short += 1
            reasons_short.append("Price below VWAP — institutional pressure")

    # ── Bollinger Bands ───────────────────────
    if bb_low and bb_high:
        if current_price <= bb_low * 1.002:
            score_long += 2
            reasons_long.append("Price at/below lower Bollinger — mean reversion")
        elif current_price >= bb_high * 0.998:
            score_short += 2
            reasons_short.append("Price at/above upper Bollinger — mean reversion")

    # ── Liquidity sweep ───────────────────────
    if sweep:
        stype, level, close_price = sweep
        if stype == "BULLISH_SWEEP":
            score_long += 3
            reasons_long.append(f"BULLISH LIQUIDITY SWEEP at {level:.4f} — stop hunt complete")
        elif stype == "BEARISH_SWEEP":
            score_short += 3
            reasons_short.append(f"BEARISH LIQUIDITY SWEEP at {level:.4f} — stop hunt complete")

    # ── Order Block ───────────────────────────
    if bull_ob:
        ob_mid = (bull_ob[0] + bull_ob[1]) / 2
        if abs(current_price - ob_mid) / ob_mid < 0.015:
            score_long += 2
            reasons_long.append(f"Price at BULLISH ORDER BLOCK {bull_ob[0]:.4f}–{bull_ob[1]:.4f}")

    if bear_ob:
        ob_mid = (bear_ob[0] + bear_ob[1]) / 2
        if abs(current_price - ob_mid) / ob_mid < 0.015:
            score_short += 2
            reasons_short.append(f"Price at BEARISH ORDER BLOCK {bear_ob[0]:.4f}–{bear_ob[1]:.4f}")

    # ── FVG ───────────────────────────────────
    if fvg_bull:
        if fvg_bull[0] <= current_price <= fvg_bull[1]:
            score_long += 2
            reasons_long.append(f"Price inside BULLISH FVG {fvg_bull[0]:.4f}–{fvg_bull[1]:.4f}")

    if fvg_bear:
        if fvg_bear[1] <= current_price <= fvg_bear[0]:
            score_short += 2
            reasons_short.append(f"Price inside BEARISH FVG {fvg_bear[1]:.4f}–{fvg_bear[0]:.4f}")

    # ── Volume spike ──────────────────────────
    if vol_spike:
        score_long  += 1; score_short += 1
        reasons_long.append("Volume spike — institutional activity")
        reasons_short.append("Volume spike — institutional activity")

    # ── Momentum structure ────────────────────
    recent_highs = highs[-10:]
    recent_lows  = lows[-10:]
    if len(recent_highs) >= 3:
        if recent_highs[-1] > recent_highs[-2] > recent_highs[-3] and \
           recent_lows[-1]  > recent_lows[-2]  > recent_lows[-3]:
            score_long += 2
            reasons_long.append("Higher highs + higher lows — bullish structure")
        elif recent_highs[-1] < recent_highs[-2] < recent_highs[-3] and \
             recent_lows[-1]  < recent_lows[-2]  < recent_lows[-3]:
            score_short += 2
            reasons_short.append("Lower highs + lower lows — bearish structure")

    # ── Determine direction & quality ─────────
    if score_long >= score_short:
        direction = "LONG"
        score     = score_long
        reasons   = reasons_long
    else:
        direction = "SHORT"
        score     = score_short
        reasons   = reasons_short

    if score >= 10:
        quality = "A+"
    elif score >= 8:
        quality = "A"
    elif score >= 6:
        quality = "B+"
    elif score >= 4:
        quality = "B"
    else:
        return None, None, None, None, None, None, None, None, []

    if not atr_val or atr_val == 0:
        return None, None, None, None, None, None, None, None, []

    # ── Entry / SL / TP calculation ───────────
    if direction == "LONG":
        entry_low  = current_price - atr_val * 0.3
        entry_high = current_price + atr_val * 0.1
        sl         = current_price - atr_val * 1.5
        tp1        = current_price + atr_val * 1.5
        tp2        = current_price + atr_val * 3.0
        tp3        = current_price + atr_val * 5.0
    else:
        entry_low  = current_price - atr_val * 0.1
        entry_high = current_price + atr_val * 0.3
        sl         = current_price + atr_val * 1.5
        tp1        = current_price - atr_val * 1.5
        tp2        = current_price - atr_val * 3.0
        tp3        = current_price - atr_val * 5.0

    risk   = abs(current_price - sl)
    reward = abs(tp3 - current_price)
    rr     = round(reward / risk, 2) if risk > 0 else 0

    if rr < 2.5:
        return None, None, None, None, None, None, None, None, []

    return (quality, direction,
            (entry_low, entry_high),
            sl, tp1, tp2, tp3, rr, reasons)

# ── background data loader ────────────────────────────────────────────────────
def background_loader():
    """Periodically fetch REST candle data and quotes for equities."""
    global news_cache
    while True:
        try:
            # Fetch equity quotes
            for sym in EQUITY_SYMBOLS:
                q = fetch_quote(sym)
                if q and q.get("c"):
                    with lock:
                        md = market_data[sym]
                        md.price = q["c"]
                        md.prev  = q["pc"]
                        md.high  = q["h"]
                        md.low   = q["l"]
                        md.open_ = q["o"]

            # Fetch candles for equities + crypto
            for sym in CANDLE_EQUITY + CANDLE_CRYPTO:
                candles = fetch_candles(sym, resolution="60", count=CANDLE_HISTORY)
                if candles:
                    with lock:
                        if sym not in market_data:
                            market_data[sym] = MarketData(sym)
                        market_data[sym].candles = candles
                        if candles:
                            market_data[sym].price = candles[-1][4]

            # News
            n = fetch_news()
            if n:
                news_cache = n

        except Exception as e:
            pass

        time.sleep(45)

# ── analysis engine ───────────────────────────────────────────────────────────
def run_analysis():
    """Score all symbols with enough data. Return sorted list of setups."""
    global last_analysis_time, setup_alerts
    now = time.time()
    if now - last_analysis_time < analysis_interval:
        return
    last_analysis_time = now
    results = []
    with lock:
        for sym, md in market_data.items():
            if not md.price:
                continue
            candles = list(md.candles) if md.candles else []
            # Build synthetic candles from ticks if no REST candles
            if not candles and len(md.ticks) >= 50:
                candles = ticks_to_candles(md.ticks, 60)
            if len(candles) < 30:
                continue
            try:
                quality, direction, entry, sl, tp1, tp2, tp3, rr, reasons = \
                    score_setup(sym, candles, md.price)
                if quality in ("A+", "A", "B+"):
                    results.append({
                        "symbol":    sym,
                        "quality":   quality,
                        "direction": direction,
                        "price":     md.price,
                        "entry":     entry,
                        "sl":        sl,
                        "tp1":       tp1,
                        "tp2":       tp2,
                        "tp3":       tp3,
                        "rr":        rr,
                        "reasons":   reasons,
                        "time":      datetime.now().strftime("%H:%M:%S"),
                        "atr":       atr(candles),
                        "rsi":       rsi([c[4] for c in candles][-50:], 14),
                    })
            except Exception:
                pass
    # Sort: A+ first, then A, then B+
    order = {"A+": 0, "A": 1, "B+": 2}
    results.sort(key=lambda x: (order.get(x["quality"], 9), -x["rr"]))
    setup_alerts = results

def ticks_to_candles(ticks, interval_sec):
    """Aggregate tick data into OHLCV candles."""
    if not ticks:
        return []
    ticks = list(ticks)
    start = ticks[0][0] - (ticks[0][0] % interval_sec)
    candles = []
    o = h = l = c = None
    v = 0
    bucket = start
    for ts, price, vol in ticks:
        b = ts - (ts % interval_sec)
        if b != bucket and o is not None:
            candles.append((bucket, o, h, l, c, v))
            o = h = l = c = None; v = 0; bucket = b
        if o is None:
            o = h = l = price
        h = max(h, price)
        l = min(l, price)
        c = price
        v += vol
    if o is not None:
        candles.append((bucket, o, h, l, c, v))
    return candles

# ── WebSocket handlers ────────────────────────────────────────────────────────
def on_message(ws, message):
    try:
        data = json.loads(message)
        if data.get("type") == "trade":
            for t in data["data"]:
                sym   = t["s"]
                price = t["p"]
                vol   = t.get("v", 0)
                ts    = t.get("t", int(time.time()*1000)) // 1000
                with lock:
                    if sym not in market_data:
                        market_data[sym] = MarketData(sym)
                    md = market_data[sym]
                    if md.price is None:
                        md.open_ = price
                        md.prev  = price
                    if md.high is None or price > md.high:
                        md.high = price
                    if md.low  is None or price < md.low:
                        md.low  = price
                    md.prev  = md.price or price
                    md.price = price
                    md.volume += vol
                    md.trades += 1
                    md.ticks.append((ts, price, vol))
                    md.last_ws = datetime.now().strftime("%H:%M:%S")
    except Exception:
        pass

def on_error(ws, error):
    global ws_connected
    ws_connected = False

def on_close(ws, code, msg):
    global ws_connected
    ws_connected = False

def on_open(ws):
    global ws_connected
    ws_connected = True
    for sym in ALL_SUBSCRIBE:
        ws.send(json.dumps({"type": "subscribe", "symbol": sym}))

def start_websocket():
    global ws_conn
    while True:
        try:
            ws_conn = websocket.WebSocketApp(
                WS_URL,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open,
            )
            ws_conn.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            pass
        time.sleep(5)

# ── display helpers ───────────────────────────────────────────────────────────
def fmt_price(p, decimals=4):
    if p is None: return "[dim]N/A[/dim]"
    if p > 1000:
        return f"{p:,.2f}"
    elif p > 10:
        return f"{p:.3f}"
    else:
        return f"{p:.5f}"

def color_pct(pct):
    if pct > 0:    return f"[bright_green]+{pct:.2f}%[/bright_green]"
    elif pct < 0:  return f"[bright_red]{pct:.2f}%[/bright_red]"
    else:          return f"[dim]{pct:.2f}%[/dim]"

def quality_color(q):
    colors = {"A+": "bright_yellow", "A": "green", "B+": "cyan", "B": "blue"}
    return f"[{colors.get(q,'white')}]{q}[/{colors.get(q,'white')}]"

def dir_color(d):
    return f"[bright_green]{d}[/bright_green]" if d == "LONG" else f"[bright_red]{d}[/bright_red]"

# ── main display ──────────────────────────────────────────────────────────────
def build_header():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M:%S UTC")
    status = "[bright_green]● WS LIVE[/bright_green]" if ws_connected else "[bright_red]● WS OFFLINE[/bright_red]"
    return Panel(
        Align.center(
            f"[bold bright_yellow]TITAN FLOW[/bold bright_yellow]  "
            f"[dim]│[/dim]  [bold white]INSTITUTIONAL INTELLIGENCE TERMINAL[/bold white]  "
            f"[dim]│[/dim]  {status}  [dim]│[/dim]  [dim]{now}[/dim]"
        ),
        style="bold",
        box=box.HEAVY,
        border_style="bright_yellow",
    )

def build_market_table():
    t = Table(
        title="[bold]LIVE MARKET FEED[/bold]",
        box=box.SIMPLE_HEAVY,
        border_style="bright_blue",
        header_style="bold bright_blue",
        show_lines=True,
    )
    t.add_column("SYMBOL",    style="bold white",   width=18)
    t.add_column("PRICE",     style="bright_white", width=14, justify="right")
    t.add_column("CHANGE",    width=10,              justify="right")
    t.add_column("HIGH",      style="bright_green",  width=13, justify="right")
    t.add_column("LOW",       style="bright_red",    width=13, justify="right")
    t.add_column("VOLUME",    style="dim",           width=12, justify="right")
    t.add_column("LAST TICK", style="dim",           width=10)

    groups = [
        ("── CRYPTO ──", CRYPTO_SYMBOLS),
        ("── FOREX / METALS ──", FOREX_SYMBOLS),
        ("── EQUITIES ──", EQUITY_SYMBOLS),
    ]
    with lock:
        for label, syms in groups:
            t.add_row(f"[dim]{label}[/dim]", "", "", "", "", "", "")
            for sym in syms:
                md = market_data.get(sym)
                if not md:
                    continue
                name = md.short_name
                p    = fmt_price(md.price)
                chg  = color_pct(md.change_pct)
                h    = fmt_price(md.high) if md.high else "[dim]—[/dim]"
                l    = fmt_price(md.low)  if md.low  else "[dim]—[/dim]"
                vol  = f"{md.volume:,.1f}" if md.volume else "[dim]—[/dim]"
                last = md.last_ws or "[dim]—[/dim]"
                t.add_row(name, p, chg, h, l, vol, last)
    return t

def build_setup_table():
    alerts = list(setup_alerts)
    if not alerts:
        return Panel(
            Align.center(
                "[bold bright_yellow]SCANNING FOR INSTITUTIONAL SETUPS...[/bold bright_yellow]\n"
                "[dim]A+, A, B+ setups will appear here. "
                "Minimum RR 1:2.5 required.[/dim]"
            ),
            title="[bold bright_yellow]● ACTIVE SETUPS[/bold bright_yellow]",
            border_style="bright_yellow",
            box=box.HEAVY,
        )

    t = Table(
        title="[bold bright_yellow]● ACTIVE SETUPS — A+ / A / B+[/bold bright_yellow]",
        box=box.SIMPLE_HEAVY,
        border_style="bright_yellow",
        header_style="bold bright_yellow",
        show_lines=True,
    )
    t.add_column("QUALITY",   width=8,  justify="center")
    t.add_column("SYMBOL",    width=16, style="bold white")
    t.add_column("DIR",       width=7,  justify="center")
    t.add_column("PRICE",     width=13, justify="right")
    t.add_column("ENTRY ZONE",width=22, justify="right")
    t.add_column("STOP LOSS", width=13, justify="right", style="bright_red")
    t.add_column("TP1",       width=13, justify="right", style="green")
    t.add_column("TP2",       width=13, justify="right", style="bright_green")
    t.add_column("TP3",       width=13, justify="right", style="bright_yellow")
    t.add_column("R:R",       width=8,  justify="center")
    t.add_column("RSI",       width=7,  justify="center")
    t.add_column("TIME",      width=10)

    for s in alerts:
        entry_str = f"{fmt_price(s['entry'][0])} – {fmt_price(s['entry'][1])}"
        rsi_val   = s.get("rsi")
        rsi_str   = f"{rsi_val:.0f}" if rsi_val else "—"
        rsi_color = ("bright_green" if rsi_val and rsi_val < 40
                     else "bright_red" if rsi_val and rsi_val > 65
                     else "white")
        t.add_row(
            quality_color(s["quality"]),
            s["symbol"].replace("BINANCE:","").replace("OANDA:","").replace("_","/"),
            dir_color(s["direction"]),
            fmt_price(s["price"]),
            entry_str,
            fmt_price(s["sl"]),
            fmt_price(s["tp1"]),
            fmt_price(s["tp2"]),
            fmt_price(s["tp3"]),
            f"[bold]1:{s['rr']}[/bold]",
            f"[{rsi_color}]{rsi_str}[/{rsi_color}]",
            s["time"],
        )
    return Panel(t, border_style="bright_yellow", box=box.HEAVY)

def build_detail_panels():
    """Show detailed confluence reasons for top 2 setups."""
    alerts = list(setup_alerts)[:2]
    panels = []
    for s in alerts:
        reasons_text = "\n".join(f"  [bright_green]✓[/bright_green] {r}" for r in s["reasons"])
        entry_l, entry_h = s["entry"]
        content = (
            f"[bold bright_yellow]{s['quality']}[/bold bright_yellow]  "
            f"{dir_color(s['direction'])}  "
            f"[white]{s['symbol'].replace('BINANCE:','').replace('OANDA:','').replace('_','/')}[/white]\n\n"
            f"[dim]ENTRY :[/dim]  [bright_white]{fmt_price(entry_l)} – {fmt_price(entry_h)}[/bright_white]\n"
            f"[dim]SL    :[/dim]  [bright_red]{fmt_price(s['sl'])}[/bright_red]\n"
            f"[dim]TP1   :[/dim]  [green]{fmt_price(s['tp1'])}[/green]\n"
            f"[dim]TP2   :[/dim]  [bright_green]{fmt_price(s['tp2'])}[/bright_green]\n"
            f"[dim]TP3   :[/dim]  [bright_yellow]{fmt_price(s['tp3'])}[/bright_yellow]\n"
            f"[dim]R:R   :[/dim]  [bold]1:{s['rr']}[/bold]\n\n"
            f"[bold dim]CONFLUENCE:[/bold dim]\n{reasons_text}"
        )
        panels.append(Panel(content, border_style="bright_yellow", box=box.ROUNDED,
                            title=f"[bold]SETUP DETAIL — {s['time']}[/bold]"))
    if not panels:
        panels.append(Panel(
            "[dim]No A+/A/B+ setups detected yet.\nAnalysis runs every 30 seconds.[/dim]",
            border_style="dim", box=box.ROUNDED,
            title="[dim]SETUP DETAIL[/dim]"
        ))
    return panels

def build_news_panel():
    t = Table(box=box.SIMPLE, show_header=False, padding=(0,1))
    t.add_column("", style="dim", width=2)
    t.add_column("headline", style="white")
    for item in news_cache[:6]:
        headline = item.get("headline", "")[:90]
        source   = item.get("source", "")
        t.add_row("▸", f"{headline} [dim]({source})[/dim]")
    if not news_cache:
        t.add_row("▸", "[dim]Fetching news...[/dim]")
    return Panel(t,
                 title="[bold]MACRO NEWS FEED[/bold]",
                 border_style="bright_blue",
                 box=box.ROUNDED)

def build_risk_panel():
    content = (
        "[bold bright_red]RISK MANAGEMENT PROTOCOL[/bold bright_red]\n\n"
        "[dim]Max risk per trade   :[/dim] [bold bright_yellow]1–2% of capital[/bold bright_yellow]\n"
        "[dim]Min R:R accepted     :[/dim] [bold]1:2.5[/bold]\n"
        "[dim]Setup filter         :[/dim] [bold]A+ · A · B+ only[/bold]\n"
        "[dim]Analysis interval    :[/dim] [bold]30 seconds[/bold]\n"
        "[dim]WS stream symbols    :[/dim] [bold]8 live[/bold]\n"
        "[dim]Candle history       :[/dim] [bold]200 bars[/bold]\n\n"
        "[dim]Never average into a losing trade.\n"
        "Stop out is final. Capital first.[/dim]"
    )
    return Panel(content, border_style="bright_red", box=box.ROUNDED,
                 title="[bold bright_red]RISK[/bold bright_red]")

def render():
    """Compose the full terminal layout."""
    run_analysis()

    layout = Layout()
    layout.split_column(
        Layout(name="header",  size=3),
        Layout(name="markets", size=18),
        Layout(name="setups",  size=16),
        Layout(name="details", size=16),
        Layout(name="bottom",  size=10),
    )

    layout["header"].update(build_header())
    layout["markets"].update(build_market_table())
    layout["setups"].update(build_setup_table())

    detail_panels = build_detail_panels()
    if len(detail_panels) >= 2:
        layout["details"].split_row(
            Layout(detail_panels[0]),
            Layout(detail_panels[1]),
        )
    else:
        layout["details"].update(detail_panels[0])

    layout["bottom"].split_row(
        Layout(build_news_panel(), ratio=3),
        Layout(build_risk_panel(), ratio=1),
    )

    return layout

# ── entry point ───────────────────────────────────────────────────────────────
def main():
    console.print(Panel.fit(
        "[bold bright_yellow]TITAN FLOW[/bold bright_yellow] — Starting up...\n"
        "[dim]Loading WebSocket · Fetching initial data · Building indicators[/dim]",
        border_style="bright_yellow"
    ))

    # Start WebSocket in background thread
    ws_thread = threading.Thread(target=start_websocket, daemon=True)
    ws_thread.start()

    # Start REST loader in background thread
    loader_thread = threading.Thread(target=background_loader, daemon=True)
    loader_thread.start()

    # Give time for initial connections
    time.sleep(3)

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
