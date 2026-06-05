#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TITAN FLOW — Institutional Trading Intelligence Terminal
Crypto (WebSocket) · Forex · Metals · Oil · Equities · COT · News · Journal
"""

import sys, os, time, math, json, threading, sqlite3
from datetime import datetime, timezone
from collections import deque

# ── dependency check ──────────────────────────────────────────────────────────
def check_deps():
    missing = []
    for pkg in ["rich", "requests", "websocket", "yfinance", "numpy"]:
        try: __import__(pkg)
        except ImportError: missing.append(pkg if pkg != "websocket" else "websocket-client")
    if missing:
        print(f"Eksik paket: pip install {' '.join(missing)}")
        sys.exit(1)
check_deps()

import requests, websocket, yfinance as yf
from rich.console import Console
from rich.table   import Table
from rich.panel   import Panel
from rich.layout  import Layout
from rich.live    import Live
from rich.align   import Align
from rich         import box

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
API_KEY  = "d8ce4jpr01qidic7ibt0d8ce4jpr01qidic7ibtg"
BASE_URL = "https://finnhub.io/api/v1"
WS_URL   = f"wss://ws.finnhub.io?token={API_KEY}"
DB_PATH  = os.path.join(os.path.expanduser("~"), "titan_journal.db")

REFRESH_SEC       = 2
ANALYSIS_SEC      = 30
DEDUP_SEC         = 7200
MIN_TRADES_ADAPT  = 5

# ── Trade212 CFD Account Settings ────────────────────────────────────────────
ACCOUNT = {
    "balance":        50.0,    # Starting balance £
    "risk_pct":       0.01,    # 1% default risk per trade
    "max_risk_pct":   0.02,    # 2% max risk per trade
    "max_daily_dd":   0.03,    # 3% max daily drawdown
    "max_weekly_dd":  0.05,    # 5% max weekly drawdown
    "compounding":    True,
}

# Trade212 CFD leverage by asset class
LEVERAGE = {
    "forex":   30,
    "gold":    20,
    "silver":  20,
    "indices": 20,
    "oil":     10,
    "stocks":   5,
    "crypto":   2,
}

def get_leverage(sym):
    if sym in ["XAU/USD"]:                           return LEVERAGE["gold"]
    if sym in ["XAG/USD"]:                           return LEVERAGE["silver"]
    if sym in ["WTI","BRENT"]:                       return LEVERAGE["oil"]
    if sym in ["SPY","QQQ","NVDA","AAPL","MSFT","TSLA"]: return LEVERAGE["stocks"]
    if sym.endswith("USDT"):                         return LEVERAGE["crypto"]
    return LEVERAGE["forex"]

def get_asset_class(sym):
    if sym in ["XAU/USD"]:                           return "GOLD"
    if sym in ["XAG/USD"]:                           return "SILVER"
    if sym in ["WTI","BRENT"]:                       return "OIL"
    if sym in ["SPY","QQQ","NVDA","AAPL","MSFT","TSLA"]: return "STOCKS"
    if sym.endswith("USDT"):                         return "CRYPTO"
    return "FOREX"

# Currency exposure map: sym -> {currency: +1 long / -1 short for LONG direction}
CURR_EXP_MAP = {
    "EUR/USD":{"EUR":+1,"USD":-1}, "GBP/USD":{"GBP":+1,"USD":-1},
    "USD/JPY":{"USD":+1,"JPY":-1}, "USD/CHF":{"USD":+1,"CHF":-1},
    "AUD/USD":{"AUD":+1,"USD":-1}, "USD/CAD":{"USD":+1,"CAD":-1},
    "NZD/USD":{"NZD":+1,"USD":-1}, "EUR/GBP":{"EUR":+1,"GBP":-1},
    "EUR/JPY":{"EUR":+1,"JPY":-1}, "GBP/JPY":{"GBP":+1,"JPY":-1},
    "EUR/CHF":{"EUR":+1,"CHF":-1}, "AUD/JPY":{"AUD":+1,"JPY":-1},
    "GBP/CHF":{"GBP":+1,"CHF":-1}, "XAU/USD":{"GOLD":+1,"USD":-1},
    "XAG/USD":{"SILVER":+1,"USD":-1}, "WTI":{"OIL":+1,"USD":-1},
    "BRENT":{"OIL":+1,"USD":-1},
}

# Correlation clusters (pairs that move together, treat as 1 risk unit)
CORR_CLUSTERS = [
    {"label":"USD Strength",  "syms":["EUR/USD","GBP/USD","AUD/USD","NZD/USD","XAU/USD"]},
    {"label":"Risk-On",       "syms":["SPY","QQQ","AUD/USD","NZD/USD"]},
    {"label":"Oil-CAD",       "syms":["WTI","BRENT","USD/CAD"]},
    {"label":"Safe Haven",    "syms":["XAU/USD","USD/CHF","USD/JPY"]},
    {"label":"EUR Complex",   "syms":["EUR/USD","EUR/GBP","EUR/JPY","EUR/CHF"]},
]

# Yahoo Finance tickers
YF_SYMBOLS = {
    # Forex Majors
    "EUR/USD":"EURUSD=X", "GBP/USD":"GBPUSD=X", "USD/JPY":"JPY=X",
    "USD/CHF":"CHF=X",    "AUD/USD":"AUDUSD=X", "USD/CAD":"CAD=X",
    "NZD/USD":"NZDUSD=X",
    # Forex Crosses
    "EUR/GBP":"EURGBP=X", "EUR/JPY":"EURJPY=X", "GBP/JPY":"GBPJPY=X",
    "EUR/CHF":"EURCHF=X", "AUD/JPY":"AUDJPY=X", "GBP/CHF":"GBPCHF=X",
    # Metals
    "XAU/USD":"GC=F", "XAG/USD":"SI=F",
    # Oil
    "WTI":"CL=F", "BRENT":"BZ=F",
}

# Finnhub REST equities
EQ_SYMBOLS = ["NVDA","AAPL","SPY","QQQ","MSFT","TSLA","AMZN","META","GOOGL","JPM"]

ALL_SYMBOLS = list(YF_SYMBOLS.keys()) + EQ_SYMBOLS

DISPLAY_GROUPS = [
    ("FOREX MAJORS",  ["EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD","USD/CAD","NZD/USD"]),
    ("FOREX CROSSES", ["EUR/GBP","EUR/JPY","GBP/JPY","EUR/CHF","AUD/JPY","GBP/CHF"]),
    ("METALS & OIL",  ["XAU/USD","XAG/USD","WTI","BRENT"]),
    ("EQUITIES",      EQ_SYMBOLS),
]

COT_MAP = {
    "EUR/USD":"EURO FX - CHICAGO MERCANTILE EXCHANGE",
    "GBP/USD":"BRITISH POUND - CHICAGO MERCANTILE EXCHANGE",
    "USD/JPY":"JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    "USD/CHF":"SWISS FRANC - CHICAGO MERCANTILE EXCHANGE",
    "AUD/USD":"AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "USD/CAD":"CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "NZD/USD":"NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE",
    "XAU/USD":"GOLD - COMMODITY EXCHANGE INC.",
    "XAG/USD":"SILVER - COMMODITY EXCHANGE INC.",
    "WTI":    "CRUDE OIL, LIGHT SWEET - NEW YORK MERCANTILE EXCHANGE",
    "BRENT":  "BRENT LAST DAY FINANCIAL - ICE FUTURES EUROPE",
    "SPY":    "S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE",
}

NEWS_KW = {
    "EUR/USD":["euro","ecb","eurozone","lagarde"],
    "GBP/USD":["pound","sterling","boe","bank of england"],
    "USD/JPY":["yen","boj","bank of japan","japan"],
    "USD/CHF":["franc","snb","swiss"],
    "AUD/USD":["aussie","rba","australia"],
    "USD/CAD":["loonie","boc","canada"],
    "NZD/USD":["kiwi","rbnz","new zealand"],
    "EUR/GBP":["euro","pound","ecb","boe"],
    "EUR/JPY":["euro","yen"], "GBP/JPY":["pound","yen"],
    "EUR/CHF":["euro","franc"], "AUD/JPY":["aussie","yen"],
    "GBP/CHF":["pound","franc"],
    "XAU/USD":["gold","xau","bullion","safe haven"],
    "XAG/USD":["silver","xag"],
    "WTI":    ["wti","crude","oil","opec","barrel"],
    "BRENT":  ["brent","crude","opec","oil"],
    "NVDA":   ["nvidia","nvda","gpu","ai chip"],
    "AAPL":   ["apple","aapl","iphone"],
    "SPY":    ["s&p 500","spx","equity market","fed"],
    "QQQ":    ["nasdaq","qqq","tech stocks"],
    "MSFT":   ["microsoft","msft","azure"],
    "TSLA":   ["tesla","tsla","elon","musk"],
}

BULL_W = ["surge","rally","soar","gain","jump","rise","breakout","bullish","beat",
          "upgrade","buy","inflow","record","growth","recovery","rebound","dovish",
          "stimulus","profit","approval","deal","expansion","rate cut","fed pivot"]
BEAR_W = ["drop","fall","crash","plunge","decline","selloff","bearish","miss",
          "downgrade","sell","outflow","ban","hack","lawsuit","fine","hawkish",
          "inflation","recession","default","war","tariff","loss","rate hike","risk-off"]

# ── Telegram (CHAT_ID ayarlı — BOT_TOKEN'ı @BotFather'dan al ve buraya yaz) ──
TELEGRAM_TOKEN   = "7731993816:AAHZ1gRt7xxolBzEA9ptAlGv48igZfIuGL0"
TELEGRAM_CHAT_ID = "8237226783"

# ── High-impact keyword → base importance score ───────────────────────────────
HIGH_IMP_KW = {
    # Central bank decisions — genuinely market-moving
    "fomc":90,"federal reserve":82,"rate decision":88,"rate hike":85,"rate cut":85,
    "emergency meeting":92,"quantitative tightening":75,"quantitative easing":78,
    "powell":60,"lagarde":60,"ueda":60,"bailey":60,"jordan":58,
    "ecb decision":88,"boe decision":88,"boj decision":88,"snb decision":82,
    "rba decision":78,"boc decision":78,"rbnz decision":75,
    # Inflation / employment — major
    "cpi":80,"core inflation":82,"pce":80,"non-farm payroll":88,"nfp":88,
    "unemployment rate":75,"gdp":72,"recession":78,"stagflation":80,
    # Geopolitical — only truly major events
    "war":82,"nuclear":90,"invasion":88,"sanctions":75,
    # Financial crisis — rare but major
    "default":88,"bank run":90,"flash crash":88,"systemic":82,
    # Earnings — moderate, not over-inflated
    "earnings miss":52,"earnings beat":50,"guidance cut":55,
}
MED_IMP_KW = {
    "interest rate":48,"monetary policy":50,"fiscal":45,"stimulus":52,
    "inflation":58,"deflation":62,"ppi":52,"retail sales":48,"pmi":42,"ism":44,
    "jobless claims":50,"consumer confidence":40,"trade balance":42,
    "upgrade":30,"downgrade":32,"buy rating":28,"sell rating":30,
    "geopolitical":45,"election":50,"debt ceiling":58,"budget":38,
    "opec":55,"production cut":52,"merger":35,"acquisition":38,
    "tariff":60,"trade war":68,"conflict":52,"ceasefire":48,
    "regulatory":40,"antitrust":42,"lawsuit":35,"bankruptcy":62,"collapse":70,
    "sec investigation":55,"fraud":62,"circuit breaker":72,"bailout":68,
    "attack":55,"military":52,"margin call":60,"crisis":65,"contagion":70,
    "producer price":48,"guidance raise":38,
}

# ── Per-asset directional keyword banks ──────────────────────────────────────
ASSET_BULL = {
    "USD": ["dollar surge","fed hawkish","rate hike","strong jobs","dollar strength","dxy up","dollar rally"],
    "EUR": ["ecb hawkish","euro rally","eurozone growth","ecb rate hike","euro strength"],
    "GBP": ["boe hawkish","pound rally","uk growth","boe rate hike","sterling"],
    "JPY": ["boj hawkish","yen strength","yen rally","boj tightening","safe haven"],
    "CHF": ["snb hawkish","franc strength","safe haven","swiss franc"],
    "CAD": ["oil rally","canada jobs","boc hawkish","loonie strength","cad bid"],
    "AUD": ["rba hawkish","aussie rally","iron ore","china growth","commodity rally"],
    "NZD": ["rbnz hawkish","kiwi rally","nzd bid","nz growth"],
    "GOLD":  ["safe haven","gold rally","inflation surge","geopolitical risk","bullion rally"],
    "SILVER":["silver rally","industrial demand","precious metals"],
    "OIL":   ["opec cut","supply shortage","oil rally","crude surge","demand growth"],
    "BTC":   ["bitcoin rally","crypto surge","institutional buying","etf approval","adoption"],
    "ETH":   ["ethereum rally","eth bid","defi growth","staking"],
    "SPY":   ["fed pivot","risk on","strong earnings","gdp beat","soft landing","bull market"],
    "NVDA":  ["ai demand","chip demand","data center","nvidia beat","gpu"],
    "AAPL":  ["iphone demand","apple beat","services growth","buyback"],
}
ASSET_BEAR = {
    "USD": ["dollar fall","fed dovish","rate cut","dollar weakness","dxy down","dollar dump"],
    "EUR": ["ecb dovish","euro fall","eurozone recession","ecb rate cut","euro weakness"],
    "GBP": ["boe dovish","pound fall","uk recession","sterling weakness"],
    "JPY": ["boj dovish","yen weakness","boj easing","carry trade"],
    "CHF": ["snb dovish","franc weakness"],
    "CAD": ["oil decline","canada recession","boc dovish","loonie weakness"],
    "AUD": ["rba dovish","aussie fall","china slowdown","commodity selloff"],
    "NZD": ["rbnz dovish","kiwi fall","nz recession"],
    "GOLD":  ["risk on","gold selloff","real yields rise","gold drop"],
    "SILVER":["silver selloff","industrial slowdown"],
    "OIL":   ["opec increase","demand drop","oil fall","crude selloff","recession"],
    "BTC":   ["bitcoin crash","crypto selloff","regulatory ban","hack","exchange collapse"],
    "ETH":   ["ethereum crash","eth sold","security issue"],
    "SPY":   ["recession","fed hawkish","rate hike","earnings miss","risk off","bear market"],
    "NVDA":  ["chip ban","export restriction","nvda miss","tariff"],
    "AAPL":  ["iphone miss","china ban","revenue miss"],
}

# ── Market regime keyword banks ───────────────────────────────────────────────
REGIME_KW = {
    "RISK ON":       ["rate cut","fed pivot","dovish","stimulus","qe","bailout","earnings beat","soft landing","risk on"],
    "RISK OFF":      ["rate hike","hawkish","tightening","recession","default","crisis","collapse","war","risk off","geopolitical","panic"],
    "INFLATIONARY":  ["cpi beat","inflation surge","hot inflation","price surge","stagflation","pce beat","wage growth","energy prices"],
    "DEFLATIONARY":  ["deflation","cpi miss","below target","price decline","disinflation","demand drop"],
    "GROWTH POSITIVE":["gdp beat","jobs beat","nfp beat","retail beat","pmi expansion","consumer confidence","growth acceleration"],
    "GROWTH NEGATIVE":["gdp miss","jobs miss","recession","contraction","pmi below 50","consumer confidence fall","layoffs","job cuts"],
}

# ── Correlation matrix ────────────────────────────────────────────────────────
CORR_MAP = {
    "USD_BULL":  {"DXY":"↑","EUR/USD":"↓","GBP/USD":"↓","USD/JPY":"↑","USD/CHF":"↑","AUD/USD":"↓","USD/CAD":"↑","XAU/USD":"↓","WTI":"↓","BTCUSDT":"↓","SPY":"↓"},
    "USD_BEAR":  {"DXY":"↓","EUR/USD":"↑","GBP/USD":"↑","USD/JPY":"↓","USD/CHF":"↓","AUD/USD":"↑","USD/CAD":"↓","XAU/USD":"↑","WTI":"↑","BTCUSDT":"↑","SPY":"↑"},
    "RISK ON":   {"SPY":"↑","QQQ":"↑","BTCUSDT":"↑","XAU/USD":"↓","AUD/USD":"↑","WTI":"↑"},
    "RISK OFF":  {"SPY":"↓","QQQ":"↓","BTCUSDT":"↓","XAU/USD":"↑","USD/JPY":"↓","AUD/USD":"↓","WTI":"↓"},
    "INFLATIONARY":{"XAU/USD":"↑","WTI":"↑","USD/JPY":"↑","SPY":"↓","BTCUSDT":"↑"},
}

# ── Symbol map for impact analysis ───────────────────────────────────────────
ASSET_SYM_MAP = {
    "USD": ["EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD","USD/CAD","NZD/USD"],
    "EUR": ["EUR/USD","EUR/GBP","EUR/JPY","EUR/CHF"],
    "GBP": ["GBP/USD","GBP/JPY","GBP/CHF"],
    "JPY": ["USD/JPY","EUR/JPY","GBP/JPY","AUD/JPY"],
    "CAD": ["USD/CAD"],
    "AUD": ["AUD/USD","AUD/JPY"],
    "NZD": ["NZD/USD"],
    "GOLD":["XAU/USD"], "SILVER":["XAG/USD"],
    "OIL": ["WTI","BRENT"],
    "BTC": ["BTCUSDT"], "ETH":["ETHUSDT"],
    "SPY": ["SPY","QQQ"], "NVDA":["NVDA"], "AAPL":["AAPL"],
}

console = Console(width=240)
lock    = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# NEWS ANALYSIS ENGINE
# ─────────────────────────────────────────────────────────────────────────────
analyzed_news: list  = []
_tg_sent:      set   = set()
_tg_setup_sent:set   = set()

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM MODULE
# ─────────────────────────────────────────────────────────────────────────────
def send_telegram(message: str):
    """Send a message via Telegram Bot API."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=8)
    except: pass

# ── AI Karar Motoru — bireysel skorlar ───────────────────────────────────────
def ai_decision_scores(s):
    """Sinyalden 8 bireysel AI skoru türet (0-100)."""
    reasons  = " ".join(s.get("reasons", []) or []).upper()
    sm_notes = " ".join(s.get("sm_notes", []) or []).upper()
    neg      = " ".join(s.get("neg_factors", []) or []).upper()
    base     = (s.get("score") or 50) / 100
    def kw(*words): return any(w in reasons or w in sm_notes for w in words)
    clamp = lambda v: int(min(100, max(0, round(v))))
    return {
        "Trend":    clamp(50 + (base-0.5)*80 + (15 if kw("EMA","TREND","YÜKSEL","DÜŞÜŞ") else 0) + (-10 if "KARŞI" in neg else 0)),
        "Yapı":     clamp(45 + (base-0.5)*60 + (20 if kw("BOS","CHOCH","KIRILIM","YAPI") else 0) + (10 if kw("DESTEK","DİRENÇ") else 0)),
        "Likidite": clamp(40 + (base-0.5)*70 + (20 if kw("LİKİDİTE","SWEEP","OB","BLOK","FVG") else 0)),
        "Hacim":    clamp(50 + (base-0.5)*50 + (15 if kw("HACİM","VOLUME") else 0)),
        "Momentum": clamp(45 + (base-0.5)*70 + (15 if kw("RSI","MACD","MOMENTUM") else 0) + (-10 if ("AŞIRI ALIM" in neg or "AŞIRI SATIM" in neg) else 0)),
        "Seans":    {"LONDRA+NY":85,"NEW YORK":80,"LONDRA":75,"ASYA":55}.get(signal_session(), 65),
        "Haber":    clamp(50 + (1-(_news_risk_num(s)))*50),
        "Risk":     clamp(100 - (s.get("contrarian_score") or 50)*0.6),
    }

def _news_risk_num(s):
    nr = s.get("news_risk")
    if isinstance(nr, (int, float)): return float(nr)
    m = {"Düşük":0.2, "Orta":0.5, "Yüksek":0.8}
    return m.get(str(nr), 0.5)

def signal_session():
    h = datetime.utcnow().hour
    if h < 7:  return "ASYA"
    if h < 12: return "LONDRA"
    if h < 16: return "LONDRA+NY"
    if h < 21: return "NEW YORK"
    return "ASYA"

def signal_strategy(s):
    txt = " ".join(s.get("reasons", []) or []).upper()
    if any(w in txt for w in ("ICT","OB","FVG","SMART")): return "ICT / SMC"
    if "TREND" in txt and "EMA" in txt:                   return "Trend Takip"
    if "KONTRAR" in txt or (s.get("contrarian_score",0) or 0) > 70: return "Kontraryan"
    return "Yapı Kırılımı"

def signal_probability(s):
    sc = s.get("score") or 0; conf = s.get("confidence") or sc
    return min(99, max(1, round(sc*0.6 + conf*0.4)))

def _tg_chart_png(s):
    """Kurumsal analiz grafiği — Giriş/SL/TP, EMA, zon ve S/R çizgileriyle.
    s: sinyal sözlüğü (sym, direction, el, eh, sl, tp, score, quality)."""
    try:
        import mplfinance as mpf, matplotlib
        matplotlib.use("Agg")
        import io as _io
        import pandas as _pd
        sym = s["sym"] if isinstance(s, dict) else s
        tmap = {"EUR/USD":"EURUSD=X","GBP/USD":"GBPUSD=X","USD/JPY":"JPY=X",
                "XAU/USD":"GC=F","XAG/USD":"SI=F","WTI":"CL=F","BRENT":"BZ=F"}
        ticker = tmap.get(sym, sym.replace("/","")+"=X")
        df = yf.Ticker(ticker).history(period="5d", interval="15m")
        if df.empty: return None
        if df.index.tzinfo: df.index = df.index.tz_localize(None)
        df = df.tail(70).copy()

        # EMA'lar (trend yapısı)
        df["EMA9"]  = df["Close"].ewm(span=9,  adjust=False).mean()
        df["EMA20"] = df["Close"].ewm(span=20, adjust=False).mean()
        df["EMA50"] = df["Close"].ewm(span=50, adjust=False).mean()

        # Swing High/Low → Destek/Direnç (yapı)
        win = df.tail(50)
        sup = float(win["Low"].min())
        res = float(win["High"].max())

        mc = mpf.make_marketcolors(up="#00ff88", down="#ff3b5c", wick="inherit",
                                   edge="inherit", volume="#3b82f6")
        st = mpf.make_mpf_style(marketcolors=mc, facecolor="#0a0b0e", figcolor="#0a0b0e",
                                gridcolor="#1e2028", rc={"axes.labelcolor":"#6b7280",
                                "xtick.color":"#6b7280","ytick.color":"#6b7280","font.size":9})

        aps = [
            mpf.make_addplot(df["EMA9"],  color="#3b82f6", width=0.9),
            mpf.make_addplot(df["EMA20"], color="#f59e0b", width=0.9),
            mpf.make_addplot(df["EMA50"], color="#a855f7", width=1.1),
        ]

        # Yatay seviyeler (Giriş/SL/TP/S/R)
        hl_prices, hl_colors, hl_styles = [], [], []
        d = s if isinstance(s, dict) else {}
        def _add(p, col, sty="-"):
            if p is None: return
            try: hl_prices.append(float(p)); hl_colors.append(col); hl_styles.append(sty)
            except: pass
        entry = d.get("el") or d.get("price")
        _add(entry,        "#e8eaf0", "-")   # Giriş
        _add(d.get("eh"),  "#e8eaf0", ":")   # Giriş zon üst
        _add(d.get("sl"),  "#ff3b5c", "--")  # Stop Loss
        _add(d.get("tp"),  "#00ff88", "--")  # Take Profit

        direction = d.get("direction","")
        score = d.get("score",0); grade = d.get("quality","")
        rr = d.get("rr",""); prob = signal_probability(d) if d else 0
        arrow = "▲ LONG" if direction=="LONG" else "▼ SHORT" if direction=="SHORT" else ""
        title = f"\n{sym}  {arrow}   {grade} {score:.0f}/100   1:{rr} R:R   Olasilik %{prob:.0f}"

        kw = dict(type="candle", style=st, addplot=aps, volume=True,
                  title=title, figratio=(16,9), figscale=1.2, returnfig=True)
        if hl_prices:
            kw["hlines"] = dict(hlines=hl_prices, colors=hl_colors,
                                linestyle=hl_styles, linewidths=1.1)
        # Giriş↔TP ve Giriş↔SL bölgelerini renkli vurgula (kar/zarar alanları)
        fill = []
        try:
            if entry and d.get("tp"):
                fill.append(dict(y1=float(entry), y2=float(d["tp"]), alpha=0.06, color="#00ff88"))
            if entry and d.get("sl"):
                fill.append(dict(y1=float(entry), y2=float(d["sl"]), alpha=0.06, color="#ff3b5c"))
            if fill: kw["fill_between"] = fill
        except Exception:
            pass

        fig, axl = mpf.plot(df, **kw)
        ax = axl[0]   # fiyat paneli
        n = len(df)
        xr = n - 1
        hi = float(df["High"].max()); lo = float(df["Low"].min())
        rng = (hi - lo) or 1

        # ── Diyagonal trend çizgileri / kanal (gerçek trader gibi) ───────
        highs = df["High"].values; lows = df["Low"].values
        def _pivots(arr, kind, lw=3, rw=3):
            out=[]
            for i in range(lw, len(arr)-rw):
                w=arr[i-lw:i+rw+1]
                if kind=="high" and arr[i]==max(w): out.append(i)
                if kind=="low"  and arr[i]==min(w): out.append(i)
            return out
        def _trendline(idxs, vals, color, label):
            # ilk ve son önemli pivotu birleştirip tüm grafiği kateden çizgi
            if len(idxs)<2: return
            i1,i2=idxs[0],idxs[-1]
            if i2==i1: return
            y1,y2=vals[i1],vals[i2]
            slope=(y2-y1)/(i2-i1)
            x0=0; y0=y1+slope*(x0-i1)
            x_end=xr; y_end=y1+slope*(x_end-i1)
            ax.plot([x0,x_end],[y0,y_end], color=color, lw=1.5, alpha=0.9,
                    linestyle="-", solid_capstyle="round")
            ax.annotate(label, xy=(x_end, y_end), xytext=(-2,4),
                        textcoords="offset points", fontsize=7.5,
                        color=color, ha="right", alpha=0.95)
        ph=_pivots(highs,"high"); pl=_pivots(lows,"low")
        _trendline(ph, highs, "#9aa3b7", "trend hattı")
        _trendline(pl, lows,  "#9aa3b7", "trend hattı")

        # ── İnsan tipi etiketler (sağ kenarda kutu içinde) ───────────────
        def _tag(price, text, color):
            if price is None: return
            try: price = float(price)
            except: return
            ax.annotate(text, xy=(xr, price), xytext=(6, 0),
                        textcoords="offset points", va="center", ha="left",
                        fontsize=8.5, fontweight="bold", color="#0a0b0e",
                        bbox=dict(boxstyle="round,pad=0.3", fc=color, ec="none", alpha=0.95))
        _tag(entry,       f"GİRİŞ {fp_plain(entry)}",   "#e8eaf0")
        _tag(d.get("sl"), f"SL {fp_plain(d.get('sl'))}", "#ff3b5c")
        _tag(d.get("tp"), f"TP {fp_plain(d.get('tp'))}", "#00ff88")

        # ── Yön oku (giriş bölgesine işaret eden ok) ─────────────────────
        if entry:
            up = direction == "LONG"
            ay = float(entry) + (rng*0.16)*(-1 if up else 1)
            ax.annotate("AL  ▲" if up else "SAT  ▼",
                        xy=(xr-2, float(entry)), xytext=(xr-12, ay),
                        fontsize=11, fontweight="bold",
                        color="#00ff88" if up else "#ff3b5c",
                        arrowprops=dict(arrowstyle="-|>", lw=2.2,
                                        color="#00ff88" if up else "#ff3b5c"))

        # ── Analiz kutusu (sol üst — okunabilir gerekçe) ─────────────────
        bias = "Yükseliş yapısı" if direction=="LONG" else "Düşüş yapısı"
        reasons = (d.get("reasons") or [])[:3]
        lines = [f"{sym}   |   {bias}",
                 f"EMA 9/20/50 {'pozitif dizilim' if direction=='LONG' else 'negatif dizilim'}",
                 f"R:R 1:{rr}  ·  Olasılık %{prob:.0f}  ·  {grade}"]
        for rsn in reasons:
            lines.append(f"• {str(rsn)[:46]}")
        txt = "\n".join(lines)
        ax.text(0.012, 0.975, txt, transform=ax.transAxes, va="top", ha="left",
                fontsize=8, color="#e8eaf0", linespacing=1.5,
                bbox=dict(boxstyle="round,pad=0.5", fc="#111318", ec="#1e2028", alpha=0.92))

        # ── Watermark (profesyonel görünüm) ──────────────────────────────
        ax.text(0.99, 0.02, "TITAN PRIME", transform=ax.transAxes,
                va="bottom", ha="right", fontsize=9, fontweight="bold",
                color="#1e2028", alpha=0.8)

        buf = _io.BytesIO()
        fig.savefig(buf, dpi=130, bbox_inches="tight", facecolor="#0a0b0e")
        import matplotlib.pyplot as _plt; _plt.close(fig)
        buf.seek(0); return buf.read()
    except Exception:
        return None

def fp_plain(v):
    """Grafik etiketleri için sade fiyat formatı (rich markup yok)."""
    if v is None: return "—"
    try: v=float(v)
    except: return str(v)
    a=abs(v)
    if a>10000: return f"{v:,.1f}"
    if a>100:   return f"{v:,.3f}"
    if a>1:     return f"{v:.4f}"
    return f"{v:.5f}"

def tg_setup_alert(s):
    """VIP-grade trade signal alert."""
    key=f"{s['sym']}_{s['direction']}_{round(s['score'])}"
    if key in _tg_setup_sent: return
    _tg_setup_sent.add(key)
    # Assign signal ID for tracking
    sig_id=s.get("db_id") or abs(hash(key))%100000
    s["_tg_signal_id"]=sig_id
    sz=s.get("sizing",{}); rr=s["rr"]; q=s["quality"]
    sc=s["score"]; regime=s.get("regime","Nötr")
    direction=s["direction"]; sym=s["sym"]
    c_score=s.get("contrarian_score",0); c_label=s.get("contrarian_label","—")
    duration=s.get("duration","Intraday")
    hold_h=s.get("hold_h",0)
    sm=s.get("sm_notes",[])
    traps=s.get("trap_warnings",[])
    cv=s.get("consensus_view",""); smv=s.get("sm_view","")
    news_risk=s.get("news_risk","—")

    # Grade styling
    if q=="A+":
        grade_hdr="🔥 A+ SETUPu — KURUMSAL ONAYLI 🔥"
        grade_bar="★★★★★"
    elif q=="A":
        grade_hdr="✅ A SETUPu — YÜKSEK OLASILIK"
        grade_bar="★★★★☆"
    else:
        grade_hdr="🔔 B+ SETUPu — GÜÇLÜ FIRSAT"
        grade_bar="★★★☆☆"

    dir_emoji="📈" if direction=="LONG" else "📉"
    regime_emoji=("🟢" if regime=="Risk-On" else "🔴" if regime=="Risk-Off" else "🟡")

    # Score bar (10 blocks)
    filled=int(sc/10); bar="█"*filled+"░"*(10-filled)

    # Sizing block — front and center
    sz_block=""
    if sz:
        margin=sz.get('margin',0); lev2=sz.get('leverage',1)
        exp_loss=sz.get('exp_loss',0); exp_profit=sz.get('exp_profit',0)
        notional=sz.get('notional',0); risk_pct2=sz.get('risk_pct',0)
        sz_block=(
            f"\n┌──────────────────────────────┐\n"
            f"│  💷 HESABINDAN KAÇ POUND GİR?  │\n"
            f"└──────────────────────────────┘\n"
            f"🔑 Trade212'ye yatır  : <b>£{margin:.2f}</b> (marj)\n"
            f"⚡ Kaldıraç            : <b>{lev2}:1</b>  →  £{notional:.0f} nominal pozisyon\n"
            f"⚖️ Hesabının riski     : <b>%{risk_pct2:.2f}</b>\n"
            f"❌ Maksimum kayıp      : <b>£{exp_loss:.2f}</b>  (SL'de)\n"
            f"✅ Hedef kâr           : <b>£{exp_profit:.2f}</b>  (TP'de)\n")

    # Smart money block
    sm_block=""
    if sm:
        sm_block="\n🧠 <b>SMART MONEY ANALİZİ</b>\n"
        for n in sm[:3]: sm_block+=f"  ◈ {n}\n"

    # Trap warning block
    trap_block=""
    if traps:
        trap_block="\n⚠️ <b>TUZAK UYARISI</b>\n"
        for t in traps[:2]: trap_block+=f"  {t}\n"

    # Consensus block
    cv_block=""
    if cv:
        cv_block=(f"\n🔍 <b>GÖRÜŞ KARŞILAŞTIRMASI</b>\n"
                  f"  {cv}\n"
                  f"  {smv}\n")

    # Contrarian bar
    c_bar="█"*int(c_score/10)+"░"*(10-int(c_score/10))
    c_emoji="⚡" if c_score>=70 else "〰️" if c_score>=40 else "➡️"

    # AI Karar Motoru bireysel skorları
    ai=ai_decision_scores(s)
    def _mini(v):
        f=int(v/20); return "▰"*f+"▱"*(5-f)
    ai_block=("\n🤖 <b>AI KARAR MOTORU</b>\n"
        f"  Trend     {_mini(ai['Trend'])} {ai['Trend']}\n"
        f"  Yapı      {_mini(ai['Yapı'])} {ai['Yapı']}\n"
        f"  Likidite  {_mini(ai['Likidite'])} {ai['Likidite']}\n"
        f"  Hacim     {_mini(ai['Hacim'])} {ai['Hacim']}\n"
        f"  Momentum  {_mini(ai['Momentum'])} {ai['Momentum']}\n"
        f"  Risk      {_mini(ai['Risk'])} {ai['Risk']}\n")
    prob=signal_probability(s); sess=signal_session(); strat=signal_strategy(s)
    meta_block=(f"🎲 Olasılık : <b>%{prob:.0f}</b>  |  🕑 Seans: <b>{sess}</b>\n"
                f"🧩 Strateji : <b>{strat}</b>\n")

    now_str=datetime.now().strftime("%d.%m.%Y %H:%M")

    msg=(
        f"╔══════════════════════════╗\n"
        f"║  🏆 <b>TITAN PRIME ELITE</b>  🏆  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"<b>{grade_hdr}</b>\n"
        f"{grade_bar}  <b>{sc:.0f}/100</b>  {bar}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_emoji} <b>{sym}</b>  —  <b>{direction}</b>\n"
        f"{regime_emoji} Rejim: <b>{regime}</b>  |  Süre: <b>{duration}</b> (~{hold_h:.0f}s)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📌 <b>GİRİŞ ZON</b>  <code>{fp(s['el'])} — {fp(s['eh'])}</code>\n"
        f"🔴 <b>STOP LOSS</b>  <code>{fp(s['sl'])}</code>\n"
        f"🟢 <b>TAKE PROFIT</b>  <code>{fp(s['tp'])}</code>\n\n"
        f"⚖️ <b>Risk/Ödül : 1:{rr}</b>\n"
        f"🎯 Güven    : <b>{s.get('confidence',sc):.0f}/100</b>\n"
        f"📰 Haber    : <b>{news_risk}</b>\n"
        f"{meta_block}\n"
        f"{c_emoji} <b>Kontraryan Skoru</b>: {c_score}/100\n"
        f"   {c_bar}  {c_label}\n"
        f"{ai_block}{cv_block}{sm_block}{trap_block}{sz_block}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {now_str}\n"
        f"🔖 Sinyal ID: <code>#{s.get('_tg_signal_id',0):05d}</code>\n"
        f"⚡ <b>BU SİNYAL ÖZEL VE KİŞİSELDİR</b> ⚡")
    def _send():
        try:
            png=_tg_chart_png(s)
            if png:
                # Grafik + açıklama (caption 1024 karakter sınırı için kısalt)
                cap=msg if len(msg)<=1024 else msg[:1015]+"…"
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                    data={"chat_id":TELEGRAM_CHAT_ID,"caption":cap,"parse_mode":"HTML"},
                    files={"photo":("chart.png",png,"image/png")},timeout=20)
                if len(msg)>1024:
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML",
                              "disable_web_page_preview":True},timeout=8)
            else:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML",
                          "disable_web_page_preview":True},timeout=8)
        except: pass
    threading.Thread(target=_send,daemon=True).start()

def tg_outcome_alert(sym, direction, status, entry, out_price, act_rr, sig_id=None):
    """VIP-grade trade outcome notification."""
    now_str=datetime.now().strftime("%d.%m.%Y %H:%M")
    # Running stats for context
    with lock: sc=dict(stats_cache)
    total=sc.get("total",0)+1; wins=sc.get("wins",0)+(1 if status=="TP" else 0)
    run_wr=round(wins/total*100,1) if total else 0
    if status=="TP":
        header="╔══════════════════════════╗\n║  🎯  TAKE PROFIT HIT  🎯  ║\n╚══════════════════════════╝"
        result_line=f"✅ <b>BAŞARILI — +{act_rr:.2f}R KÂR</b>" if act_rr else "✅ <b>TAKE PROFIT HIT</b>"
        pnl_emoji="💰"
    elif status=="SL":
        header="╔══════════════════════════╗\n║  ❌  STOP LOSS HIT  ❌  ║\n╚══════════════════════════╝"
        result_line=f"🛑 <b>STOP — {act_rr:.2f}R KAYIP</b>" if act_rr else "🛑 <b>STOP LOSS HIT</b>"
        pnl_emoji="🔻"
    elif status=="INVALIDATED":
        header="╔══════════════════════════╗\n║  🚫  SİNYAL İPTAL  🚫  ║\n╚══════════════════════════╝"
        result_line="⛔ <b>SETUP GEÇERSİZ HALE GELDİ</b>"
        pnl_emoji="⚠️"
    elif status=="EXPIRED":
        header="╔══════════════════════════╗\n║  ⏰  ZAMAN AŞIMI  ⏰  ║\n╚══════════════════════════╝"
        result_line="📭 <b>SİNYAL SÜRESİ DOLDU</b>"
        pnl_emoji="🕐"
    else:
        return
    move=""; pct=0
    if entry and out_price and entry>0:
        pct=(round((out_price-entry)/entry*100,3) if direction=="LONG"
             else round((entry-out_price)/entry*100,3))
        move_emoji="📈" if pct>=0 else "📉"
        move=f"\n{move_emoji} Hareket    : <b>{pct:+.3f}%</b>"
    rr_bar=("█"*min(int(abs(act_rr)*2),10)+"░"*(10-min(int(abs(act_rr)*2),10))) if act_rr else "░"*10
    msg=(
        f"{header}\n\n"
        f"📊 <b>{sym}</b>  —  {direction}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Giriş   : <code>{fp(entry)}</code>\n"
        f"📤 Çıkış   : <code>{fp(out_price)}</code>"
        f"{move}\n"
        f"{pnl_emoji} Sonuç    : <b>{act_rr:+.2f}R</b>  {rr_bar}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{result_line}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Sistem Win Rate: <b>{run_wr:.1f}%</b>  ({wins}TP/{total-wins}SL / {total} işlem)\n"
        f"🕐 {now_str}"
        +(f"\n🔖 Sinyal ID: <code>#{sig_id:05d}</code>" if sig_id else "")+
        f"\n<i>Titan Prime Elite — sistem bu sonuçtan öğreniyor</i>")
    def _send():
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"},timeout=8)
        except: pass
    threading.Thread(target=_send,daemon=True).start()

# ── Best-probability digest ───────────────────────────────────────────────────
_tg_digest_sent: set = set()
def _tg_best_picks(results):
    """VIP digest — top-3 highest probability setups per analysis cycle."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    approved=[r for r in results if r.get("status")=="APPROVED" and r.get("quality") in ("A+","A","B+")]
    if not approved: return
    top=sorted(approved, key=lambda x:-x.get("score",0))[:3]
    keys=tuple(f"{s['sym']}_{s['direction']}_{round(s['score'])}" for s in top)
    if keys in _tg_digest_sent: return
    _tg_digest_sent.add(keys)
    now_str=datetime.now().strftime("%d.%m.%Y %H:%M")
    lines=[
        "╔══════════════════════════╗\n"
        "║  🔭 EN YÜKSEK OLASILIKLI  ║\n"
        "║    TITAN PRIME SINYALLER   ║\n"
        "╚══════════════════════════╝\n"
    ]
    medals=["🥇","🥈","🥉"]
    for i,s in enumerate(top):
        q=s["quality"]; rr=s["rr"]; sc=s["score"]
        grade_e="🔥" if q=="A+" else "✅" if q=="A" else "🔔"
        regime=s.get("regime","Nötr")
        c_score=s.get("contrarian_score",0)
        duration=s.get("duration","Intraday")
        dir_emoji="📈" if s["direction"]=="LONG" else "📉"
        bar="█"*int(sc/10)+"░"*(10-int(sc/10))
        lines.append(
            f"{medals[i]} {grade_e} <b>{s['sym']}</b>  {dir_emoji} <b>{s['direction']}</b>  [{q}]\n"
            f"   Skor: <b>{sc:.0f}/100</b>  {bar}\n"
            f"   📌 Giriş : <code>{fp(s['el'])} — {fp(s['eh'])}</code>\n"
            f"   🔴 SL    : <code>{fp(s['sl'])}</code>\n"
            f"   🟢 TP    : <code>{fp(s['tp'])}</code>\n"
            f"   ⚖️ R:R: <b>1:{rr}</b>  |  {regime}  |  {duration}\n"
            f"   🧠 Kontraryan: {c_score}/100\n"
        )
    lines.append(
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {now_str}\n"
        f"⚡ <b>TITAN PRIME ELITE</b> — <i>Sadece en güçlü sinyaller</i>"
    )
    msg="\n".join(lines)
    def _send():
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"},timeout=8)
        except: pass
    threading.Thread(target=_send,daemon=True).start()

# ── Performance report (daily/weekly) ────────────────────────────────────────
_last_daily_report = 0
_last_weekly_report = 0

def tg_performance_report(period="daily"):
    """Send daily or weekly performance card to Telegram."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    with lock: sc=dict(stats_cache)
    if not sc.get("total"): return
    total=sc.get("total",0); wins=sc.get("wins",0); losses=sc.get("losses",0)
    wr=sc.get("wr",0); avg_rr=sc.get("avg_rr",0); pf=sc.get("pf",0)
    sharpe=sc.get("sharpe"); sortino=sc.get("sortino")
    streak=sc.get("streak",0); streak_type=sc.get("streak_type","")
    best_syms=sc.get("best_syms",[])
    by_q=sc.get("by_q",{})
    kelly=sc.get("kelly",0); mdd=sc.get("mdd",0)
    with lock: bal=portfolio_state.get("shadow_balance",ACCOUNT["balance"])
    pnl=round(bal-ACCOUNT["balance"],2); pnl_pct=round(pnl/ACCOUNT["balance"]*100,1)
    now_str=datetime.now().strftime("%d.%m.%Y %H:%M")

    # Win rate visual bar
    wr_filled=int(wr/10); wr_bar="█"*wr_filled+"░"*(10-wr_filled)
    wr_emoji="🟢" if wr>=60 else ("🟡" if wr>=50 else "🔴")

    # Streak line
    streak_line=""
    if streak>=2:
        streak_line=f"\n🔥 <b>{'Kazanma' if streak_type=='TP' else 'Kayıp'} Serisi: {streak} art arda {'✅' if streak_type=='TP' else '❌'}</b>"

    # Quality breakdown
    q_lines=""
    for q,d in by_q.items():
        if d["t"]>0:
            q_emoji="🔥" if q=="A+" else ("✅" if q=="A" else "🔔")
            q_lines+=f"  {q_emoji} {q}: {d['wr']:.0f}% WR  ({d['w']}W/{d['t']-d['w']}L / {d['t']} işlem)\n"

    # Best symbols
    best_lines=""
    if best_syms:
        best_lines="\n🏆 <b>EN BAŞARILI SEMBOLLER</b>\n"
        medals=["🥇","🥈","🥉","4️⃣","5️⃣"]
        for i,(sym,d) in enumerate(best_syms[:5]):
            best_lines+=f"  {medals[i]} {sym}: {d['wr']:.0f}% WR  avg {d['avg_rr']:+.2f}R  ({d['t']} işlem)\n"

    # Learning status
    adap=dict(adap_weights)
    top_feat=sorted(adap.items(),key=lambda x:-x[1])[:3]
    bot_feat=sorted(adap.items(),key=lambda x:x[1])[:2]
    learn_lines="  En güçlü: "+", ".join(f"{f}={w:.2f}x" for f,w in top_feat)
    learn_lines+="\n  En zayıf: "+", ".join(f"{f}={w:.2f}x" for f,w in bot_feat)

    period_hdr="📅 GÜNLÜK PERFORMANS RAPORU" if period=="daily" else "📆 HAFTALIK PERFORMANS RAPORU"

    msg=(
        f"╔══════════════════════════╗\n"
        f"║  🏆 <b>TITAN PRIME ELITE</b>  🏆  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"<b>{period_hdr}</b>\n"
        f"🕐 {now_str}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>GENEL PERFORMANS</b>\n"
        f"  Toplam İşlem : <b>{total}</b>  ({wins} TP / {losses} SL)\n"
        f"  {wr_emoji} Win Rate    : <b>{wr:.1f}%</b>  {wr_bar}\n"
        f"  Ort. R:R     : <b>{avg_rr:+.2f}R</b>\n"
        f"  Kâr Faktörü  : <b>{pf:.2f}</b>\n"
        f"  Max DD       : <b>{mdd:.1f}%</b>\n"
        f"  Kelly        : <b>{kelly:.1f}%</b>\n"
        f"{streak_line}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>HESAP DURUMU</b>\n"
        f"  Başlangıç : £{ACCOUNT['balance']:.2f}\n"
        f"  Şu an     : <b>£{bal:.2f}</b>\n"
        f"  Net P&L   : <b>{'+'if pnl>=0 else ''}{pnl:.2f} ({pnl_pct:+.1f}%)</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎖 <b>KALİTE DAĞILIMI</b>\n{q_lines}"
        f"{best_lines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 <b>YAPAY ZEKA ÖĞRENMESİ</b>\n{learn_lines}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 Sharpe: {sharpe:.2f}  |  Sortino: {sortino:.2f}\n"
        f"⚡ <b>TITAN PRIME ELITE</b>  —  <i>Sistem sürekli öğreniyor</i>"
    )
    def _send():
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML",
                      "disable_web_page_preview":True},timeout=10)
        except: pass
    threading.Thread(target=_send,daemon=True).start()

def performance_report_loop():
    """Send daily report at 22:00 and weekly report on Sunday 22:00."""
    global _last_daily_report, _last_weekly_report
    while True:
        try:
            now=datetime.now()
            today_key=now.strftime("%Y-%m-%d")
            # Daily report at 22:00
            if now.hour==22 and today_key!=str(_last_daily_report):
                tg_performance_report("daily")
                _last_daily_report=today_key
            # Weekly report on Sunday
            if now.weekday()==6 and now.hour==22:
                week_key=now.strftime("%Y-W%W")
                if week_key!=str(_last_weekly_report):
                    tg_performance_report("weekly")
                    _last_weekly_report=week_key
        except: pass
        time.sleep(60)

def _importance(text):
    high_score = 0
    for kw,pts in HIGH_IMP_KW.items():
        if kw in text: high_score = max(high_score, pts)
    med_score = 0
    for kw,pts in MED_IMP_KW.items():
        if kw in text: med_score = max(med_score, pts)
    # Base = highest keyword match; medium only bumps if no high hit
    score = high_score if high_score>0 else med_score
    # Small hits bonus capped at +5 — prevents over-inflation
    high_hits = sum(1 for kw in HIGH_IMP_KW if kw in text)
    bonus = min(high_hits, 3) * 1  # max +3
    return min(score + bonus, 100)

def _asset_sentiment(text):
    result = {}
    for asset in ASSET_BULL:
        b = sum(1 for kw in ASSET_BULL[asset]         if kw in text)
        e = sum(1 for kw in ASSET_BEAR.get(asset,[])  if kw in text)
        if b > e:   result[asset] = ("BULLISH", min(b*20+40, 100))
        elif e > b: result[asset] = ("BEARISH", min(e*20+40, 100))
    return result

def _market_regimes(text):
    r = [name for name,kws in REGIME_KW.items() if any(k in text for k in kws)]
    return r[:3] if r else ["NEUTRAL"]

def _vol_forecast(importance):
    if importance>=85: return ("EXTREME","1h–4h")
    if importance>=65: return ("HIGH","30m–2h")
    if importance>=40: return ("NORMAL","15m–1h")
    return ("LOW","<15m")

def _sym_impacts(asset_sent):
    impacts = {}
    for asset,(sent,strength) in asset_sent.items():
        for sym in ASSET_SYM_MAP.get(asset,[]):
            if asset=="USD":
                dir_="↑" if (sent=="BULLISH" and sym.startswith("USD/")) or \
                             (sent=="BEARISH" and not sym.startswith("USD/")) else "↓"
            else:
                dir_="↑" if sent=="BULLISH" else "↓"
            if sym not in impacts or impacts[sym][1] < strength:
                impacts[sym] = (dir_, strength, sent)
    return impacts

def _corr_impact(asset_sent, regimes):
    corr={}
    if "USD" in asset_sent:
        key="USD_BULL" if asset_sent["USD"][0]=="BULLISH" else "USD_BEAR"
        corr.update(CORR_MAP.get(key,{}))
    for r in regimes:
        for k,v in CORR_MAP.get(r,{}).items(): corr.setdefault(k,v)
    return corr

def analyze_article(article):
    text=(article.get("headline","")+article.get("summary","")).lower()
    imp   =_importance(text)
    asent =_asset_sentiment(text)
    regs  =_market_regimes(text)
    vol,vd=_vol_forecast(imp)
    simp  =_sym_impacts(asent)
    corr  =_corr_impact(asent,regs)
    rl    =("CRITICAL" if imp>=80 else "HIGH" if imp>=60
            else "MEDIUM" if imp>=40 else "LOW" if imp>=20 else "NOISE")
    enriched={**article,"importance":imp,"risk_level":rl,
              "asset_sent":asent,"regimes":regs,
              "vol":vol,"vol_dur":vd,"sym_impacts":simp,"correlations":corr}
    enriched["macro"]=_macro_analysis(enriched)
    return enriched

def _turkce_aciklama(headline, summary, imp, bias_tr, hist_key):
    """Generate Turkish explanation + 'ne anlamalıyız' for a news headline."""
    text=(headline+" "+summary).lower()
    # --- Haber ne diyor? (Türkçe özet) ---
    ozet_parts=[]
    if any(w in text for w in ["fed","federal reserve","fomc","powell"]): ozet_parts.append("ABD Merkez Bankası (Fed) politikasına ilişkin bir gelişme")
    if any(w in text for w in ["rate hike","faiz artırım","interest rate hike"]): ozet_parts.append("faiz oranları artırılıyor")
    if any(w in text for w in ["rate cut","faiz indirim","interest rate cut"]): ozet_parts.append("faiz oranları indiriliyor")
    if any(w in text for w in ["inflation","cpi","enflasyon"]): ozet_parts.append("enflasyon verisi açıklandı")
    if any(w in text for w in ["nfp","non-farm","jobs","employment","unemployment"]): ozet_parts.append("ABD istihdam/işsizlik verisi açıklandı")
    if any(w in text for w in ["gdp","büyüme","büyüdü","growth"]): ozet_parts.append("ekonomik büyüme (GSYİH) verisi açıklandı")
    if any(w in text for w in ["recession","durgunluk","recessionary"]): ozet_parts.append("resesyon (ekonomik durgunluk) endişeleri artıyor")
    if any(w in text for w in ["war","savaş","military","strike","attack"]): ozet_parts.append("jeopolitik gerilim/askeri gelişme")
    if any(w in text for w in ["opec","petrol","oil production","crude"]): ozet_parts.append("OPEC petrol üretim kararı")
    if any(w in text for w in ["china","çin","beijing"]): ozet_parts.append("Çin ekonomisine ilişkin gelişme")
    if any(w in text for w in ["russia","rusya","ukraine","ukrayna"]): ozet_parts.append("Rusya-Ukrayna jeopolitik gelişmesi")
    if any(w in text for w in ["earnings","kar açıkladı","revenue","quarterly"]): ozet_parts.append("şirket kazanç/bilanço açıklaması")
    if any(w in text for w in ["tariff","gümrük","trade war","ticaret savaşı"]): ozet_parts.append("ticaret savaşı/gümrük tarifeleri")
    if any(w in text for w in ["bank","banka","banking crisis","svb","failure"]): ozet_parts.append("bankacılık sektörü gelişmesi")
    ozet = "; ".join(ozet_parts) if ozet_parts else "önemli makroekonomik gelişme"

    # --- Ne anlamalıyız? ---
    anlam_parts=[]
    if bias_tr=="Yükseliş":
        anlam_parts.append("Bu haber piyasalar için olumlu — risk iştahı artabilir")
        if imp>=80: anlam_parts.append("Etki büyük ihtimalle hızlı ve sert olacak, ani fiyat hareketleri beklenebilir")
    elif bias_tr=="Düşüş":
        anlam_parts.append("Bu haber piyasalar için olumsuz — yatırımcılar güvenli limanlara yönelebilir")
        if imp>=80: anlam_parts.append("Sert satış dalgası, ani dolar ve altın talebi gelebilir")
    else:
        anlam_parts.append("Etki belirsiz — piyasalar bu haberi sindirmesi gerekiyor")

    if hist_key:
        anlam_parts.append(f"Geçmişte '{hist_key}' temalı haberler sonrası piyasalar önemli hareketler yaşadı")

    if imp>=80: anlam_parts.append("🔴 ÖNEMLİ: Açık pozisyon varsa stop seviyelerini gözden geçir")
    elif imp>=60: anlam_parts.append("🟡 Dikkat: Pozisyon boyutunu azaltmayı düşünebilirsin")

    return ozet, " | ".join(anlam_parts)

# Detailed per-asset Turkish impact explanation
_ASSET_IMPACT = {
    "XAUUSD": {
        "↑": "Altın yükselir → jeopolitik/ekonomik belirsizlik güvenli liman talebini artırır. Dolar zayıflarsa altın ters korelasyon nedeniyle güçlenir.",
        "↓": "Altın düşer → risk iştahı artar veya dolar güçlenir. Yüksek faiz beklentisi altının fırsat maliyetini artırır.",
    },
    "USOIL": {
        "↑": "Petrol yükselir → arz kısıtlaması (OPEC), jeopolitik risk, ekonomik büyüme beklentisi talebi artırır.",
        "↓": "Petrol düşer → talep endişesi (resesyon), OPEC üretim artışı, dolar güçlenmesi baskı yapar.",
    },
    "NAS100": {
        "↑": "Nasdaq yükselir → düşük faiz beklentisi teknoloji hisselerini destekler, risk iştahı artar.",
        "↓": "Nasdaq düşer → faiz artışı beklentisi büyüme hisselerini ezer, resesyon korkusu sermayeyi kaçırır.",
    },
    "SPX500": {
        "↑": "S&P 500 yükselir → genel risk iştahı iyileşir, güçlü ekonomi hisse değerlemelerini destekler.",
        "↓": "S&P 500 düşer → belirsizlik artar, kurumsal kârlar tehdit altına girer.",
    },
    "EURUSD": {
        "↑": "EUR/USD yükselir → dolar zayıflar veya Avrupa ekonomisi beklenenden iyi → Euro güçlenir.",
        "↓": "EUR/USD düşer → dolar güçlenir, Avrupa resesyon riski artar, ECB faiz beklentisi düşer.",
    },
    "GBPUSD": {
        "↑": "GBP/USD yükselir → sterlin güçlenir, İngiltere ekonomisi olumlu sürpriz, BoE sıkılaşma beklentisi artar.",
        "↓": "GBP/USD düşer → dolar güçlenir, İngiltere ekonomisi zayıf, Brexit/enflasyon endişeleri.",
    },
    "USDJPY": {
        "↑": "USD/JPY yükselir → dolar güçlenir veya Japonya faizleri düşük kalır → yen zayıflar.",
        "↓": "USD/JPY düşer → güvenli liman talebi yeni güçlenir, BoJ faiz artışı ya da dolar zayıflaması.",
    },
    "BTCUSD": {
        "↑": "Bitcoin yükselir → risk iştahı artar, dolar zayıflar, kurumsal ilgi/ETF haberleri destekler.",
        "↓": "Bitcoin düşer → risk kaçışı, regülasyon baskısı, likidite daralması, resesyon korkusu.",
    },
    "ETHUSD": {
        "↑": "Ethereum yükselir → DeFi/geliştirici aktivitesi artar, Bitcoin rallisi sürükler.",
        "↓": "Ethereum düşer → genel kripto satışı, regülasyon endişesi, ağ kullanımı düşer.",
    },
}

def send_telegram(article):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    h=article.get("headline","")
    if h in _tg_sent: return
    _tg_sent.add(h)
    imp=article["importance"]
    rl=article.get("risk_level","NOISE")
    m=article.get("macro",{})
    bias_tr=m.get("bias_tr","Nötr")
    bull_pct=m.get("bull_pct",0); bear_pct=m.get("bear_pct",0); neut_pct=m.get("neut_pct",0)
    caution=m.get("caution","Normal İşlem")
    dur=m.get("dur","1h"); conf=m.get("conf",50)
    ad=m.get("asset_dirs",{})
    hist_key=m.get("hist_key"); hist_match=m.get("hist_match",{})
    now_str=datetime.now().strftime("%d.%m.%Y %H:%M")

    # Risk level header
    if imp>=80:   risk_hdr="🔴 KRİTİK HABER"; risk_bar="█████████░"
    elif imp>=60: risk_hdr="🟠 YÜKSEK ETKİLİ HABER"; risk_bar="███████░░░"
    elif imp>=40: risk_hdr="🟡 ORTA ETKİLİ HABER"; risk_bar="█████░░░░░"
    else:         risk_hdr="🔵 DÜŞÜK ETKİLİ HABER"; risk_bar="███░░░░░░░"

    bias_emoji="📈" if bias_tr=="Yükseliş" else ("📉" if bias_tr=="Düşüş" else "➡️")

    # Turkish summary + meaning
    ozet, anlam = _turkce_aciklama(h, article.get("summary",""), imp, bias_tr, hist_key)

    # Detailed per-asset impact
    asset_lines=[]
    for sym,(d,strength) in ad.items():
        if d=="→": continue
        dir_emoji="📈" if d=="↑" else "📉"
        exp=_ASSET_IMPACT.get(sym,{}).get(d,"")
        clean_sym=sym.replace("USD","").replace("500","") if len(sym)>6 else sym
        strength_bar="█"*max(1,int(strength/20))+"░"*(5-max(1,int(strength/20)))
        asset_lines.append(
            f"{dir_emoji} <b>{clean_sym}</b>  {strength_bar} {strength:.1f}%\n"
            f"   └ <i>{exp[:120]}</i>" if exp else
            f"{dir_emoji} <b>{clean_sym}</b>  {strength_bar} {strength:.1f}%"
        )

    # Historical comparison
    hist_block=""
    if hist_key and hist_match:
        hist_block=f"\n📜 <b>GEÇMİŞTE NELER OLDU? ({hist_key.upper()})</b>\n"
        for asym,mv in list(hist_match.items())[:5]:
            mv_emoji="📈" if mv>0 else "📉"
            hist_block+=f"  {mv_emoji} {asym}: ortalama {'+' if mv>0 else ''}{mv:.1f}% hareket\n"
        hist_block+="  <i>Geçmiş performans gelecek garantisi değildir.</i>"

    # Timeframe impact
    tf=m.get("tf15m","—"); tf1h=m.get("tf1h","—"); tf4h=m.get("tf4h","—"); tf24h=m.get("tf24h","—")

    assets_block="\n".join(asset_lines) if asset_lines else "Belirgin varlık etkisi tespit edilmedi"

    msg=(
        f"╔══════════════════════════╗\n"
        f"║  📰 <b>MAKRO HABER ANALİZİ</b>  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"<b>{risk_hdr}</b>  {risk_bar}\n"
        f"Etki Skoru: <b>{imp}/100</b>  |  Risk: <b>{rl}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 <b>HABER</b>\n"
        f"<b>{h}</b>\n\n"
        f"🇹🇷 <b>TÜRKÇE ÖZET</b>\n"
        f"<i>{ozet}</i>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 <b>NE ANLAMALIYIZ?</b>\n"
        f"{anlam}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{bias_emoji} <b>PİYASA YÖNÜ</b>: <b>{bias_tr}</b>  (güven: {conf}/100)\n"
        f"  📈 Yükseliş olasılığı : %{bull_pct}\n"
        f"  📉 Düşüş olasılığı   : %{bear_pct}\n"
        f"  ➡️ Nötr olasılığı    : %{neut_pct}\n\n"
        f"⏱ <b>ZAMAN DİLİMİ ETKİSİ</b>\n"
        f"  15 dak: {tf}  |  1 saat: {tf1h}  |  4 saat: {tf4h}  |  1 gün: {tf24h}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>ETKİLENECEK VARLIKLAR VE NEDEN?</b>\n\n"
        f"{assets_block}"
        f"{hist_block}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>{caution}</b>  |  Süre tahmini: {dur}\n"
        f"🕐 {now_str}  |  <i>Titan Prime Elite</i>"
    )
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML",
                            "disable_web_page_preview":True},timeout=8)
    except: pass

def news_risk_for_sym(sym, max_age_min=90):
    """Returns (importance, risk_level, caution_flag) for a symbol from recent news.
    Never blocks trading — only flags caution level."""
    now=time.time()
    with lock: arts=list(analyzed_news)
    best=(0,"NOISE",False)
    for a in arts:
        ts=a.get("datetime",0)
        age_min=(now-ts)/60 if ts else 999
        if age_min>max_age_min: continue
        imp=a.get("importance",0)
        if sym not in a.get("sym_impacts",{}): continue
        caution=(imp>=60)   # only caution, never block
        if imp>best[0]: best=(imp,a.get("risk_level","NOISE"),caution)
    return best

# ── Macro news sentiment direction helper ─────────────────────────────────────
MACRO_ASSETS = ["XAUUSD","BTCUSD","ETHUSD","NAS100","SPX500","USOIL","EURUSD","GBPUSD","USDJPY"]

HIST_PATTERNS = {
    "iran":       {"XAUUSD":+1.8,"USOIL":+2.4,"NAS100":-1.2,"SPX500":-0.9,"BTCUSD":-0.8},
    "israel":     {"XAUUSD":+1.6,"USOIL":+2.1,"NAS100":-1.0,"SPX500":-0.8},
    "fed rate":   {"XAUUSD":-0.5,"BTCUSD":-1.2,"NAS100":+1.1,"SPX500":+0.9,"USDJPY":+0.4},
    "rate hike":  {"XAUUSD":-1.1,"BTCUSD":-2.0,"NAS100":-0.8,"SPX500":-0.7,"USDJPY":+0.6},
    "rate cut":   {"XAUUSD":+1.2,"BTCUSD":+2.5,"NAS100":+1.5,"SPX500":+1.3,"USDJPY":-0.5},
    "inflation":  {"XAUUSD":+0.8,"BTCUSD":+0.5,"NAS100":-0.6,"SPX500":-0.5,"USDJPY":-0.3},
    "nfp":        {"USDJPY":+0.4,"EURUSD":-0.3,"GBPUSD":-0.2,"XAUUSD":-0.4,"SPX500":+0.5},
    "recession":  {"XAUUSD":+1.5,"BTCUSD":-1.5,"NAS100":-2.0,"SPX500":-2.2,"USOIL":-1.8},
    "china":      {"NAS100":-0.7,"SPX500":-0.5,"USOIL":-0.8,"BTCUSD":-1.0},
    "russia":     {"XAUUSD":+1.2,"USOIL":+1.8,"NAS100":-0.9,"EURUSD":-0.6},
    "ukraine":    {"XAUUSD":+1.5,"USOIL":+2.0,"NAS100":-1.1,"EURUSD":-0.7},
    "gdp":        {"SPX500":+0.4,"NAS100":+0.5,"USDJPY":+0.2},
    "tariff":     {"NAS100":-1.0,"SPX500":-0.8,"USOIL":-0.5,"BTCUSD":-0.7},
    "opec":       {"USOIL":+1.5,"XAUUSD":+0.3,"SPX500":-0.3},
    "bank":       {"SPX500":-0.6,"NAS100":-0.5,"BTCUSD":-0.8},
    "etf":        {"BTCUSD":+2.0,"ETHUSD":+1.5},
    "halving":    {"BTCUSD":+3.0,"ETHUSD":+2.0},
    "default":    {"XAUUSD":+1.0,"USOIL":-0.5,"NAS100":-1.0},
}

def _macro_analysis(article):
    """Build full Turkish macro analysis for a news article."""
    headline = article.get("headline","")
    summary  = article.get("summary","")
    text     = (headline+" "+summary).lower()
    imp      = article.get("importance",0)
    rl       = article.get("risk_level","NOISE")
    asent    = article.get("asset_sent",{})
    regs     = article.get("regimes",[])

    # Determine overall directional bias
    bull_words=sum(1 for w in BULL_W if w in text)
    bear_words=sum(1 for w in BEAR_W if w in text)
    total_w=bull_words+bear_words+1
    bull_pct=round(bull_words/total_w*100)
    bear_pct=round(bear_words/total_w*100)
    neut_pct=max(0,100-bull_pct-bear_pct)
    if bull_pct>bear_pct+15: bias="BULLISH"; bias_tr="Yükseliş"
    elif bear_pct>bull_pct+15: bias="BEARISH"; bias_tr="Düşüş"
    else: bias="NEUTRAL"; bias_tr="Nötr"

    # Historical pattern match
    hist_match=None; hist_key=None
    for kw,moves in HIST_PATTERNS.items():
        if kw in text:
            hist_match=moves; hist_key=kw; break

    # Timeframe impact estimate
    if imp>=80:   tf15m="Yüksek"; tf1h="Yüksek";  tf4h="Orta";  tf24h="Orta"
    elif imp>=60: tf15m="Orta";   tf1h="Yüksek";  tf4h="Orta";  tf24h="Düşük"
    elif imp>=40: tf15m="Düşük";  tf1h="Orta";    tf4h="Düşük"; tf24h="Çok Düşük"
    else:         tf15m="Çok Düşük"; tf1h="Düşük"; tf4h="Çok Düşük"; tf24h="—"

    # Per-asset direction
    asset_dirs={}
    for sym in MACRO_ASSETS:
        d="→"; strength=0
        # From sentiment
        for asset,(s,st) in asent.items():
            if asset in sym or sym in asset:
                d="↑" if s=="BULLISH" else "↓"
                strength=st; break
        # Override from hist pattern
        if hist_match and sym in hist_match:
            mv=hist_match[sym]
            d="↑" if mv>0 else "↓"
            strength=abs(mv)
        asset_dirs[sym]=(d,round(strength,1))

    # Volatility expectation
    vol="YÜKSEK" if imp>=80 else "ORTA-YÜKSEK" if imp>=60 else "ORTA" if imp>=40 else "DÜŞÜK"

    # Confidence
    conf=min(95,imp+10) if bias!="NEUTRAL" else max(20,50-imp//4)

    # Duration
    if imp>=80:   dur="Multi-day"
    elif imp>=60: dur="4h"
    elif imp>=40: dur="1h"
    else:         dur="15m"

    # Risk label — never 'Trading Blocked'
    if imp>=80:   risk_label="⚠️  Yüksek Volatilite Bekleniyor"; caution="Dikkatli İşlem Yap"
    elif imp>=60: risk_label="📊 Yönsel Önyargı Onaylandı";      caution="Dikkatli İşlem Yap"
    elif imp>=40: risk_label="📈 Orta Etki";                      caution="Normal İşlem"
    else:         risk_label="🔵 Düşük Etki";                     caution="Normal İşlem"

    # Permanent vs temporary
    perm = any(w in text for w in ["policy","rate","law","regulation","ban","permanent","struktur","yapısal"])

    return {
        "bias":bias,"bias_tr":bias_tr,
        "bull_pct":bull_pct,"bear_pct":bear_pct,"neut_pct":neut_pct,
        "tf15m":tf15m,"tf1h":tf1h,"tf4h":tf4h,"tf24h":tf24h,
        "asset_dirs":asset_dirs,"vol":vol,"conf":conf,"dur":dur,
        "risk_label":risk_label,"caution":caution,
        "hist_key":hist_key,"hist_match":hist_match,
        "permanent":perm,
    }


class MD:
    def __init__(self, sym):
        self.sym     = sym
        self.price   = None
        self.prev    = None
        self.high    = None
        self.low     = None
        self.volume  = 0.0
        self.candles = []          # list of (t,o,h,l,c,v)
        self.ticks   = deque(maxlen=500)
        self.updated = None

    @property
    def chg(self):
        if self.price and self.prev and self.prev:
            return (self.price - self.prev) / self.prev * 100
        return 0.0

market: dict[str, MD] = {}
for s in ALL_SYMBOLS:
    market[s] = MD(s)

news_cache   = []
cat_news     = {}
cot_cache    = {}
setups       = []
stats_cache  = {}
adap_weights = {}

# ── Watchlist Lifecycle Manager ──────────────────────────────────────────────
# Each entry: setup dict + "_wl_status", "_wl_added", "_wl_updated", "_wl_reason"
_wl_active      = {}   # key → setup dict  (currently watching)
active_trades   = {}   # key → trade dict  (entered from watchlist)
_wl_triggered   = []   # list of closed-out setups that hit entry
_wl_invalidated = []   # list with reason why cancelled
_wl_expired     = []   # list that ran past max age
_WL_MAX_AGE_H   = 48   # hours before auto-expiry
_WL_MAX_HISTORY = 40   # keep last N per bucket

def _wl_key(s):
    return f"{s['sym']}_{s['direction']}"

def _wl_structure_changed(old, new):
    """True if market structure or score dropped significantly."""
    if old["direction"] != new["direction"]: return True, "Piyasa yönü değişti"
    if new["score"] < old["score"] - 20:    return True, f"Skor düştü {old['score']:.0f}→{new['score']:.0f}"
    return False, ""

def _wl_sl_invalidated(old, cur_price):
    """True if current price has already moved past the SL."""
    sl=old["sl"]; ep=old["price"]
    if old["direction"]=="LONG"  and cur_price<=sl: return True, f"Fiyat ({fp_plain(cur_price)}) SL altına ({fp_plain(sl)}) düştü"
    if old["direction"]=="SHORT" and cur_price>=sl: return True, f"Fiyat ({fp_plain(cur_price)}) SL üstüne ({fp_plain(sl)}) çıktı"
    return False, ""

def _wl_entry_triggered(old, cur_price):
    """True if price entered the entry zone."""
    el=old["el"]; eh=old["eh"]
    return el<=cur_price<=eh

def fp_plain(v):
    if v is None: return "—"
    a=abs(v)
    if a>10000: return f"{v:,.1f}"
    if a>100:   return f"{v:,.3f}"
    if a>1:     return f"{v:.5f}"
    return f"{v:.6f}"

def update_watchlist(new_setups):
    """Reconcile new analysis results against active watchlist."""
    global _wl_active, _wl_triggered, _wl_invalidated, _wl_expired
    now=datetime.now()
    new_keys={_wl_key(s):s for s in new_setups}

    # ── Check existing watchlist entries ─────────────────────────
    to_remove=[]
    for k,old in list(_wl_active.items()):
        cur_md=market.get(old["sym"])
        cur_price=cur_md.price if cur_md else None
        added=old.get("_wl_added",now)
        age_h=(now-added).total_seconds()/3600

        # 1. Expired
        if age_h>_WL_MAX_AGE_H:
            exp={**old,"_wl_status":"EXPIRED",
                 "_wl_updated":now,"_wl_reason":f"{age_h:.0f} saat sonra zaman aşımı"}
            _wl_expired.insert(0,exp); _wl_expired=_wl_expired[:_WL_MAX_HISTORY]
            to_remove.append(k); continue

        if cur_price is None:
            continue

        # 2. SL invalidated
        inv, reason=_wl_sl_invalidated(old, cur_price)
        if inv:
            inv_entry={**old,"_wl_status":"INVALIDATED",
                       "_wl_updated":now,"_wl_reason":reason,
                       "_wl_fail_condition":"Stop Loss Geçersiz Kılındı"}
            _wl_invalidated.insert(0,inv_entry); _wl_invalidated=_wl_invalidated[:_WL_MAX_HISTORY]
            to_remove.append(k); continue

        # 3. Entry triggered
        if _wl_entry_triggered(old, cur_price):
            trig={**old,"_wl_status":"TRIGGERED","_wl_updated":now,
                  "_wl_reason":f"Giriş bölgesine ulaşıldı ({fp_plain(cur_price)})"}
            _wl_triggered.insert(0,trig); _wl_triggered=_wl_triggered[:_WL_MAX_HISTORY]
            to_remove.append(k); continue

        # 4. Structure changed (new analysis disagrees)
        if k in new_keys:
            changed, reason=_wl_structure_changed(old, new_keys[k])
            if changed:
                inv_entry={**old,"_wl_status":"INVALIDATED",
                           "_wl_updated":now,"_wl_reason":reason,
                           "_wl_fail_condition":"Piyasa Yapısı Değişti"}
                _wl_invalidated.insert(0,inv_entry); _wl_invalidated=_wl_invalidated[:_WL_MAX_HISTORY]
                to_remove.append(k); continue

    for k in to_remove:
        _wl_active.pop(k,None)

    # ── Add new setups to watchlist if not already there ─────────
    for k,s in new_keys.items():
        if k not in _wl_active:
            _wl_active[k]={**s,"_wl_added":now,"_wl_status":"ACTIVE","_wl_updated":now,"_wl_reason":"Yeni setup tespit edildi"}

def enter_trade(key):
    """Move a watchlist setup to active trades."""
    global active_trades
    s=_wl_active.get(key)
    if not s: return False
    now=datetime.now()
    active_trades[key]={**s,
        "_trade_entered":now,
        "_trade_status":"OPEN",
        "_trade_entry_price":market.get(s["sym"],MD(s["sym"])).price or s["price"],
    }
    _wl_active.pop(key,None)
    # Log to DB and send Telegram
    try: log_signal(s)
    except: pass
    try: tg_setup_alert(s)
    except: pass
    return True

def close_trade(key, reason="MANUAL"):
    """Close an active trade and sync DB."""
    t=active_trades.get(key)
    if not t: return
    now=datetime.now(); now_iso=now.isoformat(timespec="seconds")
    sym=t["sym"]
    cur=market.get(sym)
    out_p=cur.price if cur else t["_trade_entry_price"]
    ep=t["_trade_entry_price"]; sl=t["sl"]
    risk=abs(ep-sl) if abs(ep-sl)>0 else 1
    if t["direction"]=="LONG": act_rr=round((out_p-ep)/risk,2)
    else: act_rr=round((ep-out_p)/risk,2)
    db_status="TP" if act_rr>=t.get("rr",1.8) else "SL" if act_rr<=-0.8 else reason
    t.update({"_trade_status":reason,"_trade_closed":now,"_trade_out_price":out_p,"_trade_act_rr":act_rr})
    _wl_triggered.insert(0,{**t,"_wl_status":"TRIGGERED",
        "_wl_reason":f"Kapatıldı: {reason} @ {fp_plain(out_p)} ({act_rr:+.2f}R)"})
    _wl_triggered[:] = _wl_triggered[:_WL_MAX_HISTORY]
    active_trades.pop(key,None)
    # Update DB signal
    try:
        with db() as c:
            c.execute("UPDATE signals SET status=?,out_price=?,out_at=?,act_rr=? WHERE sym=? AND status='OPEN' AND entry=?",
                      (db_status,out_p,now_iso,act_rr,sym,ep))
            c.commit()
    except: pass
    # Telegram outcome
    try: tg_outcome_alert(sym,t["direction"],db_status,ep,out_p,act_rr)
    except: pass

portfolio_state = {
    "heat": 0.0, "open_positions": {},
    "daily_pnl": 0.0, "weekly_pnl": 0.0,
    "daily_risk_used": 0.0, "weekly_risk_used": 0.0,
    "currency_exp": {}, "macro_exp": {},
    "shadow_balance": ACCOUNT["balance"],
    "shadow_peak": ACCOUNT["balance"],
    "shadow_equity": [ACCOUNT["balance"]],
    "shadow_wins": 0, "shadow_losses": 0,
    "shadow_daily_start": ACCOUNT["balance"],
    "inst_risk_score": 100,
}

ws_ok        = False
last_analysis= 0
last_stats   = 0
_last_logged : dict[str,float] = {}

# ═══════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════
def db():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def db_init():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS signals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sym TEXT, quality TEXT, direction TEXT,
            entry REAL, sl REAL, tp REAL,
            rr_t REAL, score INTEGER,
            f_ema INTEGER DEFAULT 0, f_rsi INTEGER DEFAULT 0,
            f_macd INTEGER DEFAULT 0, f_sweep INTEGER DEFAULT 0,
            f_ob INTEGER DEFAULT 0,  f_fvg INTEGER DEFAULT 0,
            f_struct INTEGER DEFAULT 0, f_cot INTEGER DEFAULT 0,
            f_news INTEGER DEFAULT 0,
            status TEXT DEFAULT 'OPEN',
            out_price REAL, out_at TEXT, act_rr REAL,
            created TEXT
        );
        CREATE TABLE IF NOT EXISTS weights(
            feature TEXT PRIMARY KEY,
            mult REAL DEFAULT 1.0,
            win_rate REAL,
            n INTEGER DEFAULT 0,
            updated TEXT
        );
        CREATE TABLE IF NOT EXISTS shadow_trades(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER,
            sym TEXT, direction TEXT,
            entry REAL, sl REAL, tp REAL, rr REAL,
            capital REAL, risk_amount REAL,
            status TEXT DEFAULT 'OPEN',
            pnl REAL, pnl_pct REAL,
            out_price REAL, out_at TEXT,
            created TEXT
        );
        CREATE TABLE IF NOT EXISTS account_state(
            key TEXT PRIMARY KEY, value REAL, updated TEXT
        );
        CREATE INDEX IF NOT EXISTS i1 ON signals(status);
        CREATE INDEX IF NOT EXISTS i2 ON signals(sym);
        CREATE INDEX IF NOT EXISTS i3 ON shadow_trades(status);
        """)
        for f in ["f_ema","f_rsi","f_macd","f_sweep","f_ob","f_fvg","f_struct","f_cot","f_news"]:
            c.execute("INSERT OR IGNORE INTO weights(feature,mult) VALUES(?,1.0)",(f,))
        c.commit()

db_init()

def db_cleanup():
    """
    Purge stale DB entries at startup:
    - OPEN signals older than 30 days → mark EXPIRED
    - OPEN signals older than 7 days → mark EXPIRED
    - Keep all TP/SL/EXPIRED signals for learning (no delete)
    - Keep max 500 closed signals (prune oldest beyond that)
    """
    try:
        with db() as c:
            now_iso=datetime.utcnow().isoformat(timespec="seconds")
            cutoff_7d=(datetime.utcnow()-__import__('datetime').timedelta(days=7)).isoformat(timespec="seconds")
            cutoff_30d=(datetime.utcnow()-__import__('datetime').timedelta(days=30)).isoformat(timespec="seconds")
            # Expire OPEN signals older than 7 days
            c.execute("UPDATE signals SET status='EXPIRED',out_at=? WHERE status='OPEN' AND created<?",
                      (now_iso,cutoff_7d))
            expired_n=c.rowcount
            # Prune TP/SL/EXPIRED beyond 500 (keep most recent for learning)
            ids=c.execute("SELECT id FROM signals WHERE status!='OPEN' ORDER BY id DESC LIMIT -1 OFFSET 500").fetchall()
            if ids:
                c.execute(f"DELETE FROM signals WHERE id IN ({','.join(str(r['id']) for r in ids)})")
            c.commit()
    except: pass

db_cleanup()

# ═══════════════════════════════════════════════════════════════
# PORTFOLIO ENGINE
# ═══════════════════════════════════════════════════════════════
def calc_sizing(setup):
    """
    Calculate position sizing for a setup using Trade212 CFD rules.
    Returns dict with capital, risk_amount, expected_loss/profit, margin.
    """
    sym=setup["sym"]; entry=setup["price"]; sl=setup["sl"]
    rr=setup["rr"]
    lev=get_leverage(sym)
    with lock:
        bal=portfolio_state["shadow_balance"]
        heat=portfolio_state["heat"]
    # Reduce risk if heat is elevated
    risk_pct=ACCOUNT["risk_pct"]
    if heat>10: risk_pct=risk_pct*0.5
    risk_pct=min(risk_pct,ACCOUNT["max_risk_pct"])
    risk_amt=round(bal*risk_pct,2)
    sl_dist_pct=abs(entry-sl)/entry if entry else 0.01
    if sl_dist_pct==0: return None
    # Notional size required to risk exactly risk_amt at SL
    notional=risk_amt/sl_dist_pct
    margin=round(notional/lev,2)
    # Cap margin at 40% of balance
    margin=min(margin,round(bal*0.4,2))
    # Recalculate actual risk at capped margin
    actual_notional=margin*lev
    actual_risk=round(actual_notional*sl_dist_pct,2)
    exp_profit=round(actual_risk*rr,2)
    return {
        "margin":margin,"notional":round(actual_notional,2),
        "risk_amt":actual_risk,"leverage":lev,
        "exp_loss":actual_risk,
        "exp_profit":exp_profit,
        "risk_pct":round(actual_risk/bal*100,2),
        "asset_class":get_asset_class(sym),
    }

def calc_portfolio_heat():
    """Sum risk% of all open positions from shadow_trades."""
    try:
        with db() as c:
            rows=c.execute("SELECT risk_amount FROM shadow_trades WHERE status='OPEN'").fetchall()
        bal=portfolio_state["shadow_balance"]
        total_risk=sum(r["risk_amount"] for r in rows)
        return round(total_risk/bal*100,1) if bal else 0
    except: return 0

def calc_currency_exposure():
    """Net currency exposure across open positions."""
    exp={}
    try:
        with db() as c:
            rows=c.execute("SELECT sym,direction,capital FROM shadow_trades WHERE status='OPEN'").fetchall()
        for r in rows:
            sym=r["sym"]; cap=r["capital"] or 1
            mult=1 if r["direction"]=="LONG" else -1
            for curr,sign in CURR_EXP_MAP.get(sym,{}).items():
                exp[curr]=exp.get(curr,0)+sign*mult*cap
    except: pass
    return {k:round(v,2) for k,v in exp.items()}

def calc_correlation_clusters():
    """Find active correlated clusters among open positions."""
    try:
        with db() as c:
            open_syms={r["sym"] for r in c.execute("SELECT DISTINCT sym FROM shadow_trades WHERE status='OPEN'").fetchall()}
    except: return []
    active=[]
    for cl in CORR_CLUSTERS:
        hits=[s for s in cl["syms"] if s in open_syms]
        if len(hits)>=2: active.append({"label":cl["label"],"syms":hits})
    return active

def calc_macro_exposure():
    """Classify open positions into macro baskets."""
    baskets={"USD Strength":[],"Risk-On":[],"Risk-Off":[],"Inflation":[],"Safe Haven":[]}
    try:
        with db() as c:
            rows=c.execute("SELECT sym,direction FROM shadow_trades WHERE status='OPEN'").fetchall()
        for r in rows:
            s=r["sym"]; d=r["direction"]
            if s in ["EUR/USD","GBP/USD","AUD/USD","NZD/USD"]:
                if d=="SHORT": baskets["USD Strength"].append(s)
            if s in ["BTCUSDT","ETHUSDT","SPY","QQQ","AUD/USD"]:
                if d=="LONG":  baskets["Risk-On"].append(s)
            if s in ["XAU/USD","USD/CHF"]:
                if d=="LONG":  baskets["Safe Haven"].append(s)
                if d=="LONG" and s=="XAU/USD": baskets["Inflation"].append(s)
            if s in ["WTI","BRENT","XAG/USD"]:
                if d=="LONG":  baskets["Inflation"].append(s)
    except: pass
    return {k:v for k,v in baskets.items() if v}

def calc_inst_risk_score():
    """0-100: institutional risk score (higher = safer)."""
    score=100
    heat=portfolio_state.get("heat",0)
    score-=min(heat*2,30)
    cur_exp=portfolio_state.get("currency_exp",{})
    max_exp=max((abs(v) for v in cur_exp.values()),default=0)
    if max_exp>20: score-=15
    elif max_exp>10: score-=8
    clusters=portfolio_state.get("corr_clusters",[])
    score-=len(clusters)*5
    daily_dd=portfolio_state.get("daily_pnl",0)
    if daily_dd<-ACCOUNT["balance"]*ACCOUNT["max_daily_dd"]*0.8: score-=20
    return max(0,round(score,0))

def update_portfolio_state():
    global portfolio_state
    heat=calc_portfolio_heat()
    curr=calc_currency_exposure()
    macro=calc_macro_exposure()
    clusters=calc_correlation_clusters()
    # shadow balance from DB
    try:
        with db() as c:
            bal_row=c.execute("SELECT value FROM account_state WHERE key='shadow_balance'").fetchone()
            if bal_row: sb=bal_row["value"]
            else: sb=ACCOUNT["balance"]
            day_row=c.execute("SELECT value FROM account_state WHERE key='daily_start'").fetchone()
            daily_start=day_row["value"] if day_row else ACCOUNT["balance"]
    except: sb=portfolio_state["shadow_balance"]; daily_start=sb
    with lock:
        portfolio_state["heat"]=heat
        portfolio_state["currency_exp"]=curr
        portfolio_state["macro_exp"]=macro
        portfolio_state["corr_clusters"]=clusters
        portfolio_state["shadow_balance"]=sb
        portfolio_state["daily_pnl"]=round(sb-daily_start,2)
        portfolio_state["inst_risk_score"]=calc_inst_risk_score()

def portfolio_loop():
    while True:
        try: update_portfolio_state()
        except: pass
        time.sleep(30)

# ═══════════════════════════════════════════════════════════════
# QUANT ANALYTICS
# ═══════════════════════════════════════════════════════════════
def monte_carlo(wr_pct, avg_win_r, avg_loss_r, n_trades=163, n_sims=1000, risk_pct=0.01, ruin_threshold=0.5):
    """
    Monte Carlo simulation of equity curve.
    Returns dict with median/best/worst final balance and P(ruin).
    """
    import random
    wr = wr_pct / 100.0
    finals = []
    ruin_count = 0
    for _ in range(n_sims):
        equity = 1.0
        ruined = False
        for _ in range(n_trades):
            if equity <= ruin_threshold:
                ruined = True; break
            if random.random() < wr:
                equity *= (1 + avg_win_r * risk_pct)
            else:
                equity *= (1 - avg_loss_r * risk_pct)
        if ruined or equity <= ruin_threshold:
            ruin_count += 1
        finals.append(equity)
    finals.sort()
    return {
        "median":   round(finals[n_sims // 2], 4),
        "best":     round(finals[int(n_sims * 0.95)], 4),
        "worst":    round(finals[int(n_sims * 0.05)], 4),
        "p_ruin":   round(ruin_count / n_sims * 100, 1),
        "n_sims":   n_sims,
        "n_trades": n_trades,
    }

def rolling_vol(prices, n=20):
    if len(prices) < n+1: return None
    from statistics import stdev
    rets=[(prices[i]/prices[i-1]-1) for i in range(len(prices)-n,len(prices))]
    try: return round(stdev(rets)*math.sqrt(252)*100,2)
    except: return None

def vol_regime(prices):
    v=rolling_vol(prices)
    if v is None: return "?"
    if v<10: return "DÜŞÜK"
    if v<25: return "ORTA"
    return "YÜKSEK"

def historical_var(returns, conf=0.95):
    if len(returns)<10: return None
    return round(sorted(returns)[int((1-conf)*len(returns))],4)

def kelly_pct(wr, avg_win, avg_loss):
    if avg_loss==0: return 0
    b=abs(avg_win/avg_loss); p=wr/100; q=1-p
    k=(b*p-q)/b
    return round(max(0,min(k*100,25)),1)

def sharpe_ratio(returns):
    if len(returns)<3: return None
    from statistics import stdev
    try:
        m=sum(returns)/len(returns); s=stdev(returns)
        return round(m/s*math.sqrt(252),2) if s else None
    except: return None

def sortino_ratio(returns):
    if len(returns)<3: return None
    from statistics import stdev
    try:
        m=sum(returns)/len(returns)
        neg=[r for r in returns if r<0]
        if not neg: return 99.0
        ds=stdev(neg)
        return round(m/ds*math.sqrt(252),2) if ds else None
    except: return None

def max_drawdown_pct(equity):
    if len(equity)<2: return 0
    peak=equity[0]; mdd=0
    for v in equity:
        if v>peak: peak=v
        dd=(peak-v)/peak*100 if peak else 0
        if dd>mdd: mdd=dd
    return round(mdd,2)

# ═══════════════════════════════════════════════════════════════
# INDICATORS
# ═══════════════════════════════════════════════════════════════
def ema(p, n):
    if len(p) < n: return []
    k = 2/(n+1); r = [sum(p[:n])/n]
    for x in p[n:]: r.append(x*k + r[-1]*(1-k))
    return r

def swing_lows(candles, lb=5):
    """Significant swing lows from recent candles."""
    lows=[]
    c=[c[3] for c in candles]
    for i in range(lb, len(c)-lb):
        if c[i]==min(c[i-lb:i+lb+1]):
            lows.append(c[i])
    return lows

def swing_highs(candles, lb=5):
    """Significant swing highs from recent candles."""
    highs=[]
    c=[c[2] for c in candles]
    for i in range(lb, len(c)-lb):
        if c[i]==max(c[i-lb:i+lb+1]):
            highs.append(c[i])
    return highs

def structural_sl(candles, direction, price, av):
    """
    SL based on market structure (swing high/low) + ATR buffer.
    Not random — placed beyond the nearest significant swing level.
    """
    if len(candles)<20 or not av: return None
    recent=candles[-40:]
    if direction=="LONG":
        sl_levels=swing_lows(recent,lb=3)
        # Use the highest swing low below current price
        candidates=[s for s in sl_levels if s < price - av*0.1]
        if candidates:
            swing=max(candidates)
            sl=swing - av*0.25      # buffer below swing low
        else:
            sl=price - av*2.0       # fallback: 2 ATR
        # Don't let SL be more than 3 ATR away (over-exposed)
        sl=max(sl, price - av*3.0)
    else:
        sl_levels=swing_highs(recent,lb=3)
        candidates=[s for s in sl_levels if s > price + av*0.1]
        if candidates:
            swing=min(candidates)
            sl=swing + av*0.25
        else:
            sl=price + av*2.0
        sl=min(sl, price + av*3.0)
    return round(sl, 8)

def structural_tp(candles, direction, price, sl, min_rr=1.8):
    """
    TP placed just before nearest significant S/R level.
    Guarantees minimum RR. Returns (tp, actual_rr).
    """
    if not sl: return None, 0
    risk=abs(price-sl)
    if risk==0: return None, 0
    min_dist=risk*min_rr
    recent=candles[-80:]

    if direction=="LONG":
        # Find resistance levels (swing highs) above price + min_dist
        resistances=swing_highs(recent,lb=3)
        targets=[r for r in resistances if r > price + min_dist]
        if targets:
            # Place TP just before the nearest resistance
            tp=min(targets) * 0.9992
        else:
            # No clear resistance — use 3x ATR target for quality
            tp=price + max(min_dist, abs(price-sl)*3.0)
    else:
        supports=swing_lows(recent,lb=3)
        targets=[s for s in supports if s < price - min_dist]
        if targets:
            tp=max(targets) * 1.0008
        else:
            tp=price - max(min_dist, abs(price-sl)*3.0)

    actual_rr=round(abs(tp-price)/risk, 2) if risk else 0
    return round(tp, 8), actual_rr

def rsi(p, n=14):
    if len(p) < n+1: return None
    d = [p[i+1]-p[i] for i in range(len(p)-1)]
    ag = sum(max(x,0) for x in d[:n])/n
    al = sum(max(-x,0) for x in d[:n])/n
    for x in d[n:]:
        ag = (ag*(n-1)+max(x,0))/n
        al = (al*(n-1)+max(-x,0))/n
    return round(100-100/(1+ag/al),1) if al else 100.0

def macd(p):
    if len(p) < 35: return None,None,None
    ef=ema(p,12); es=ema(p,26)
    n=min(len(ef),len(es))
    ml=[ef[-n+i]-es[-n+i] for i in range(n)]
    sg=ema(ml,9)
    if not sg: return None,None,None
    h=ml[-1]-sg[-1]
    return round(ml[-1],6),round(sg[-1],6),round(h,6)

def atr(candles, n=14):
    if len(candles)<n+1: return None
    trs=[max(candles[i][2]-candles[i][3],
             abs(candles[i][2]-candles[i-1][4]),
             abs(candles[i][3]-candles[i-1][4]))
         for i in range(1,len(candles))]
    return round(sum(trs[-n:])/n, 8) if len(trs)>=n else None

def vwap(candles):
    if not candles: return None
    tv=sum(((c[2]+c[3]+c[4])/3)*c[5] for c in candles)
    v =sum(c[5] for c in candles)
    return tv/v if v else None

def bbands(p, n=20):
    if len(p)<n: return None,None,None
    from statistics import stdev
    mid=sum(p[-n:])/n; s=stdev(p[-n:])
    return round(mid-2*s,8),round(mid,8),round(mid+2*s,8)

def sweep(candles, lb=20):
    if len(candles)<lb+2: return None
    rec=candles[-lb-2:-2]; last=candles[-1]
    sh=max(c[2] for c in rec); sl=min(c[3] for c in rec)
    if last[2]>sh and last[4]<sh: return ("BEAR",sh)
    if last[3]<sl and last[4]>sl: return ("BULL",sl)
    return None

def order_block(candles):
    if len(candles)<10: return None,None
    rec=candles[-20:]; bull=bear=None
    for i in range(len(rec)-3):
        c=rec[i]; nx=rec[i+1:i+4]
        if c[4]<c[1] and any(n[4]>c[2] for n in nx): bull=(c[3],c[2])
        if c[4]>c[1] and any(n[4]<c[3] for n in nx): bear=(c[3],c[2])
    return bull,bear

def fvg(candles):
    if len(candles)<3: return None,None
    fb=fb2=None
    for i in range(len(candles)-2):
        c1,_,c3=candles[i],candles[i+1],candles[i+2]
        if c1[2]<c3[3]: fb=(c1[2],c3[3])
        if c1[3]>c3[2]: fb2=(c1[3],c3[2])
    return fb,fb2

def structure(highs,lows,n=8):
    if len(highs)<n: return "?"
    h=highs[-n:]; l=lows[-n:]
    if h[-1]>h[-2]>h[-3] and l[-1]>l[-2]>l[-3]: return "BULL"
    if h[-1]<h[-2]<h[-3] and l[-1]<l[-2]<l[-3]: return "BEAR"
    return "RANGE"

# ═══════════════════════════════════════════════════════════════
# SCORING ENGINE  (100-point institutional rejection filter)
# ═══════════════════════════════════════════════════════════════
# Point budget:
#   EMA stack      0-11   (8 stack + 3 EMA200)
#   RSI            0-10
#   MACD           0-8
#   VWAP           0-4
#   Bollinger      0-5
#   Liq Sweep      0-12   (required for A+)
#   Order Block    0-8
#   FVG            0-6
#   Structure      0-8
#   Volume         0-4
#   COT            0-10
#   News           0-10
#   RR bonus       10-15  (applied after normalisation)
#   Time bonus     -3..+2
#   Max raw ≈ 96
MAX_RAW = 96

def score_setup(sym, candles, price, news_items=None):
    if len(candles) < 40: return None
    cl=[c[4] for c in candles]
    hi=[c[2] for c in candles]
    lo=[c[3] for c in candles]
    vo=[c[5] for c in candles]

    e9=ema(cl,9); e20=ema(cl,20); e50=ema(cl,50)
    e200=ema(cl,200) if len(cl)>=200 else []
    rv=rsi(cl[-50:])
    ml,ms,mh=macd(cl)
    av=atr(candles)
    vw=vwap(candles[-24:])
    bbl,bbm,bbh=bbands(cl)
    sw=sweep(candles)
    bOB,beOB=order_block(candles)
    bFVG,beFVG=fvg(candles[-10:])
    st=structure(hi,lo)
    avg_v=sum(vo[-20:])/20 if vo else 1
    vol_sp=vo[-1]>avg_v*1.5 if vo else False

    if not av or av==0: return None

    sl_=sr_=0
    rl=[]; rs=[]
    neg_l=[]; neg_s=[]
    fl={"f_ema":0,"f_rsi":0,"f_macd":0,"f_sweep":0,
        "f_ob":0,"f_fvg":0,"f_struct":0,"f_cot":0,"f_news":0}
    sweep_confirmed=False

    # ── EMA Stack (0-11) ─────────────────────────────────────────
    if e9 and e20 and e50:
        if e9[-1]>e20[-1]>e50[-1]:
            sl_+=8; fl["f_ema"]=1; rl.append("EMA 9>20>50 bullish stack")
        elif e9[-1]<e20[-1]<e50[-1]:
            sr_+=8; fl["f_ema"]=1; rs.append("EMA 9<20<50 bearish stack")
        else:
            neg_l.append("EMA stack misaligned — no trend conviction")
            neg_s.append("EMA stack misaligned — no trend conviction")
    if e200:
        if price>e200[-1]: sl_+=3; rl.append("Above EMA200 — macro bullish")
        else:              sr_+=3; rs.append("Below EMA200 — macro bearish")

    # ── RSI (0-10) ───────────────────────────────────────────────
    if rv is not None:
        fl["f_rsi"]=1 if rv<35 or rv>65 else 0
        if rv<30:   sl_+=10; rl.append(f"RSI extreme oversold ({rv})")
        elif rv<40: sl_+=7;  rl.append(f"RSI oversold ({rv})")
        elif rv>70: sr_+=10; rs.append(f"RSI extreme overbought ({rv})")
        elif rv>60: sr_+=7;  rs.append(f"RSI overbought ({rv})")
        else:
            sl_+=3; sr_+=3
            neg_l.append(f"RSI neutral ({rv}) — weak directional signal")
            neg_s.append(f"RSI neutral ({rv}) — weak directional signal")

    # ── MACD (0-8) ───────────────────────────────────────────────
    if mh is not None:
        fl["f_macd"]=1 if abs(mh)>0 else 0
        if mh>0 and ml>ms:   sl_+=8; rl.append(f"MACD bullish crossover (hist {mh:+.5f})")
        elif mh<0 and ml<ms: sr_+=8; rs.append(f"MACD bearish crossover (hist {mh:+.5f})")
        elif mh>0:            sl_+=4; rl.append(f"MACD histogram positive ({mh:+.5f})")
        elif mh<0:            sr_+=4; rs.append(f"MACD histogram negative ({mh:+.5f})")

    # ── VWAP (0-4) ───────────────────────────────────────────────
    if vw:
        dev=(price-vw)/vw*100
        if price>vw*1.001:   sl_+=4; rl.append(f"Above VWAP {dev:+.2f}% — institutional bid")
        elif price<vw*0.999: sr_+=4; rs.append(f"Below VWAP {dev:+.2f}% — sell pressure")

    # ── Bollinger (0-5) ──────────────────────────────────────────
    if bbl and bbh:
        if price<=bbl*1.001:   sl_+=5; rl.append(f"At lower Bollinger ({bbl:.5f}) — mean reversion")
        elif price>=bbh*0.999: sr_+=5; rs.append(f"At upper Bollinger ({bbh:.5f}) — mean reversion")

    # ── Liquidity Sweep (0-12) — required for A+ ─────────────────
    if sw:
        fl["f_sweep"]=1; sweep_confirmed=True
        if sw[0]=="BULL": sl_+=12; rl.append(f"BULLISH LIQUIDITY SWEEP below {sw[1]:.5f} — stops taken")
        else:             sr_+=12; rs.append(f"BEARISH LIQUIDITY SWEEP above {sw[1]:.5f} — stops taken")
    else:
        neg_l.append("No liquidity sweep confirmed")
        neg_s.append("No liquidity sweep confirmed")

    # ── Order Block (0-8) ────────────────────────────────────────
    if bOB and abs(price-(bOB[0]+bOB[1])/2)/((bOB[0]+bOB[1])/2)<0.015:
        fl["f_ob"]=1; sl_+=8; rl.append(f"At BULLISH ORDER BLOCK {bOB[0]:.5f}–{bOB[1]:.5f}")
    if beOB and abs(price-(beOB[0]+beOB[1])/2)/((beOB[0]+beOB[1])/2)<0.015:
        fl["f_ob"]=1; sr_+=8; rs.append(f"At BEARISH ORDER BLOCK {beOB[0]:.5f}–{beOB[1]:.5f}")

    # ── FVG (0-6) ────────────────────────────────────────────────
    if bFVG and bFVG[0]<=price<=bFVG[1]:
        fl["f_fvg"]=1; sl_+=6; rl.append(f"Inside BULLISH FVG {bFVG[0]:.5f}–{bFVG[1]:.5f}")
    if beFVG and beFVG[1]<=price<=beFVG[0]:
        fl["f_fvg"]=1; sr_+=6; rs.append(f"Inside BEARISH FVG {beFVG[1]:.5f}–{beFVG[0]:.5f}")

    # ── Market Structure (0-8) ───────────────────────────────────
    if st=="BULL":
        fl["f_struct"]=1; sl_+=8; rl.append("HH+HL bullish structure — trend continuation")
        neg_s.append("HTF structure is BULLISH — counter-trend short risk")
    elif st=="BEAR":
        fl["f_struct"]=1; sr_+=8; rs.append("LH+LL bearish structure — trend continuation")
        neg_l.append("HTF structure is BEARISH — counter-trend long risk")
    else:
        neg_l.append("Market structure unclear (RANGE) — no institutional bias")
        neg_s.append("Market structure unclear (RANGE) — no institutional bias")

    # ── Volume (0-4) ─────────────────────────────────────────────
    if vol_sp:
        sl_+=4; sr_+=4
        rl.append(f"Volume spike {vo[-1]/avg_v:.1f}x avg — institutional activity")
        rs.append(f"Volume spike {vo[-1]/avg_v:.1f}x avg — institutional activity")
    else:
        neg_l.append("No volume spike — weak institutional participation")
        neg_s.append("No volume spike — weak institutional participation")

    # ── COT (0-10) ───────────────────────────────────────────────
    cot=cot_cache.get(sym,{})
    if cot:
        cs=cot.get("cot_score",0)
        cot_pts=min(abs(cs)*3,10)
        if cs>0:
            fl["f_cot"]=1; sl_+=cot_pts
            rl.append(f"COT bullish ({cot.get('bias','')}, {cot.get('pct_rank',50):.0f}th pct)")
        elif cs<0:
            fl["f_cot"]=1; sr_+=cot_pts
            rs.append(f"COT bearish ({cot.get('bias','')}, {cot.get('pct_rank',50):.0f}th pct)")
        ct=cot.get("contrarian")
        if ct=="LONG":  fl["f_cot"]=1; sl_+=2; rl.append("COT CONTRARIAN LONG — specs at extreme short")
        elif ct=="SHORT": fl["f_cot"]=1; sr_+=2; rs.append("COT CONTRARIAN SHORT — specs at extreme long")
    else:
        neg_l.append("COT data unavailable — no open-interest confirmation")
        neg_s.append("COT data unavailable — no open-interest confirmation")

    # ── Portfolio heat check ─────────────────────────────────────
    heat=portfolio_state.get("heat",0)
    if heat>=15:
        return None   # HARD REJECT — portfolio heat too high

    # ── News Risk Check (caution only — never blocks trading) ──
    n_imp, n_rl, n_caution = news_risk_for_sym(sym)

    # ── News Sentiment (0-10) ────────────────────────────────────
    news_score=0; news_rl=[]; news_rs=[]
    news_penalty=0
    if n_caution and n_imp>=80:
        news_penalty=5   # high-impact news → slight confidence reduction only
        neg_l.append(f"⚠️ Yüksek Volatilite Bekleniyor (impact {n_imp}/100) — Dikkatli İşlem Yap")
        neg_s.append(f"⚠️ Yüksek Volatilite Bekleniyor (impact {n_imp}/100) — Dikkatli İşlem Yap")
    elif n_imp>=60:
        news_penalty=2
        neg_l.append(f"📊 Yönsel Önyargı Onaylandı (impact {n_imp}/100)")
        neg_s.append(f"📊 Yönsel Önyargı Onaylandı (impact {n_imp}/100)")

    if news_items:
        for n in news_items:
            t=(n.get("headline","")+n.get("summary","")).lower()
            b=sum(1 for w in BULL_W if w in t)
            be=sum(1 for w in BEAR_W if w in t)
            news_score+=(b-be)
        news_pts=min(abs(news_score)*2,10)
        if news_score>=2:
            fl["f_news"]=1; sl_+=news_pts
            news_rl=[f"NEWS BULLISH (score +{news_score}): {news_items[0].get('headline','')[:70]}"]
        elif news_score<=-2:
            fl["f_news"]=1; sr_+=news_pts
            news_rs=[f"NEWS BEARISH (score {news_score}): {news_items[0].get('headline','')[:70]}"]
        else:
            neg_l.append("No strong news catalyst")
            neg_s.append("No strong news catalyst")
    else:
        neg_l.append("No news data available")
        neg_s.append("No news data available")

    # ── Adaptive weight bonus (max ±5) ───────────────────────────
    for feat,val in fl.items():
        if val:
            w=adap_weights.get(feat,1.0)
            sl_=sl_+(w-1.0)*2 if sl_>=sr_ else sl_
            sr_=sr_+(w-1.0)*2 if sr_>sl_ else sr_

    # ── Pick direction ───────────────────────────────────────────
    if sl_>=sr_:
        direction="LONG"; raw=sl_; reasons=rl+news_rl; neg_factors=neg_l
    else:
        direction="SHORT"; raw=sr_; reasons=rs+news_rs; neg_factors=neg_s

    # ── Compute entry zone ───────────────────────────────────────
    if direction=="LONG":
        el=price-av*0.2; eh=price+av*0.1
    else:
        el=price-av*0.1; eh=price+av*0.2

    # ── Structural SL (swing-based + ATR buffer) ─────────────────
    sl=structural_sl(candles, direction, price, av)
    if sl is None: return None

    # ── Structural TP (S/R based, min 1:1.8) ────────────────────
    tp, rr=structural_tp(candles, direction, price, sl, min_rr=1.8)
    if tp is None or rr<1.8: return None   # HARD REJECT

    # ── RR bonus scoring ─────────────────────────────────────────
    rr_bonus = 16 if rr>=3.5 else 14 if rr>=3.0 else 12 if rr>=2.5 else 9 if rr>=2.0 else 6
    if rr>=3.0:  reasons.append(f"Excellent RR 1:{rr} — yüksek beklenti")
    elif rr>=2.5: reasons.append(f"Güçlü RR 1:{rr}")
    elif rr>=2.0: reasons.append(f"İyi RR 1:{rr}")
    else: neg_factors.append(f"RR 1:{rr} — kabul edilebilir minimum")

    # ── Smart Money Analysis ─────────────────────────────────────
    sm_notes=[]
    trap_warnings=[]
    # Liquidity trap: price at obvious level after big move — potential fake breakout
    recent_hi=max(hi[-20:]) if hi else price
    recent_lo=min(lo[-20:]) if lo else price
    near_top=price>recent_hi*0.995
    near_bot=price<recent_lo*1.005
    if direction=="LONG" and near_bot and not sw:
        sm_notes.append("Price at recent low — possible stop-hunt zone or accumulation")
    if direction=="SHORT" and near_top and not sw:
        sm_notes.append("Price at recent high — possible liquidity grab or distribution")
    if sw:
        sm_notes.append(f"Liquidity sweep confirmed at {sw[1]:.5f} — smart money absorbed stops")
    if bOB:
        sm_notes.append(f"Institutional order block present {bOB[0]:.5f}–{bOB[1]:.5f}")
    if bFVG:
        sm_notes.append(f"Fair Value Gap imbalance {bFVG[0]:.5f}–{bFVG[1]:.5f} — likely to fill")

    # Fake breakout trap warning
    if near_top and direction=="LONG" and not sw:
        trap_warnings.append("⚠️ BREAKOUT TRAP: Buying at recent high without sweep — retail longs may be trapped")
    if near_bot and direction=="SHORT" and not sw:
        trap_warnings.append("⚠️ BREAKDOWN TRAP: Shorting at recent low without sweep — retail shorts may be trapped")
    if not bOB and not beFVG and not sw:
        trap_warnings.append("⚠️ No institutional confirmation — setup may lack smart money backing")

    # ── Contrarian Score (0-100) ─────────────────────────────────
    # High = contrarian opportunity; Low = follow the trend
    c_score=0
    if cot:
        pr=cot.get("pct_rank",50)
        if pr>=90 or pr<=10: c_score+=40   # extreme positioning
        elif pr>=80 or pr<=20: c_score+=20
    if sw: c_score+=25   # sweep = possible reversal
    if (near_top and direction=="SHORT") or (near_bot and direction=="LONG"): c_score+=15
    if n_imp>=60: c_score+=10   # news priced in risk
    if rv and (rv>75 or rv<25): c_score+=10
    c_score=min(c_score,100)
    if c_score>=70:
        c_label="Contrarian Fırsat"; c_col="bright_yellow"
    elif c_score>=40:
        c_label="Nötr"; c_col="yellow"
    else:
        c_label="Trend Takip Et"; c_col="bright_green"

    # Market regime from structure + RSI + EMA
    if st=="BULL" and (rv or 50)<65:   regime="Risk-On"
    elif st=="BEAR" and (rv or 50)>35: regime="Risk-Off"
    else: regime="Nötr"

    # ── Equity bonus — stocks get +4 if EMA+structure aligned ───
    eq_bonus=0
    if get_asset_class(sym) in ("stocks","indices") and fl["f_ema"] and fl["f_struct"]:
        eq_bonus=4

    # ── Normalise to 100 then blend RR bonus ─────────────────────
    score_100=round(min(max(raw/MAX_RAW*85+rr_bonus+eq_bonus-news_penalty,0),100),1)

    # ── Expected hold time (TP distance ÷ ATR = hours) ───────────
    hold_h=round(abs(tp-price)/av,1) if av else 8.0
    time_bonus=(2 if hold_h<=4 else 1 if hold_h<=8 else 0 if hold_h<=24 else -3)
    score_100=round(min(max(score_100+time_bonus,0),100),1)

    if hold_h>24: neg_factors.append(f"Hold time ~{hold_h:.0f}h — capital locked overnight+")
    elif hold_h>8: neg_factors.append(f"Hold time ~{hold_h:.0f}h — crosses session boundary")

    # ── HARD REJECT: score too low ───────────────────────────────
    if score_100<38: return None

    # ── Quality thresholds ───────────────────────────────────────
    if   score_100>=80: quality="A+"
    elif score_100>=66: quality="A"
    elif score_100>=50: quality="B+"
    elif score_100>=38: quality="WATCH"
    else: return None

    # A+ requires liquidity sweep
    if quality=="A+" and not sweep_confirmed:
        quality="A"

    # Status: score>=58 → APPROVED directly
    if score_100>=58:
        status="APPROVED"
    elif score_100>=38:
        status="WATCHLIST"
    else:
        status="REJECTED"

    confidence=score_100

    news_risk_label=("NO RISK" if n_imp<20 else "LOW" if n_imp<40
                     else "MEDIUM" if n_imp<60 else "HIGH" if n_imp<80 else "CRITICAL")

    # Build placeholder setup for sizing (fill in entry/sl/tp before calling)
    _tmp={"sym":sym,"price":price,"sl":sl,"rr":rr}
    sizing=calc_sizing(_tmp) or {}

    # Institutional risk score
    inst_rs=portfolio_state.get("inst_risk_score",100)

    duration=("Scalp" if hold_h<=2 else "Intraday" if hold_h<=12 else "Swing")
    # Consensus vs Smart Money view contrast
    ema_bull=fl.get("f_ema",False) and direction=="LONG"
    ema_bear=fl.get("f_ema",False) and direction=="SHORT"
    consensus_bias="Yükseliş" if ema_bull else ("Düşüş" if ema_bear else "Nötr")
    sm_conf=any([fl.get("f_ob"),fl.get("f_fvg"),fl.get("f_sweep")])
    sm_bias=("Yükseliş" if direction=="LONG" else "Düşüş") if sm_conf else "Belirsiz"
    contrast_txt="✅ Uyumlu" if consensus_bias==sm_bias else "⚡ Uyumsuz — dikkat"
    consensus_view=f"Perakende görüş: {consensus_bias} (EMA+trend takipçileri)"
    sm_view=f"Smart Money: {sm_bias} (OB/sweep/FVG) — {contrast_txt}"

    return {
        "sym":sym,"quality":quality,"direction":direction,"status":status,
        "price":price,"el":el,"eh":eh,"sl":sl,
        "tp":tp,"rr":rr,
        "score":score_100,"confidence":confidence,"hold_h":hold_h,"duration":duration,
        "consensus_view":consensus_view,"sm_view":sm_view,
        "reasons":reasons,"neg_factors":neg_factors,
        "flags":fl,"rsi":rv,"atr":av,"cot":cot,
        "news_score":news_score,"news_imp":n_imp,"news_risk":news_risk_label,
        "sizing":sizing,"inst_risk_score":inst_rs,"portfolio_heat":heat,
        "sm_notes":sm_notes,"trap_warnings":trap_warnings,
        "contrarian_score":c_score,"contrarian_label":c_label,
        "regime":regime,
        "time":datetime.now().strftime("%H:%M:%S"),
        "narrative": _narrative(sym,direction,price,el,eh,sl,tp,rr,av,rv,mh,st,sw,bOB,beOB,bFVG,beFVG,cot,news_rl+news_rs,news_score),
    }

def _narrative(sym,dirn,price,el,eh,sl,tp,rr,av,rv,mh,st,sw,bOB,beOB,bFVG,beFVG,cot,news,ns):
    L=[f"WHY ENTER {dirn} on {sym}:",""]
    ow="upside" if dirn=="LONG" else "downside"
    if st=="BULL" and dirn=="LONG": L.append("► HH+HL structure — bulls in control, trend continuation.")
    elif st=="BEAR" and dirn=="SHORT": L.append("► LH+LL structure — bears in control, trend continuation.")
    else: L.append("► Ranging market — playing boundary extremes.")
    if sw:
        if sw[0]=="BULL" and dirn=="LONG": L.append(f"► Liquidity swept below {sw[1]:.5f} — retail stops taken, smart money absorbed.")
        elif sw[0]=="BEAR" and dirn=="SHORT": L.append(f"► Liquidity swept above {sw[1]:.5f} — retail longs stopped out, distribution complete.")
    if bOB and dirn=="LONG": L.append(f"► Bullish Order Block {bOB[0]:.5f}–{bOB[1]:.5f} — institutional buy zone defended.")
    if beOB and dirn=="SHORT": L.append(f"► Bearish Order Block {beOB[0]:.5f}–{beOB[1]:.5f} — institutional sell zone active.")
    if bFVG and dirn=="LONG": L.append(f"► Bullish FVG {bFVG[0]:.5f}–{bFVG[1]:.5f} — price filling imbalance.")
    if beFVG and dirn=="SHORT": L.append(f"► Bearish FVG {beFVG[1]:.5f}–{beFVG[0]:.5f} — distribution zone.")
    if rv:
        if rv<30 and dirn=="LONG": L.append(f"► RSI {rv} — extreme oversold, statistical reversal edge maximum.")
        elif rv>70 and dirn=="SHORT": L.append(f"► RSI {rv} — extreme overbought, institutional fade zone.")
        else: L.append(f"► RSI {rv} — neutral, room to extend {ow}.")
    if mh:
        if mh>0 and dirn=="LONG": L.append(f"► MACD expanding positive — momentum accelerating {ow}.")
        elif mh<0 and dirn=="SHORT": L.append(f"► MACD expanding negative — sellers in control.")
    if cot:
        pr=cot.get("pct_rank",50); bias=cot.get("bias","")
        L.append(f"► COT: Spec positioning {pr:.0f}th percentile → {bias}")
        if cot.get("contrarian"): L.append(f"  ⚠ CONTRARIAN SIGNAL: specs at extreme — fade the crowd.")
    if news: L.append(""); L.append("MACRO/NEWS:"); [L.append(f"  {n}") for n in news[:2]]
    L+= ["","RISK MANAGEMENT:",
         f"  Entry   : {el:.5f} – {eh:.5f}",
         f"  Stop    : {sl:.5f}  (close beyond = immediate exit)",
         f"  TP      : {tp:.5f}  → full exit at target",
         f"  R:R     : 1:{rr}  |  Risk max 1-2% of capital"]
    return "\n".join(L)

# ═══════════════════════════════════════════════════════════════
# COT ENGINE (CFTC public API)
# ═══════════════════════════════════════════════════════════════
def fetch_cot_all():
    global cot_cache
    new={}
    for sym,mkt in COT_MAP.items():
        try:
            kw=mkt.split(" - ")[0][:30]
            r=requests.get("https://publicreporting.cftc.gov/resource/jun7-fc8e.json",
                params={"$where":f"upper(market_and_exchange_names) like upper('%{kw}%')",
                        "$order":"report_date_as_yyyy_mm_dd DESC","$limit":"12"},timeout=10)
            rows=r.json()
            if not isinstance(rows,list) or not rows: continue
            def iv(d,k):
                try: return int(d.get(k,0))
                except: return 0
            parsed=[]
            for d in rows:
                parsed.append({
                    "date":d.get("report_date_as_yyyy_mm_dd","")[:10],
                    "sl":iv(d,"noncomm_positions_long_all"),
                    "ss":iv(d,"noncomm_positions_short_all"),
                    "slc":iv(d,"change_in_noncomm_long_all"),
                    "ssc":iv(d,"change_in_noncomm_short_all"),
                    "cl":iv(d,"comm_positions_long_all"),
                    "cs":iv(d,"comm_positions_short_all"),
                    "oi":iv(d,"open_interest_all"),
                })
            if not parsed: continue
            nets=[p["sl"]-p["ss"] for p in parsed]
            mn,mx=min(nets),max(nets)
            rng=mx-mn if mx!=mn else 1
            pr=round((nets[0]-mn)/rng*100,1)
            bias=("EXTREME LONG" if pr>=75 else "BULLISH" if pr>=55
                  else "EXTREME SHORT" if pr<=25 else "BEARISH" if pr<=45 else "NEUTRAL")
            ct=("SHORT" if pr>=85 else "LONG" if pr<=15 else None)
            sn_chg=parsed[0]["slc"]-parsed[0]["ssc"]
            cs=2 if pr<=25 else -2 if pr>=75 else 0
            if sn_chg>0: cs+=1
            elif sn_chg<0: cs-=1
            if ct=="LONG": cs+=2
            elif ct=="SHORT": cs-=2
            new[sym]={
                "date":parsed[0]["date"],"pct_rank":pr,"bias":bias,"contrarian":ct,
                "spec_net":nets[0],"spec_chg":sn_chg,
                "comm_net":parsed[0]["cl"]-parsed[0]["cs"],
                "oi":parsed[0]["oi"],"cot_score":cs,
            }
        except: pass
        time.sleep(0.3)
    with lock: cot_cache=new

# ═══════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════
def _fetch_one_yf(name, ticker):
    try:
        tk = yf.Ticker(ticker)
        df = tk.history(period="5d", interval="1h", auto_adjust=True)
        if df is None or df.empty: return
        df = df.dropna(subset=["Close"])
        if df.empty: return
        row = df.iloc[-1]
        prev = float(df.iloc[-2]["Close"]) if len(df) > 1 else float(row["Close"])
        candles = []
        for ts, r in df.iterrows():
            t = int(ts.timestamp()) if hasattr(ts, "timestamp") else 0
            candles.append((t, float(r["Open"]), float(r["High"]),
                            float(r["Low"]), float(r["Close"]), float(r.get("Volume", 0) or 0)))
        with lock:
            md = market[name]
            md.price   = float(row["Close"])
            md.prev    = prev
            md.high    = float(row["High"])
            md.low     = float(row["Low"])
            md.volume  = float(row.get("Volume", 0) or 0)
            md.candles = candles
            md.updated = datetime.now().strftime("%H:%M:%S")
    except: pass

def load_yfinance():
    """Fetch quotes + candles for all YF symbols, one ticker per thread."""
    threads = []
    for name, ticker in YF_SYMBOLS.items():
        th = threading.Thread(target=_fetch_one_yf, args=(name, ticker), daemon=True)
        th.start(); threads.append(th)
    for th in threads: th.join(timeout=30)

def load_finnhub_equity():
    """Fetch equity quotes from Finnhub REST."""
    for sym in EQ_SYMBOLS:
        try:
            r=requests.get(f"{BASE_URL}/quote",params={"symbol":sym,"token":API_KEY},timeout=4)
            q=r.json()
            if q.get("c"):
                with lock:
                    md=market[sym]
                    md.price=q["c"]; md.prev=q["pc"]
                    md.high=q["h"];  md.low=q["l"]
                    md.updated=datetime.now().strftime("%H:%M:%S")
        except: pass

def load_equity_candles():
    """Fetch equity candles from Finnhub."""
    all_syms=EQ_SYMBOLS
    for sym in all_syms:
        try:
            now=int(time.time()); frm=now-200*3600
            p={"symbol":sym,"resolution":"60","from":frm,"to":now,"token":API_KEY}
            ep=f"{BASE_URL}/stock/candle"
            r=requests.get(ep,params=p,timeout=6); d=r.json()
            if d.get("s")=="ok":
                c=list(zip(d["t"],d["o"],d["h"],d["l"],d["c"],d["v"]))
                with lock:
                    market[sym].candles=c
                    if c: market[sym].price=c[-1][4]
        except: pass

def load_news():
    global news_cache, cat_news, analyzed_news
    items=[]
    for cat in ["general","forex","merger","macro"]:
        try:
            r=requests.get(f"{BASE_URL}/news",params={"category":cat,"token":API_KEY},timeout=5)
            d=r.json()
            if isinstance(d,list): items.extend(d[:10])
        except: pass
    for sym in ["NVDA","AAPL","MSFT","TSLA"]:
        try:
            today=datetime.utcnow().strftime("%Y-%m-%d")
            frm=datetime.utcfromtimestamp(time.time()-7*86400).strftime("%Y-%m-%d")
            r=requests.get(f"{BASE_URL}/company-news",
                          params={"symbol":sym,"from":frm,"to":today,"token":API_KEY},timeout=5)
            d=r.json()
            if isinstance(d,list): items.extend(d[:8])
        except: pass
    seen=set(); unique=[]
    for x in items:
        h=x.get("headline","")
        if h and h not in seen: seen.add(h); unique.append(x)
    # ── Institutional analysis ──
    enriched=[]
    for x in unique:
        try:
            a=analyze_article(x); enriched.append(a)
            # Telegram alert for high-impact fresh news
            ts=a.get("datetime",0)
            age_min=(time.time()-ts)/60 if ts else 999
            if a["importance"]>=70 and age_min<=60:
                threading.Thread(target=send_telegram,args=(a,),daemon=True).start()
        except: enriched.append(x)
    enriched.sort(key=lambda x:-x.get("importance",0))
    # per-symbol categorisation
    new_cat={}
    for sym in list(market.keys()):
        kws=NEWS_KW.get(sym,[])
        if not kws: continue
        matched=[]
        for x in enriched:
            txt=(x.get("headline","")+x.get("summary","")).lower()
            if any(k in txt for k in kws) or sym in x.get("sym_impacts",{}):
                ts=x.get("datetime",0)
                age=(time.time()-ts)/3600 if ts else 99
                matched.append({**x,"age_h":round(age,1)})
        matched.sort(key=lambda x:(-x.get("importance",0),x["age_h"]))
        if matched: new_cat[sym]=matched[:4]
    with lock:
        news_cache=unique[:30]; cat_news=new_cat; analyzed_news=enriched[:30]

def background_loop():
    first=True
    while True:
        try: load_yfinance()
        except: pass
        try: load_finnhub_equity()
        except: pass
        try: load_equity_candles()
        except: pass
        try: load_news()
        except: pass
        time.sleep(30 if first else 60); first=False

def cot_loop():
    while True:
        try: fetch_cot_all()
        except: pass
        time.sleep(12*3600)

# ═══════════════════════════════════════════════════════════════
# WEBSOCKET (crypto)
# ═══════════════════════════════════════════════════════════════
def ws_msg(ws,msg):
    try:
        d=json.loads(msg)
        if d.get("type")=="trade":
            for t in d["data"]:
                sym=t["s"].replace("BINANCE:","")
                p=t["p"]; v=t.get("v",0)
                ts=t.get("t",int(time.time()*1000))//1000
                with lock:
                    md=market[sym]
                    if md.price is None: md.prev=p
                    if md.high is None or p>md.high: md.high=p
                    if md.low  is None or p<md.low:  md.low=p
                    md.prev=md.price or p; md.price=p
                    md.volume+=v; md.ticks.append((ts,p,v))
                    md.updated=datetime.now().strftime("%H:%M:%S")
    except: pass

def ws_open(ws):
    global ws_ok; ws_ok=True
    for s in WS_SYMBOLS:
        ws.send(json.dumps({"type":"subscribe","symbol":s}))

def ws_close(ws,c,m): global ws_ok; ws_ok=False
def ws_err(ws,e):     global ws_ok; ws_ok=False

def ws_loop():
    """WebSocket disabled — crypto removed."""
    pass

def ticks_to_candles(ticks,sec=60):
    if not ticks: return []
    ticks=list(ticks); candles=[]; o=h=l=c=None; v=0
    b=ticks[0][0]-(ticks[0][0]%sec)
    for ts,p,vol in ticks:
        nb=ts-(ts%sec)
        if nb!=b and o is not None:
            candles.append((b,o,h,l,c,v)); o=h=l=c=None; v=0; b=nb
        if o is None: o=h=l=p
        h=max(h,p); l=min(l,p); c=p; v+=vol
    if o is not None: candles.append((b,o,h,l,c,v))
    return candles

# ═══════════════════════════════════════════════════════════════
# JOURNAL
# ═══════════════════════════════════════════════════════════════
def log_signal(s):
    key=f"{s['sym']}_{s['direction']}"
    now=time.time()
    if now-_last_logged.get(key,0)<DEDUP_SEC: return
    _last_logged[key]=now
    fl=s.get("flags") or {}
    sz=s.get("sizing",{}) or {}
    created=datetime.utcnow().isoformat(timespec="seconds")
    with db() as c:
        # Zaten açık bu sembol+yön var mı? Varsa tekrar yazma
        existing=c.execute(
            "SELECT id FROM signals WHERE sym=? AND direction=? AND status='OPEN' ORDER BY id DESC LIMIT 1",
            (s["sym"],s["direction"])).fetchone()
        if existing:
            sig_id=existing["id"]
            # Skoru güncelle (iyileştiyse)
            c.execute("UPDATE signals SET score=?,quality=?,sl=?,tp=?,rr_t=? WHERE id=?",
                      (s["score"],s["quality"],s["sl"],s["tp"],s["rr"],sig_id))
            c.commit()
        else:
            cur=c.execute("""INSERT INTO signals(sym,quality,direction,entry,sl,tp,
                rr_t,score,f_ema,f_rsi,f_macd,f_sweep,f_ob,f_fvg,f_struct,f_cot,f_news,
                status,created)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)""",
                (s["sym"],s["quality"],s["direction"],s["price"],s["sl"],
                 s["tp"],s["rr"],s["score"],
                 fl.get("f_ema",0),fl.get("f_rsi",0),fl.get("f_macd",0),fl.get("f_sweep",0),
                 fl.get("f_ob",0),fl.get("f_fvg",0),fl.get("f_struct",0),fl.get("f_cot",0),fl.get("f_news",0),
                 created))
            sig_id=cur.lastrowid
            # Shadow trade
            if sz:
                c.execute("""INSERT INTO shadow_trades(signal_id,sym,direction,
                    entry,sl,tp,rr,capital,risk_amount,status,created)
                    VALUES(?,?,?,?,?,?,?,?,?,'OPEN',?)""",
                    (sig_id,s["sym"],s["direction"],s["price"],s["sl"],
                     s["tp"],s["rr"],sz.get("margin",0),sz.get("risk_amt",0),created))
                c.execute("INSERT OR REPLACE INTO account_state(key,value,updated) VALUES('shadow_balance',?,?)",
                          (portfolio_state["shadow_balance"],created))
                c.execute("INSERT OR IGNORE INTO account_state(key,value,updated) VALUES('daily_start',?,?)",
                          (portfolio_state["shadow_balance"],created))
            c.commit()

        # active_trades dict'e ekle — web panelinde hemen görünsün
        at_key=f"{s['sym']}_{s['direction']}"
        with lock:
            if at_key not in active_trades:
                active_trades[at_key]={
                    **s,
                    "db_id": sig_id,
                    "_trade_entered": datetime.now(),
                    "_trade_status": "OPEN",
                    "_trade_entry_price": s["price"],
                }
        _last_logged[key]=now

def monitor_loop():
    while True:
        try: _check_open()
        except: pass
        time.sleep(15)  # 60→15sn: TP/SL daha hızlı yakalanır

def _check_open():
    now=datetime.utcnow().isoformat(timespec="seconds")
    # Auto-expire old active_trades entries (>7 days open)
    with lock:
        stale=[k for k,t in active_trades.items()
               if (datetime.now()-t.get("_trade_entered",datetime.now())).days>=7]
        for k in stale:
            t=active_trades.pop(k,None)
            if t:
                _wl_triggered.insert(0,{**t,"_wl_status":"TRIGGERED",
                    "_wl_reason":"7 gün sonra zaman aşımı — otomatik kapatıldı"})
                _wl_triggered[:] = _wl_triggered[:_WL_MAX_HISTORY]
    with db() as c:
        rows=c.execute("SELECT * FROM signals WHERE status='OPEN'").fetchall()
        for r in rows:
            sym=r["sym"]; ep=r["entry"]; sl=r["sl"]; tp=r["tp"]
            with lock: md=market.get(sym); cur=md.price if md else None
            if cur is None: continue
            risk=abs(ep-sl)
            if risk==0: continue
            ns=None; arr=None
            if r["direction"]=="LONG":
                if cur<=sl:    ns="SL";  arr=round(-risk/ep*100,3) if ep else -1.0; arr_r=-1.0
                elif cur>=tp:  ns="TP";  arr_r=round(r["rr_t"],2); arr=round((cur-ep)/ep*100,3) if ep else arr_r
            else:
                if cur>=sl:    ns="SL";  arr=round(-risk/ep*100,3) if ep else -1.0; arr_r=-1.0
                elif cur<=tp:  ns="TP";  arr_r=round(r["rr_t"],2); arr=round((ep-cur)/ep*100,3) if ep else arr_r
            # Auto-expire after 7 days
            try:
                age=(datetime.utcnow()-datetime.fromisoformat(r["created"])).days
                if age>=7 and ns is None:
                    ns="EXPIRED"
                    arr_r=round((cur-ep)/risk,2) if r["direction"]=="LONG" else round((ep-cur)/risk,2)
                    arr=arr_r
            except: pass
            if ns:
                act_rr_val=arr_r if 'arr_r' in dir() else arr
                c.execute("UPDATE signals SET status=?,out_price=?,out_at=?,act_rr=? WHERE id=?",
                          (ns,cur,now,act_rr_val,r["id"]))
                # Categorized Telegram outcome
                try: tg_outcome_alert(sym,r["direction"],ns,ep,cur,act_rr_val,sig_id=r["id"])
                except: pass
                # Sync active_trades dict — remove closed entries
                with lock:
                    keys_to_rm=[k for k,t in active_trades.items()
                                if t.get("sym")==sym and abs((t.get("_trade_entry_price") or 0)-ep)<1e-8]
                    for k in keys_to_rm:
                        t2=active_trades.pop(k,None)
                        if t2:
                            _wl_triggered.insert(0,{**t2,"_wl_status":"TRIGGERED",
                                "_wl_reason":f"{ns} @ {fp_plain(cur)} ({act_rr_val:+.2f}R)"})
                            _wl_triggered[:] = _wl_triggered[:_WL_MAX_HISTORY]
                # Update shadow trade + balance
                st_row=c.execute("SELECT * FROM shadow_trades WHERE signal_id=? AND status='OPEN'",
                                 (r["id"],)).fetchone()
                if st_row:
                    cap=st_row["capital"] or 0; risk_amt=st_row["risk_amount"] or 0
                    pnl=round(act_rr_val*risk_amt,2) if act_rr_val is not None else 0
                    bal_row=c.execute("SELECT value FROM account_state WHERE key='shadow_balance'").fetchone()
                    old_bal=bal_row["value"] if bal_row else ACCOUNT["balance"]
                    new_bal=round(old_bal+pnl,2)
                    c.execute("UPDATE shadow_trades SET status=?,out_price=?,out_at=?,pnl=?,pnl_pct=? WHERE id=?",
                              (ns,cur,now,pnl,round(pnl/cap*100,1) if cap else 0,st_row["id"]))
                    c.execute("INSERT OR REPLACE INTO account_state(key,value,updated) VALUES('shadow_balance',?,?)",
                              (new_bal,now))
                    with lock:
                        portfolio_state["shadow_balance"]=new_bal
                        portfolio_state["shadow_equity"].append(new_bal)
                        if ns=="TP":   portfolio_state["shadow_wins"]+=1
                        elif ns=="SL": portfolio_state["shadow_losses"]+=1
                # Trigger adaptive learning immediately after each close
                try: compute_stats()
                except: pass
        c.commit()

def compute_stats():
    global stats_cache, adap_weights
    with db() as c:
        # Use only signals from last 90 days for relevance
        cutoff=(datetime.utcnow()-__import__('datetime').timedelta(days=90)).isoformat(timespec="seconds")
        all_closed=c.execute("SELECT * FROM signals WHERE status!='OPEN'").fetchall()
        closed=[r for r in all_closed if (r["created"] or "")>=cutoff] or all_closed
        if not closed: return
        total=len(closed)
        wins=[r for r in closed if r["status"]=="TP"]
        losses=[r for r in closed if r["status"]=="SL"]
        expired=[r for r in closed if r["status"]=="EXPIRED"]
        wr=round(len(wins)/total*100,1)
        rrs=[r["act_rr"] for r in closed if r["act_rr"] is not None]
        avg_rr=round(sum(rrs)/len(rrs),2) if rrs else 0
        gw=sum(r for r in rrs if r>0); gl=abs(sum(r for r in rrs if r<0))
        pf=round(gw/gl,2) if gl else 99.0

        # Quality breakdown
        bq={}
        for q in ("A+","A","B+"):
            qr=[r for r in closed if r["quality"]==q]
            qw=[r for r in qr if r["status"]=="TP"]
            bq[q]={"t":len(qr),"w":len(qw),"wr":round(len(qw)/len(qr)*100,1) if qr else 0}

        # Feature win-rate → adaptive weights (lower threshold to 5 trades for faster learning)
        feats=["f_ema","f_rsi","f_macd","f_sweep","f_ob","f_fvg","f_struct","f_cot","f_news"]
        fstats={}; new_w={}
        # Recency-weighted öğrenme: son işlemler 2x ağırlıklı (closed eskiden->yeniye)
        n_closed=len(closed)
        for f in feats:
            fr=[r for r in closed if r[f]==1]
            fw=[r for r in fr if r["status"]=="TP"]
            # Düz win-rate
            fwr=round(len(fw)/len(fr)*100,1) if fr else None
            # Recency-ağırlıklı win-rate (son işlemler daha çok sayılır)
            wnum=wden=0.0
            for r in fr:
                try: pos=closed.index(r)
                except ValueError: pos=0
                rw=1.0+(pos/max(1,n_closed-1))   # eski=1.0 → yeni=2.0
                wden+=rw
                if r["status"]=="TP": wnum+=rw
            rwr=round(wnum/wden*100,1) if wden else fwr
            fstats[f]={"n":len(fr),"w":len(fw),"wr":fwr,"rwr":rwr}
            # Hızlı öğrenme: min 3 örnek yeterli, recency-WR kullanılır, daha geniş aralık
            if len(fr)>=3 and rwr is not None:
                new_w[f]=round(max(0.3,min(2.0,0.35+rwr/50)),3)
                c.execute("UPDATE weights SET mult=?,win_rate=?,n=?,updated=? WHERE feature=?",
                          (new_w[f],rwr,len(fr),datetime.utcnow().isoformat(timespec="seconds"),f))
            else: new_w[f]=adap_weights.get(f,1.0)  # keep existing weight
        c.commit()

        # Best symbols (top 5 by win rate, min 2 trades)
        sym_perf={}
        for r in closed:
            s=r["sym"]
            sym_perf.setdefault(s,{"t":0,"w":0,"rr":[]})
            sym_perf[s]["t"]+=1
            if r["status"]=="TP": sym_perf[s]["w"]+=1
            if r["act_rr"]: sym_perf[s]["rr"].append(r["act_rr"])
        for s in sym_perf:
            d=sym_perf[s]
            d["wr"]=round(d["w"]/d["t"]*100,1) if d["t"] else 0
            d["avg_rr"]=round(sum(d["rr"])/len(d["rr"]),2) if d["rr"] else 0
        best_syms=sorted([(s,d) for s,d in sym_perf.items() if d["t"]>=2],
                         key=lambda x:-x[1]["wr"])[:5]

        recent=c.execute("SELECT sym,quality,direction,status,act_rr,rr_t,entry,created FROM signals ORDER BY id DESC LIMIT 20").fetchall()

        # Quant metrics
        trade_rets=[r["act_rr"]*0.01 for r in closed if r["act_rr"] is not None]
        equity=[ACCOUNT["balance"]]
        for rt in trade_rets: equity.append(equity[-1]*(1+rt))
        wins_rr=[r["act_rr"] for r in wins if r["act_rr"]]
        loss_rr=[abs(r["act_rr"]) for r in losses if r["act_rr"]]
        avg_win=round(sum(wins_rr)/len(wins_rr),2) if wins_rr else 0
        avg_loss=round(sum(loss_rr)/len(loss_rr),2) if loss_rr else 1
        sharpe=sharpe_ratio(trade_rets)
        sortino=sortino_ratio(trade_rets)
        mdd=max_drawdown_pct(equity)
        var95=historical_var(trade_rets)
        kelly=kelly_pct(wr,avg_win,avg_loss)
        calmar=round(avg_rr/mdd,2) if mdd>0 else None

        # Monte Carlo
        mc=None
        if total>=5:
            try: mc=monte_carlo(wr,avg_win,avg_loss,n_trades=max(total,163))
            except: pass

        # Session analysis
        sess_stats={}
        for r in closed:
            try: hr=int(str(r["created"])[11:13])
            except: hr=12
            if hr<8: sess="ASYA"
            elif hr<13: sess="LONDRA"
            elif hr<21: sess="NEW YORK"
            else: sess="ASYA"
            sess_stats.setdefault(sess,{"t":0,"w":0})
            sess_stats[sess]["t"]+=1
            if r["status"]=="TP": sess_stats[sess]["w"]+=1

        # Consecutive streak
        streak=0; streak_type=""
        for r in reversed(closed):
            if r["status"]=="TP":
                if streak_type in ("","TP"): streak+=1; streak_type="TP"
                else: break
            elif r["status"]=="SL":
                if streak_type in ("","SL"): streak+=1; streak_type="SL"
                else: break
            else: break

        with lock:
            stats_cache={
                "total":total,"wins":len(wins),"losses":len(losses),"expired":len(expired),
                "wr":wr,"avg_rr":avg_rr,"pf":pf,
                "tp":len(wins),"sl":len(losses),
                "by_q":bq,"fstats":fstats,
                "recent":[dict(r) for r in recent],
                "sharpe":sharpe,"sortino":sortino,"calmar":calmar,
                "mdd":mdd,"var95":var95,"kelly":kelly,
                "avg_win":avg_win,"avg_loss":avg_loss,
                "sess_stats":sess_stats,"sym_perf":sym_perf,"mc":mc,
                "best_syms":best_syms,"equity_curve":equity[-50:],
                "streak":streak,"streak_type":streak_type,
            }
        # Adaptive learning: hızlandırıldı — sadece 3 işlemden sonra öğrenmeye başlar
        if total>=3:
            adap_weights.update(new_w)

def stats_loop():
    while True:
        try: compute_stats()
        except: pass
        time.sleep(60)   # 1 dakikada bir istatistik + adaptif ağırlık güncelle

# ═══════════════════════════════════════════════════════════════
# ANALYSIS
# ═══════════════════════════════════════════════════════════════
def run_analysis():
    global setups, last_analysis
    if time.time()-last_analysis < ANALYSIS_SEC: return
    last_analysis=time.time()
    results=[]
    with lock:
        snap={k:v for k,v in market.items()}
        sn=dict(cat_news)
    for sym,md in snap.items():
        if not md.price: continue
        candles=list(md.candles) if md.candles else []
        if not candles and len(md.ticks)>=50:
            candles=ticks_to_candles(list(md.ticks))
        if len(candles)<30: continue
        try:
            r=score_setup(sym,candles,md.price,sn.get(sym,[]))
            if r:
                results.append(r)
                # Sadece GERÇEKTEN onaylı A+/A/B+ sinyaller DB'ye yazılır ve
                # izlenen işleme dönüşür — böylece "onaylı görünüp kaybolma" biter.
                if r.get("quality") in ("A+","A","B+") and r.get("status")=="APPROVED":
                    try: log_signal(r)
                    except: pass
                    try: tg_setup_alert(r)
                    except: pass
        except: pass
    results.sort(key=lambda x:({"A+":0,"A":1,"B+":2}.get(x["quality"],9),-x["score"]))
    setups=results
    try: update_watchlist(results)
    except: pass
    # ── Best-probability Telegram digest (top 3 picks per cycle) ─
    try: _tg_best_picks(results)
    except: pass

# ═══════════════════════════════════════════════════════════════
# DISPLAY HELPERS
# ═══════════════════════════════════════════════════════════════
def fp(v):
    if v is None: return "[dim]—[/dim]"
    a=abs(v)
    if a>10000: return f"{v:,.1f}"
    if a>100:   return f"{v:,.3f}"
    if a>1:     return f"{v:.5f}"
    return f"{v:.6f}"

def cpct(v):
    if v>0:  return f"[bright_green]+{v:.2f}%[/bright_green]"
    if v<0:  return f"[bright_red]{v:.2f}%[/bright_red]"
    return "[dim]0.00%[/dim]"

def qc(q):
    c={"A+":"bold bright_yellow","A":"bold green","B+":"bold cyan","WATCH":"bold dim yellow"}.get(q,"white")
    return f"[{c}]{q}[/{c}]"

def dc(d):
    return "[bright_green]▲ LONG[/bright_green]" if d=="LONG" else "[bright_red]▼ SHORT[/bright_red]"

def oc(s):
    c={"TP":"bold bright_green","SL":"bold bright_red",
       "OPEN":"bright_yellow","EXPIRED":"dim","TRIGGERED":"bright_cyan"}.get(s,"white")
    return f"[{c}]{s}[/{c}]"

def bias_c(b):
    if "EXTREME LONG"  in b: return f"[bold bright_green]{b}[/bold bright_green]"
    if "BULLISH"       in b: return f"[bright_green]{b}[/bright_green]"
    if "EXTREME SHORT" in b: return f"[bold bright_red]{b}[/bold bright_red]"
    if "BEARISH"       in b: return f"[bright_red]{b}[/bright_red]"
    return f"[dim]{b}[/dim]"

def pbar(pct,w=10):
    f=int(pct/100*w)
    bar="█"*f+"░"*(w-f)
    c="bright_green" if pct>=75 else "bright_red" if pct<=25 else "yellow"
    return f"[{c}]{bar}[/{c}]"

# ═══════════════════════════════════════════════════════════════
# PANELS
# ═══════════════════════════════════════════════════════════════
def panel_header():
    now=datetime.now(timezone.utc).strftime("%Y-%m-%d  %H:%M:%S UTC")
    ws_s="[bright_green]● WS LIVE[/bright_green]" if ws_ok else "[bright_red]● WS OFF[/bright_red]"
    n=len(setups)
    badge=f"[bright_yellow]{n} SETUP{'S' if n!=1 else ''}[/bright_yellow]" if n else "[dim]NO SETUPS[/dim]"
    return Panel(Align.center(
        f"[bold bright_yellow]TITAN FLOW[/bold bright_yellow]  [dim]│[/dim]  "
        f"[bold]INSTITUTIONAL INTELLIGENCE TERMINAL[/bold]  [dim]│[/dim]  "
        f"{ws_s}  [dim]│[/dim]  {badge}  [dim]│[/dim]  [dim]{now}[/dim]"),
        box=box.HEAVY,border_style="bright_yellow")

def panel_portfolio():
    with lock: ps=dict(portfolio_state)
    bal   =ps.get("shadow_balance",ACCOUNT["balance"])
    start =ACCOUNT["balance"]
    heat  =ps.get("heat",0)
    irs   =ps.get("inst_risk_score",100)
    eq    =ps.get("shadow_equity",[start])
    wins  =ps.get("shadow_wins",0)
    losses=ps.get("shadow_losses",0)
    total =wins+losses
    wr    =round(wins/total*100,1) if total else 0
    pnl   =round(bal-start,2); pnl_pct=round(pnl/start*100,1)
    mdd   =max_drawdown_pct(eq) if len(eq)>1 else 0
    daily =ps.get("daily_pnl",0)
    curr  =ps.get("currency_exp",{})
    macro =ps.get("macro_exp",{})
    clusters=ps.get("corr_clusters",[])

    # Heat colour
    hc=("bright_green" if heat<5 else "yellow" if heat<10
        else "bright_yellow" if heat<15 else "bright_red")
    heat_bar="█"*int(heat/2)+"░"*(50-int(heat/2)); heat_bar=heat_bar[:20]
    heat_label="GREEN" if heat<5 else "YELLOW" if heat<10 else "ORANGE" if heat<15 else "RED ⛔"

    # IRS colour
    ic="bright_green" if irs>=80 else "yellow" if irs>=60 else "bright_red"

    # Balance & P&L
    bc="bright_green" if pnl>=0 else "bright_red"
    dc="bright_green" if daily>=0 else "bright_red"

    lines=["[bold]━━━  HESAP DURUMU  ━━━[/bold]",""]
    lines.append(f"  Başlangıç           : [dim]£{start:.2f}[/dim]")
    lines.append(f"  Shadow Bakiye       : [bold bright_white]£{bal:.2f}[/bold bright_white]  [{bc}]{pnl:+.2f} ({pnl_pct:+.1f}%)[/{bc}]")
    lines.append(f"  Günlük P&L          : [{dc}]£{daily:+.2f}[/{dc}]")
    lines.append(f"  Max Drawdown        : [bright_red]{mdd:.1f}%[/bright_red]  [dim](daily limit {ACCOUNT['max_daily_dd']*100:.0f}% · weekly {ACCOUNT['max_weekly_dd']*100:.0f}%)[/dim]")
    lines.append(f"  Shadow Win Rate     : [{('bright_green' if wr>=55 else 'bright_red')}]{wr:.1f}%[/] [dim]({wins}W / {losses}L / {total} toplam)[/dim]")
    lines.append("")

    lines.append("[bold]━━━  PORTFÖy ISISI (HEAT)  ━━━[/bold]")
    lines.append("")
    lines.append(f"  [{hc}]{heat_bar}[/{hc}]  [{hc}]{heat:.1f}%  {heat_label}[/{hc}]")
    lines.append(f"  [dim]5%=Yeşil · 10%=Sarı · 15%=Turuncu · 15%+=KIRMIZI (engel)[/dim]")
    if heat>10: lines.append("  [bold yellow]⚠ Pozisyon boyutu %50 azaltıldı (heat >10%)[/bold yellow]")
    if heat>15: lines.append("  [bold bright_red]⛔ YENİ POZİSYON ENGELLENDİ (heat >15%)[/bold bright_red]")
    lines.append("")

    # Institutional Risk Score
    lines.append(f"[bold]━━━  KURUMSAL RİSK SKORU  ━━━[/bold]")
    lines.append("")
    irs_bar="█"*int(irs/5)+"░"*(20-int(irs/5))
    lines.append(f"  [{ic}]{irs_bar}[/{ic}]  [{ic}]{irs:.0f}/100[/{ic}]")
    lines.append(f"  [dim]Risk bütçesi : £{round(bal*ACCOUNT['max_risk_pct'],2):.2f} maks  |  Default 1%=£{round(bal*ACCOUNT['risk_pct'],2):.2f}[/dim]")
    lines.append("")

    # Currency exposure
    if curr:
        lines.append("[bold]━━━  PARA BİRİMİ MARUZIYETI  ━━━[/bold]")
        lines.append("")
        for ccy,exp in sorted(curr.items(),key=lambda x:-abs(x[1])):
            cc="bright_green" if exp>0 else "bright_red"
            bar_v=min(abs(exp)/max(abs(v) for v in curr.values()),1.0) if curr else 0
            bar="█"*int(bar_v*12)+"░"*(12-int(bar_v*12))
            lines.append(f"  [bold white]{ccy:<8}[/bold white] [{cc}]{bar}[/{cc}]  [{cc}]{exp:+.0f}[/{cc}]")
            if abs(exp)>15: lines.append(f"  [yellow]  ⚠ {ccy} maruziyeti yüksek[/yellow]")
        lines.append("")

    # Correlation clusters
    if clusters:
        lines.append("[bold]━━━  KORRElASYON KÜMELERİ  ━━━[/bold]")
        lines.append("")
        for cl in clusters:
            lines.append(f"  [bright_yellow]⚠ {cl['label']}:[/bright_yellow] {', '.join(cl['syms'])}")
            lines.append("  [dim]  → Tek risk birimi olarak hesapla, boyutu küçült[/dim]")
        lines.append("")

    # Macro exposure
    if macro:
        lines.append("[bold]━━━  MAKRO MARUZIYET  ━━━[/bold]")
        lines.append("")
        for basket,syms in macro.items():
            lines.append(f"  [bright_cyan]{basket:<18}[/bright_cyan] {', '.join(syms)}")
        lines.append("")

    # Active trades (live)
    with lock: at=dict(active_trades)
    if at:
        lines.append("[bold]━━━  AKTİF AÇIK POZİSYONLAR  ━━━[/bold]")
        lines.append("")
        lines.append(f"  [bold dim]{'SEMBOL':<10} {'YÖN':<6} {'GİRİŞ':>9} {'ŞU AN':>9} {'P&L':>8} {'R:R':>5} {'DURUM':<8}[/bold dim]")
        lines.append("  " + "─"*60)
        for k,t2 in at.items():
            sym2=t2.get("sym","?"); ep=t2.get("_trade_entry_price") or t2.get("price",0)
            sl2=t2.get("sl",0); tp2=t2.get("tp",0); dir2=t2.get("direction","")
            cur=market.get(sym2); cp=cur.price if cur else ep
            if ep and sl2:
                risk_pts=abs(ep-sl2)
                if dir2=="LONG": pnl_pts=cp-ep; rr_live=pnl_pts/risk_pts if risk_pts else 0
                else: pnl_pts=ep-cp; rr_live=pnl_pts/risk_pts if risk_pts else 0
            else: pnl_pts=0; rr_live=0
            lev=LEVERAGE.get(get_asset_class(sym2),5)
            pnl_pct2=pnl_pts/ep*100*lev if ep else 0
            pnl_gbp=round(bal*ACCOUNT["risk_pct"]*rr_live,2)
            pc="bright_green" if pnl_gbp>=0 else "bright_red"
            dir_s="[bright_green]LONG[/bright_green]" if dir2=="LONG" else "[bright_red]SHORT[/bright_red]"
            rr_s=f"[bright_green]{rr_live:+.2f}R[/bright_green]" if rr_live>=0 else f"[bright_red]{rr_live:+.2f}R[/bright_red]"
            lines.append(f"  [bold white]{sym2:<10}[/bold white] {dir_s}  "
                        f"[dim]{fp(ep):>9}[/dim]  [bright_white]{fp(cp):>9}[/bright_white]  "
                        f"[{pc}]£{pnl_gbp:+.2f}[/{pc}]  {rr_s}  [bright_yellow]OPEN[/bright_yellow]")
        lines.append("")

    # Recent closed trades from DB
    try:
        with db() as c:
            st_rows=c.execute("SELECT sym,direction,entry_price,exit_price,pnl,status,created FROM signals WHERE status!='OPEN' ORDER BY id DESC LIMIT 12").fetchall()
    except: st_rows=[]
    if st_rows:
        lines.append("[bold]━━━  SON KAPANAN POZİSYONLAR  ━━━[/bold]")
        lines.append("")
        lines.append(f"  [bold dim]{'SEMBOL':<10} {'YÖN':<6} {'GİRİŞ':>9} {'ÇIKIŞ':>9} {'SONUÇ':<10} {'TARİH':<16}[/bold dim]")
        lines.append("  " + "─"*60)
        for r in st_rows:
            st2=r["status"]
            if st2=="TP": em="🎯"; sc="bright_green"
            elif st2=="SL": em="❌"; sc="bright_red"
            elif st2=="EXPIRED": em="⏰"; sc="dim"
            else: em="🚫"; sc="yellow"
            ep2=r["entry_price"] or 0; xp=r["exit_price"] or 0
            dir_s="[bright_green]L[/bright_green]" if (r["direction"] or "")=="LONG" else "[bright_red]S[/bright_red]"
            dt=(r["created"] or "")[:16]
            lines.append(f"  [bold white]{r['sym']:<10}[/bold white] {dir_s}  "
                        f"[dim]{fp(ep2):>9}[/dim]  [dim]{fp(xp):>9}[/dim]  "
                        f"[{sc}]{em} {st2:<8}[/{sc}]  [dim]{dt}[/dim]")

    return Panel("\n".join(lines),
                 title="[bold bright_green]● EXECUTIVE DASHBOARD — Trade212 CFD Portfolio Engine[/bold bright_green]",
                 border_style="bright_green",box=box.HEAVY,
                 subtitle=f"[dim]Balance: £{bal:.2f}  |  Heat: {heat:.1f}%  |  IRS: {irs:.0f}/100[/dim]")

def panel_market():
    t=Table(title="[bold]● LIVE MARKET FEED[/bold]",box=box.SIMPLE_HEAVY,
            border_style="bright_blue",header_style="bold bright_blue",show_lines=True,
            width=238)
    t.add_column("SYMBOL", width=16,style="bold white")
    t.add_column("PRICE",  width=16,justify="right")
    t.add_column("CHG",    width=12,justify="right")
    t.add_column("HIGH",   width=16,justify="right",style="green")
    t.add_column("LOW",    width=16,justify="right",style="red")
    t.add_column("UPDATED",width=12,style="dim")
    with lock: snap=dict(market)
    for label,syms in DISPLAY_GROUPS:
        t.add_row(f"[dim bold]── {label} ──[/dim bold]","","","","","")
        for sym in syms:
            md=snap.get(sym)
            if not md: continue
            t.add_row(sym,
                f"[bright_white]{fp(md.price)}[/bright_white]",
                cpct(md.chg),
                fp(md.high) if md.high else "—",
                fp(md.low)  if md.low  else "—",
                md.updated or "—")
    return Panel(t,border_style="bright_blue",box=box.ROUNDED)

def panel_setups():
    ss=[s for s in list(setups) if s.get("status")=="APPROVED"]
    act=len(active_trades)
    wl=len(_wl_active)
    if not ss:
        return Panel(
            Align.center(
                f"[bold bright_yellow]TITAN PRIME ELITE — TARAMA DEVAM EDİYOR[/bold bright_yellow]\n"
                f"[dim]Aktif İşlem: {act}  ·  İzleme: {wl}  ·  Her 30sn güncellenir[/dim]"),
            title="[bold bright_yellow]● ONAYLANAN SETUPLАР[/bold bright_yellow]",
            border_style="bright_yellow",box=box.HEAVY)
    t=Table(
        title=f"[bold bright_yellow]● ONAYLANAN SETUPLАР  —  {len(ss)} fırsat  ·  Aktif: {act}  ·  İzleme: {wl}[/bold bright_yellow]",
        box=box.SIMPLE_HEAVY,border_style="bright_yellow",
        header_style="bold bright_yellow",show_lines=True,width=238)
    t.add_column("GR",    width=5,  justify="center")
    t.add_column("SEMBOL",width=11, style="bold white")
    t.add_column("YÖN",   width=9,  justify="center")
    t.add_column("SKOR",  width=8,  justify="center")
    t.add_column("R:R",   width=7,  justify="center")
    t.add_column("FİYAT", width=13, justify="right")
    t.add_column("GİRİŞ ZONU",width=24,justify="right")
    t.add_column("STOP",  width=13, justify="right",style="bright_red")
    t.add_column("HEDEF", width=13, justify="right",style="bright_green")
    t.add_column("REJİM", width=10, justify="center")
    t.add_column("KONTRARYAN",width=12,justify="center")
    t.add_column("HOLD",  width=7,  justify="center",style="dim")
    t.add_column("SAAT",  width=7)
    for s in ss[:14]:
        sc=s["score"]
        sk_c="bold bright_yellow" if sc>=82 else "bold green" if sc>=70 else "cyan"
        regime=s.get("regime","Nötr")
        rc="bright_green" if regime=="Risk-On" else "bright_red" if regime=="Risk-Off" else "yellow"
        c_score=s.get("contrarian_score",0)
        c_col="bright_yellow" if c_score>=70 else "yellow" if c_score>=40 else "dim green"
        hold=s.get("hold_h",0); hold_s=f"{hold:.0f}h" if hold else "—"
        in_wl="[dim cyan]◎[/dim cyan] " if _wl_key(s) in _wl_active else ""
        in_at="[bright_green]▶[/bright_green] " if _wl_key(s) in active_trades else ""
        t.add_row(
            qc(s["quality"]),
            f"{in_at}{in_wl}[bold white]{s['sym']}[/bold white]",
            dc(s["direction"]),
            f"[{sk_c}]{sc:.0f}[/{sk_c}]",
            f"[bold]1:{s['rr']}[/bold]",
            f"[bright_white]{fp(s['price'])}[/bright_white]",
            f"[dim]{fp(s['el'])} – {fp(s['eh'])}[/dim]",
            fp(s["sl"]),fp(s["tp"]),
            f"[{rc}]{regime}[/{rc}]",
            f"[{c_col}]{c_score}/100[/{c_col}]",
            hold_s, s["time"])
    return Panel(t,border_style="bright_yellow",box=box.HEAVY)

def panel_details():
    ss=[s for s in list(setups) if s.get("status")=="APPROVED"][:2]
    if not ss:
        return Panel(Align.center("[dim]Onaylanan setup bekleniyor...[/dim]"),
                     title="[bold bright_yellow]● TRADE PLANI[/bold bright_yellow]",
                     border_style="bright_yellow",box=box.ROUNDED)
    panels=[]
    for s in ss:
        sc=s["score"]; rr=s["rr"]; q=s["quality"]
        regime=s.get("regime","Nötr")
        rc2="bright_green" if regime=="Risk-On" else "bright_red" if regime=="Risk-Off" else "yellow"
        c_score=s.get("contrarian_score",0); c_label=s.get("contrarian_label","—")
        c_col="bright_yellow" if c_score>=70 else "yellow" if c_score>=40 else "bright_green"
        sz=s.get("sizing",{})
        nr=s.get("news_risk","NO RISK"); ni=s.get("news_imp",0)
        nr_c={"CRITICAL":"bold bright_red","HIGH":"bright_red","MEDIUM":"yellow","LOW":"dim","NO RISK":"bright_green"}.get(nr,"dim")
        hold=s.get("hold_h",0)
        in_active=_wl_key(s) in active_trades
        in_wl=_wl_key(s) in _wl_active
        status_line=""
        if in_active: status_line="\n  [bold bright_green]▶ AKTİF İŞLEMDE[/bold bright_green]"
        elif in_wl:   status_line="\n  [bold cyan]◎ İZLEME LİSTESİNDE[/bold cyan]"

        # Trade plan card
        plan=(
            f"{qc(q)}  {dc(s['direction'])}  [bold white]{s['sym']}[/bold white]  "
            f"[{rc2}]◆ {regime}[/{rc2}]{status_line}\n\n"
            f"  [bold dim]━━  TRADE PLANI  ━━[/bold dim]\n"
            f"  [bold]Giriş Zonu :[/bold] [bright_white]{fp(s['el'])} – {fp(s['eh'])}[/bright_white]\n"
            f"  [bold]Stop Loss  :[/bold] [bright_red]{fp(s['sl'])}[/bright_red]  "
            f"[dim](yapısal swing + ATR)[/dim]\n"
            f"  [bold]Take Profit:[/bold] [bold bright_green]{fp(s['tp'])}[/bold bright_green]  "
            f"[dim](direnç/destek öncesi)[/dim]\n"
            f"  [bold]Risk/Reward:[/bold] [bold]1:{rr}[/bold]\n"
            f"  [bold]Skor       :[/bold] [bold bright_yellow]{sc:.0f}/100[/bold bright_yellow]\n"
            f"  [bold]Hold       :[/bold] ~{hold:.1f} saat  [dim][{s.get('duration','Intraday')}][/dim]\n\n"
        )
        if sz:
            plan+=(
                f"  [bold dim]━━  POZİSYON BOYUTU (Trade212 CFD)  ━━[/bold dim]\n"
                f"  Marj: [bright_white]£{sz.get('margin',0):.2f}[/bright_white]  "
                f"[dim]{sz.get('leverage',1)}:1 → £{sz.get('notional',0):.2f}[/dim]\n"
                f"  Risk: [bright_red]£{sz.get('exp_loss',0):.2f}[/bright_red]  "
                f"Kâr: [bold bright_green]£{sz.get('exp_profit',0):.2f}[/bold bright_green]  "
                f"[dim]{sz.get('risk_pct',0):.2f}% sermaye[/dim]\n\n"
            )
        # Reasons (max 4)
        pos=s.get("reasons",[])[:4]
        neg_list=s.get("neg_factors",[])[:2]
        plan+="  [bold dim]━━  NEDEN GİR?  ━━[/bold dim]\n"
        for r in pos: plan+=f"  [bright_green]✓[/bright_green] {r}\n"
        if neg_list:
            plan+="\n  [bold dim]━━  RİSKLER  ━━[/bold dim]\n"
            for r in neg_list: plan+=f"  [bright_red]✗[/bright_red] {r}\n"
        # Consensus vs Smart Money view
        cv=s.get("consensus_view",""); smv=s.get("sm_view","")
        if cv:
            plan+="\n  [bold dim]━━  GÖRÜŞ KARŞILAŞTIRMASI  ━━[/bold dim]\n"
            plan+=f"  [dim]{cv}[/dim]\n"
            plan+=f"  [bright_cyan]{smv}[/bright_cyan]\n"
        # Smart money notes
        sm=s.get("sm_notes",[])[:2]; traps=s.get("trap_warnings",[])[:1]
        if sm:
            plan+="\n  [bold dim]━━  SMART MONEY  ━━[/bold dim]\n"
            for n in sm: plan+=f"  [bright_cyan]◈[/bright_cyan] {n}\n"
        if traps:
            plan+="\n"
            for t2 in traps: plan+=f"  [bright_red]{t2}[/bright_red]\n"
        # COT
        cot=s.get("cot",{})
        if cot:
            pr=cot.get("pct_rank",50)
            plan+=(f"\n  [bold dim]━━  COT  ━━[/bold dim]\n"
                   f"  {bias_c(cot.get('bias',''))}  {pbar(pr)} {pr:.0f}th pct\n")
        plan+=(f"\n  [{nr_c}]Haber: {nr}[/{nr_c}]  [dim]etki {ni}/100[/dim]  "
               f"[{c_col}]Kontraryan: {c_score}/100[/{c_col}]")
        panels.append(Panel(plan,
            title=f"[bold bright_yellow]{s['sym']} — {s['time']}[/bold bright_yellow]",
            border_style="bright_yellow",box=box.ROUNDED))
    if len(panels)==2:
        lo=Layout(); lo.split_row(Layout(panels[0]),Layout(panels[1]))
        return lo
    return panels[0]

def panel_cot():
    with lock: snap=dict(cot_cache)
    if not snap:
        return Panel(Align.center("[dim]COT verisi yükleniyor (CFTC API)...[/dim]"),
                     title="[bold bright_magenta]● COMMITMENT OF TRADERS — CFTC[/bold bright_magenta]",
                     border_style="bright_magenta",box=box.ROUNDED)
    t=Table(box=box.SIMPLE_HEAVY,border_style="bright_magenta",
            header_style="bold bright_magenta",show_lines=True,
            title="[bold bright_magenta]● COT — CFTC Haftalık Pozisyonlar[/bold bright_magenta]")
    t.add_column("SEMBOL",    width=14,style="bold white")
    t.add_column("TARİH",     width=13,style="dim")
    t.add_column("SPEC NET",  width=14,justify="right")
    t.add_column("HAFTALIK Δ",width=14,justify="right")
    t.add_column("COMM NET",  width=14,justify="right")
    t.add_column("OI",        width=13,justify="right",style="dim")
    t.add_column("12H RANK",  width=16,justify="center")
    t.add_column("BIAS",      width=16,justify="center")
    t.add_column("CONTRARIAN",width=13,justify="center")
    for sym,d in sorted(snap.items(),key=lambda x:abs(x[1].get("pct_rank",50)-50),reverse=True):
        sn=d.get("spec_net",0); sc=d.get("spec_chg",0); cn=d.get("comm_net",0)
        pr=d.get("pct_rank",50); ct=d.get("contrarian")
        t.add_row(sym,d.get("date","—"),
            f"[bright_green]{sn:+,}[/bright_green]" if sn>0 else f"[bright_red]{sn:+,}[/bright_red]",
            f"[bright_green]{sc:+,}[/bright_green]" if sc>0 else f"[bright_red]{sc:+,}[/bright_red]" if sc<0 else f"[dim]{sc:+,}[/dim]",
            f"[bright_green]{cn:+,}[/bright_green]" if cn>0 else f"[bright_red]{cn:+,}[/bright_red]",
            f"{d.get('oi',0):,}",
            f"{pbar(pr)} {pr:.0f}%",
            bias_c(d.get("bias","—")),
            f"[bold bright_yellow]⚠ {ct}[/bold bright_yellow]" if ct else "[dim]—[/dim]")
    return Panel(t,border_style="bright_magenta",box=box.ROUNDED)

def panel_journal():
    try:
        with db() as c:
            open_rows =c.execute(
                "SELECT id,sym,quality,direction,entry,sl,tp,rr_t,score,created FROM signals WHERE status='OPEN' ORDER BY id DESC LIMIT 15"
            ).fetchall()
            tp_rows   =c.execute(
                "SELECT id,sym,quality,direction,entry,out_price,act_rr,rr_t,created,out_at FROM signals WHERE status='TP' ORDER BY id DESC LIMIT 20"
            ).fetchall()
            sl_rows   =c.execute(
                "SELECT id,sym,quality,direction,entry,out_price,act_rr,rr_t,created,out_at FROM signals WHERE status='SL' ORDER BY id DESC LIMIT 20"
            ).fetchall()
    except: open_rows=[]; tp_rows=[]; sl_rows=[]

    with lock: at=dict(active_trades)
    lines=[]

    # ── helper: make a mini table string ────────────────────────
    def col(text, w, align="left", style=""):
        s=str(text)[:w]
        s=s.ljust(w) if align=="left" else s.rjust(w)
        return f"[{style}]{s}[/{style}]" if style else s

    # ═══════════════════════════════════════════════════
    # BÖLÜM 1 — AKTİF İŞLEMLER
    # ═══════════════════════════════════════════════════
    lines.append(f"[bold bright_green]━━━━━━━━━━━━━━━  ▶ AKTİF İŞLEMLER ({len(at)})  ━━━━━━━━━━━━━━━[/bold bright_green]")
    lines.append(f"[bold dim]  {'ID':<6} {'SEMBOL':<10} {'YÖN':<7} {'GİRİŞ':>10} {'ŞU AN':>10} {'CANLI R':>8} {'CANLI £':>8} {'SL':>10} {'TP':>10}[/bold dim]")
    lines.append("  " + "─"*88)
    if not at:
        lines.append("  [dim]Henüz aktif işlem yok[/dim]")
    else:
        for k,t2 in at.items():
            sym2=t2.get("sym","?"); ep=t2.get("_trade_entry_price") or t2.get("price",0)
            sl2=t2.get("sl",0); tp2=t2.get("tp",0); dir2=t2.get("direction","")
            cur_md=market.get(sym2); cp=cur_md.price if cur_md else ep
            risk=abs(ep-sl2) if sl2 and ep else 1
            pnl_pts=(cp-ep if dir2=="LONG" else ep-cp)
            rr_live=round(pnl_pts/risk,2) if risk else 0
            sz2=t2.get("sizing",{}); exp_loss=sz2.get("exp_loss",0)
            pnl_gbp=round(rr_live*exp_loss,2) if exp_loss else 0
            rc="bright_green" if rr_live>=0 else "bright_red"
            dc2="[bright_green]LONG[/bright_green]" if dir2=="LONG" else "[bright_red]SHORT[/bright_red]"
            sig_id=t2.get("db_id",0) or 0
            lines.append(
                f"  [dim]#{sig_id:<5}[/dim] [bold white]{sym2:<10}[/bold white] {dc2:<7} "
                f"[dim]{fp(ep):>10}[/dim] [bright_white]{fp(cp):>10}[/bright_white] "
                f"[{rc}]{rr_live:>+7.2f}R[/{rc}] [{rc}]{pnl_gbp:>+7.2f}£[/{rc}]  "
                f"[bright_red]{fp(sl2):>10}[/bright_red] [bright_green]{fp(tp2):>10}[/bright_green]")

    # Also show DB OPEN signals not yet in active_trades
    db_open=[r for r in open_rows if r["sym"] not in {t2["sym"] for t2 in at.values()}]
    if db_open:
        lines.append(f"\n  [dim]DB'deki açık sinyaller (henüz girilmemiş):[/dim]")
        lines.append(f"  [bold dim]  {'ID':<6} {'SEMBOL':<10} {'YÖN':<7} {'GİRİŞ':>10} {'SL':>10} {'TP':>10} {'SKOR':>6} {'TARİH':<16}[/bold dim]")
        for r in db_open[:8]:
            dt=str(r["created"] or "")[:16]
            dir_s=("[bright_green]LONG[/bright_green]" if r["direction"]=="LONG"
                   else "[bright_red]SHORT[/bright_red]")
            lines.append(
                f"  [dim]#{r['id']:<5}[/dim] [white]{r['sym']:<10}[/white] {dir_s:<7} "
                f"[dim]{fp(r['entry']):>10}[/dim]  "
                f"[bright_red]{fp(r['sl']):>10}[/bright_red] [bright_green]{fp(r['tp']):>10}[/bright_green] "
                f"[yellow]{r['score']:>6}[/yellow] [dim]{dt}[/dim]")

    lines.append("")

    # ═══════════════════════════════════════════════════
    # BÖLÜM 2 — TAKE PROFIT ✅
    # ═══════════════════════════════════════════════════
    lines.append(f"[bold bright_green]━━━━━━━━━━━━━━━  🎯 TAKE PROFIT ({len(tp_rows)})  ━━━━━━━━━━━━━━━[/bold bright_green]")
    lines.append(f"[bold dim]  {'ID':<6} {'SEMBOL':<10} {'YÖN':<7} {'GİRİŞ':>10} {'ÇIKIŞ':>10} {'GERÇEK R':>9} {'HEDEF R':>8} {'TARİH':<16}[/bold dim]")
    lines.append("  " + "─"*84)
    if not tp_rows:
        lines.append("  [dim]Henüz take profit yok[/dim]")
    else:
        for r in tp_rows:
            arr=r["act_rr"] or 0
            dt=str(r["out_at"] or r["created"] or "")[:16]
            dir_s=("[bright_green]LONG[/bright_green]" if r["direction"]=="LONG"
                   else "[bright_red]SHORT[/bright_red]")
            lines.append(
                f"  [dim]#{r['id']:<5}[/dim] [bold white]{r['sym']:<10}[/bold white] {dir_s:<7} "
                f"[dim]{fp(r['entry']):>10}[/dim] [bright_white]{fp(r['out_price'] or 0):>10}[/bright_white] "
                f"[bold bright_green]{arr:>+8.2f}R[/bold bright_green]  "
                f"[dim]{r['rr_t']:>6.2f}R[/dim]  [dim]{dt}[/dim]")

    lines.append("")

    # ═══════════════════════════════════════════════════
    # BÖLÜM 3 — STOP LOSS ❌
    # ═══════════════════════════════════════════════════
    lines.append(f"[bold bright_red]━━━━━━━━━━━━━━━  🛑 STOP LOSS ({len(sl_rows)})  ━━━━━━━━━━━━━━━[/bold bright_red]")
    lines.append(f"[bold dim]  {'ID':<6} {'SEMBOL':<10} {'YÖN':<7} {'GİRİŞ':>10} {'ÇIKIŞ':>10} {'GERÇEK R':>9} {'HEDEF R':>8} {'TARİH':<16}[/bold dim]")
    lines.append("  " + "─"*84)
    if not sl_rows:
        lines.append("  [dim]Henüz stop loss yok[/dim]")
    else:
        for r in sl_rows:
            arr=r["act_rr"] or 0
            dt=str(r["out_at"] or r["created"] or "")[:16]
            dir_s=("[bright_green]LONG[/bright_green]" if r["direction"]=="LONG"
                   else "[bright_red]SHORT[/bright_red]")
            lines.append(
                f"  [dim]#{r['id']:<5}[/dim] [bold white]{r['sym']:<10}[/bold white] {dir_s:<7} "
                f"[dim]{fp(r['entry']):>10}[/dim] [bright_white]{fp(r['out_price'] or 0):>10}[/bright_white] "
                f"[bold bright_red]{arr:>+8.2f}R[/bold bright_red]  "
                f"[dim]{r['rr_t']:>6.2f}R[/dim]  [dim]{dt}[/dim]")

    # ── summary footer ──
    total_closed=len(tp_rows)+len(sl_rows)
    wr=round(len(tp_rows)/total_closed*100,1) if total_closed else 0
    wc="bright_green" if wr>=55 else ("yellow" if wr>=45 else "bright_red")
    lines.append("")
    lines.append(
        f"[bold dim]  Toplam kapanmış: {total_closed}  |  "
        f"Win Rate: [{wc}]{wr:.1f}%[/{wc}]  |  "
        f"TP: [bright_green]{len(tp_rows)}[/bright_green]  SL: [bright_red]{len(sl_rows)}[/bright_red]  "
        f"Aktif: [bright_yellow]{len(at)+len(open_rows)}[/bright_yellow][/bold dim]")

    return Panel("\n".join(lines),
                 title="[bold bright_cyan]● TRADE JOURNAL  —  Aktif / TP / SL[/bold bright_cyan]",
                 border_style="bright_cyan",box=box.HEAVY)

def panel_stats():
    with lock: st=dict(stats_cache)
    if not st:
        return Panel(Align.center(
            "[dim]İstatistik bekleniyor — ilk sinyaller kapanınca görünür.\n"
            f"DB: {DB_PATH}[/dim]"),
            title="[bold bright_cyan]● PERFORMANS & ADAPTİF ÖĞRENME[/bold bright_cyan]",
            border_style="bright_cyan",box=box.ROUNDED)

    total=st.get("total",0); wr=st.get("wr",0); pf=st.get("pf",0); avg=st.get("avg_rr",0)
    tp_n=st.get("tp",0); sl_n=st.get("sl",0); exp_n=st.get("expired",0)
    avg_win=st.get("avg_win",0); avg_loss=st.get("avg_loss",1)
    streak=st.get("streak",0); streak_type=st.get("streak_type","")
    wc="bright_green" if wr>=55 else "bright_red" if wr<45 else "yellow"
    adap=len(adap_weights)>0
    lines=[]

    # ── KPI header ──
    lines.append(
        f"[bold]KPI[/bold]  {total} kapanmış  [{wc}][bold]{wr:.1f}% WR[/bold][/{wc}]  "
        f"PF [bold]{pf:.2f}[/bold]  Avg R:R [bold]{avg:+.2f}[/bold]  "
        f"Avg Kazanç [bright_green]+{avg_win:.2f}R[/bright_green]  "
        f"Avg Kayıp [bright_red]-{avg_loss:.2f}R[/bright_red]")
    streak_col="bright_green" if streak_type=="TP" else "bright_red" if streak_type=="SL" else "dim"
    streak_lbl="🔥 Kazanma serisi" if streak_type=="TP" else "❄️ Kayıp serisi" if streak_type=="SL" else ""
    lines.append(
        f"🎯 TP:[bright_green]{tp_n}[/bright_green]  "
        f"🛑 SL:[bright_red]{sl_n}[/bright_red]  "
        f"⏰ Süresi Dolan:{exp_n}  |  "
        f"Adaptif: " + ("[bright_green]AKTİF ✓[/bright_green]" if adap else "[dim]bekleniyor[/dim]") +
        (f"  [{streak_col}]{streak_lbl}: {streak}[/{streak_col}]" if streak>=2 else ""))

    # ── Quality breakdown ──
    lines.append("")
    bq=st.get("by_q",{})
    row="[bold dim]KALİTE:[/bold dim]  "
    for q in ("A+","A","B+"):
        d=bq.get(q,{"t":0,"w":0,"wr":0})
        if d["t"]:
            wc2="bright_green" if d["wr"]>=60 else "bright_red" if d["wr"]<40 else "yellow"
            row+=f"{qc(q)} [{wc2}]{d['wr']:.0f}%[/{wc2}] ({d['w']}/{d['t']})   "
    lines.append(row)

    # ── Best symbols ──
    best_syms=st.get("best_syms",[])
    if best_syms:
        lines.append("")
        lines.append("[bold dim]EN BAŞARILI SEMBOLLERnEN İYİ 5 (son 90 gün):[/bold dim]")
        row2=""
        for sym,d in best_syms:
            wc3="bright_green" if d["wr"]>=60 else "yellow"
            row2+=f"  [bold white]{sym}[/bold white] [{wc3}]{d['wr']:.0f}%[/{wc3}] {d['w']}/{d['t']} [dim]avg {d['avg_rr']:+.2f}R[/dim]"
        lines.append(row2)

    # ── Feature weights ──
    lines.append("")
    lines.append("[bold dim]ADAPTİF ÖĞRENME — Özellik Win-Rate → Ağırlık Çarpanı:[/bold dim]")
    feat_names={"f_ema":"EMA Stack","f_rsi":"RSI Extreme","f_macd":"MACD Cross",
                "f_sweep":"Liq Sweep","f_ob":"Order Block","f_fvg":"FVG",
                "f_struct":"Structure","f_cot":"COT Signal","f_news":"News Kataliz"}
    fst=st.get("fstats",{})
    sorted_f=sorted(fst.items(),key=lambda x:(x[1].get("wr") or 0) if x[1]["n"]>=3 else -1,reverse=True)
    row3=""
    for col,fd in sorted_f:
        n=fd["n"]; fw=fd.get("wr"); wt=adap_weights.get(col,1.0)
        if n<2: continue
        wc3="bright_green" if (fw or 0)>=60 else "bright_red" if (fw or 0)<40 else "yellow"
        wts=(f"[bright_green]↑{wt:.2f}x[/bright_green]" if wt>1.05
             else f"[bright_red]↓{wt:.2f}x[/bright_red]" if wt<0.95 else f"[dim]{wt:.2f}x[/dim]")
        row3+=f"  {feat_names.get(col,col)}: [{wc3}]{fw:.0f}%[/{wc3}] {wts}"
    lines.append(row3 or "  [dim]Henüz yeterli veri yok[/dim]")

    # ── Active trades ──
    _now2=datetime.now()
    lines.append("")
    lines.append(f"[bold bright_green]━━━  AKTİF İŞLEMLER ({len(active_trades)})  ━━━[/bold bright_green]")
    if not active_trades:
        lines.append("  [dim]Açık işlem yok. Sayfa 9'dan izleme listesini takip edin.[/dim]")
    else:
        for _k,_t in active_trades.items():
            _ep=_t.get("_trade_entry_price",_t.get("price",0))
            _cur_md=market.get(_t["sym"]); _cur=_cur_md.price if _cur_md else _ep
            _sl=_t["sl"]; _risk=abs(_ep-_sl); _risk=_risk if _risk>0 else 1
            if _t["direction"]=="LONG": _live_rr=round((_cur-_ep)/_risk,2)
            else: _live_rr=round((_ep-_cur)/_risk,2)
            _pnl_col="bright_green" if _live_rr>=0 else "bright_red"
            _age_h=(_now2-_t.get("_trade_entered",_now2)).total_seconds()/3600
            _sz=_t.get("sizing",{})
            _pnl_gbp=round(_live_rr*_sz.get("exp_loss",0),2) if _sz else 0
            lines.append(
                f"  [bright_green]▶[/bright_green] {qc(_t.get('quality','?'))} {dc(_t['direction'])} "
                f"[bold white]{_t['sym']:<10}[/bold white]  "
                f"Giriş:[dim]{fp(_ep)}[/dim] → [bright_white]{fp(_cur)}[/bright_white]  "
                f"SL:[bright_red]{fp(_sl)}[/bright_red]  TP:[bright_green]{fp(_t.get('tp',0))}[/bright_green]  "
                f"[{_pnl_col}]{_live_rr:+.2f}R[/{_pnl_col}]"
                +(f" [dim](£{_pnl_gbp:+.2f})[/dim]" if _pnl_gbp else "")
                +f"  [dim]{_age_h:.1f}sa[/dim]")

    # ── Closed trades ──
    lines.append("")
    lines.append("[bold dim]━━━  SON KAPANMIŞ POZİSYONLAR (son 20)  ━━━[/bold dim]")
    try:
        with db() as c:
            rows=c.execute("""SELECT sym,quality,direction,entry,sl,tp,
                               out_price,act_rr,rr_t,status,created
                               FROM signals WHERE status!='OPEN'
                               ORDER BY id DESC LIMIT 20""").fetchall()
    except: rows=[]
    if not rows:
        lines.append("  [dim]Henüz kapanmış pozisyon yok.[/dim]")
    else:
        lines.append(
            f"  [bold dim]{'SAAT':<6} {'SEMBOL':<10} {'YÖN':<6} "
            f"{'GİRİŞ':>11} {'ÇIKIŞ':>11} "
            f"{'DURUM':<8} {'GERÇEK R:R':>10} {'HEDEF':>8}[/bold dim]")
        lines.append("  [dim]" + "─"*82 + "[/dim]")
        for r in rows:
            ts=str(r["created"])[11:16] if r["created"] else "—"
            st2=r["status"]
            sc={"TP":"bold bright_green","SL":"bold bright_red","EXPIRED":"dim"}.get(st2,"white")
            arr=r["act_rr"]
            arr_s=(f"[bright_green]+{arr:.2f}R[/bright_green]" if arr and arr>0
                   else f"[bright_red]{arr:.2f}R[/bright_red]" if arr and arr<0 else "[dim]—[/dim]")
            st_emoji={"TP":"🎯","SL":"🛑","EXPIRED":"⏰"}.get(st2,"?")
            lines.append(
                f"  [dim]{ts:<6}[/dim] [bold white]{r['sym']:<10}[/bold white] "
                f"{'▲' if r['direction']=='LONG' else '▼'} "
                f"[dim]{fp(r['entry']):>11}[/dim] "
                f"[dim]{fp(r['out_price']):>11}[/dim]  "
                f"{st_emoji}[{sc}]{st2:<6}[/{sc}]  "
                f"{arr_s:>10}  [dim]1:{r['rr_t']:.1f}[/dim]")

    return Panel("\n".join(lines),
                 title="[bold bright_cyan]● PERFORMANS & ADAPTİF ÖĞRENME[/bold bright_cyan]",
                 border_style="bright_cyan",box=box.ROUNDED,
                 subtitle=f"[dim]{DB_PATH}  ·  Son güncelleme: {datetime.now().strftime('%H:%M:%S')}[/dim]")

def panel_news():
    with lock: arts=list(analyzed_news) or list(news_cache)
    if not arts:
        return Panel(Align.center("[dim]Haberler yükleniyor...[/dim]"),
                     title="[bold bright_blue]● KURUMSAL MAKRO İSTİHBARAT[/bold bright_blue]",
                     border_style="bright_blue",box=box.ROUNDED)
    lines=[]
    DIV="━"*110
    for x in arts[:6]:
        imp   =x.get("importance",0)
        rl    =x.get("risk_level","NOISE")
        ts    =x.get("datetime",0)
        age_m =(time.time()-ts)/60 if ts else 0
        age_s =f"{age_m:.0f}dk" if age_m<60 else f"{age_m/60:.1f}sa"
        m     =x.get("macro",{})

        rc={"CRITICAL":"bold bright_red","HIGH":"bright_red",
            "MEDIUM":"yellow","LOW":"dim green","NOISE":"dim"}.get(rl,"dim")
        bar_w=14; filled=int(imp/100*bar_w)
        bar="█"*filled+"░"*(bar_w-filled)
        bc="bright_red" if imp>=80 else "yellow" if imp>=60 else "green" if imp>=40 else "dim"

        # ── Header ──
        lines.append(f"[{rc}]{DIV}[/{rc}]")
        lines.append(
            f"  [{bc}]{bar}[/{bc}] [bold {rc}]{imp:>3}/100[/bold {rc}]  "
            f"[bold white]{x.get('headline','')[:95]}[/bold white]")
        lines.append(
            f"  [dim]{x.get('source','')[:16]}  ·  {age_s}[/dim]  [{rc}]{rl}[/{rc}]")

        summ=x.get("summary","").replace("\n"," ").strip()
        if summ:
            lines.append(f"  [dim italic]{summ[:140]}{'...' if len(summ)>140 else ''}[/dim italic]")

        if not m:
            lines.append("")
            continue

        bias_tr=m.get("bias_tr","Nötr")
        bull_pct=m.get("bull_pct",0); bear_pct=m.get("bear_pct",0); neut_pct=m.get("neut_pct",0)
        caution=m.get("caution","Normal İşlem"); conf=m.get("conf",50); dur=m.get("dur","1h")
        risk_label=m.get("risk_label",""); permanent=m.get("permanent",False)
        tf15m=m.get("tf15m","—"); tf1h=m.get("tf1h","—")
        tf4h=m.get("tf4h","—"); tf24h=m.get("tf24h","—")
        ad=m.get("asset_dirs",{})
        hist_key=m.get("hist_key"); hist_match=m.get("hist_match",{})

        # ── Türkçe Analiz ──
        bc2="bright_green" if bias_tr=="Yükseliş" else "bright_red" if bias_tr=="Düşüş" else "yellow"
        lines.append(
            f"\n  [bold]Yön Önyargısı:[/bold] [{bc2}]{bias_tr}[/{bc2}]  "
            f"[dim]|[/dim]  "
            f"[bright_green]Yükseliş %{bull_pct}[/bright_green]  "
            f"[bright_red]Düşüş %{bear_pct}[/bright_red]  "
            f"[yellow]Nötr %{neut_pct}[/yellow]  "
            f"[dim]|[/dim]  Güven: [bold]{conf}/100[/bold]")

        lines.append(
            f"  [bold]Zaman Dilimi Etkisi:[/bold]  "
            f"[dim]15m:[/dim] {tf15m}  "
            f"[dim]1h:[/dim] {tf1h}  "
            f"[dim]4h:[/dim] {tf4h}  "
            f"[dim]24h:[/dim] {tf24h}  "
            f"[dim]|[/dim]  Kalıcı: {'[bright_red]EVET[/bright_red]' if permanent else '[dim]Hayır[/dim]'}")

        # ── Per-asset directions ──
        if ad:
            bull_assets=[f"[bright_green]{s}↑{v:.1f}%[/bright_green]" for s,(d,v) in ad.items() if d=="↑"]
            bear_assets=[f"[bright_red]{s}↓{v:.1f}%[/bright_red]"   for s,(d,v) in ad.items() if d=="↓"]
            neutral_assets=[f"[dim]{s}→[/dim]" for s,(d,v) in ad.items() if d=="→"]
            if bull_assets:  lines.append("  [bold]Yükseliş Eğilimi:[/bold]  "+"  ".join(bull_assets))
            if bear_assets:  lines.append("  [bold]Düşüş Eğilimi:[/bold]    "+"  ".join(bear_assets))

        # ── Historical comparison ──
        if hist_key and hist_match:
            lines.append(f"\n  [bold dim]📜 Geçmiş Benzer Olaylar ({hist_key.upper()}):[/bold dim]")
            hist_parts=[]
            for sym,mv in list(hist_match.items())[:5]:
                hc="bright_green" if mv>0 else "bright_red"
                hist_parts.append(f"[{hc}]{sym} {'+' if mv>0 else ''}{mv:.1f}%[/{hc}]")
            lines.append("  "+"  ".join(hist_parts))

        # ── Market Expectation conclusion ──
        lines.append(f"\n  [bold]━━  MARKET BEKLENTİSİ  ━━[/bold]")
        lines.append(
            f"  Yön: [{bc2}]{bias_tr}[/{bc2}]  |  "
            f"Güven: [bold]{conf}/100[/bold]  |  "
            f"Süre: [bold]{dur}[/bold]  |  "
            f"{risk_label}")
        lines.append(f"  [bold bright_yellow]► {caution}[/bold bright_yellow]")
        lines.append("")

    return Panel("\n".join(lines).rstrip(),
                 title="[bold bright_blue]● KURUMSAL MAKRO İSTİHBARAT — Gelişmiş Haber Analizi[/bold bright_blue]",
                 border_style="bright_blue",box=box.ROUNDED,
                 subtitle="[dim]Impact 80+ → Telegram · Yönsel Önyargı · Geçmiş Karşılaştırma · Türkçe Analiz[/dim]")

def panel_quant():
    with lock: st=dict(stats_cache)
    if not st:
        return Panel(Align.center("[dim]Quant analiz için en az 5 kapanmış sinyal gerekiyor.[/dim]"),
                     title="[bold bright_magenta]● QUANT ANALİTİK — RİSK METRİKLERİ[/bold bright_magenta]",
                     border_style="bright_magenta",box=box.ROUNDED)

    sharpe=st.get("sharpe"); sortino=st.get("sortino"); calmar=st.get("calmar")
    mdd=st.get("mdd",0); var95=st.get("var95"); kelly=st.get("kelly",0)
    avg_win=st.get("avg_win",0); avg_loss=st.get("avg_loss",0)
    total=st.get("total",0); wr=st.get("wr",0)

    def fc(v,good,bad):
        if v is None: return "[dim]—[/dim]"
        c="bright_green" if v>=good else "bright_red" if v<=bad else "yellow"
        return f"[{c}]{v}[/{c}]"

    # ── Equity curve (ASCII sparkline) ──
    equity_curve=st.get("equity_curve",[])
    lines=[]
    if len(equity_curve)>=3:
        mn=min(equity_curve); mx=max(equity_curve); rng=mx-mn or 1
        h=4; w=min(len(equity_curve),60)
        step=max(1,len(equity_curve)//w)
        sampled=equity_curve[::step][-w:]
        bars=["▁","▂","▃","▄","▅","▆","▇","█"]
        spark=""
        for v in sampled:
            idx=int((v-mn)/rng*(len(bars)-1))
            c2="bright_green" if v>=equity_curve[0] else "bright_red"
            spark+=f"[{c2}]{bars[idx]}[/{c2}]"
        cur_bal=equity_curve[-1]; start_bal=equity_curve[0]
        pct_chg=(cur_bal-start_bal)/start_bal*100 if start_bal else 0
        pct_col="bright_green" if pct_chg>=0 else "bright_red"
        lines.append(f"[bold]━━━  EQUİTY EĞRİSİ  ━━━[/bold]  "
                     f"[{pct_col}]{pct_chg:+.2f}%[/{pct_col}]  "
                     f"[dim]£{start_bal:.2f} → [/dim][bright_white]£{cur_bal:.2f}[/bright_white]")
        lines.append(f"  {spark}")
        lines.append("")

    lines.append("[bold]━━━  TEMEL ORANLAR  ━━━[/bold]")
    lines.append("")
    lines.append(f"  Sharpe Oranı    : {fc(sharpe,1.5,0.5)}  [dim](>1.5 mükemmel · >1.0 iyi)[/dim]")
    lines.append(f"  Sortino Oranı   : {fc(sortino,2.0,0.8)}  [dim](aşağı risk odaklı Sharpe)[/dim]")
    lines.append(f"  Calmar Oranı    : {fc(calmar,1.0,0.3)}  [dim](return/max drawdown)[/dim]")
    lines.append(f"  Max Drawdown    : [{'bright_red' if mdd>20 else 'yellow' if mdd>10 else 'bright_green'}]{mdd:.1f}%[/{'bright_red' if mdd>20 else 'yellow' if mdd>10 else 'bright_green'}]  [dim](öz sermaye düşüşü)[/dim]")
    if var95 is not None:
        var_pct=abs(var95)*100
        vc="bright_red" if var_pct>3 else "yellow" if var_pct>1.5 else "bright_green"
        lines.append(f"  VaR %95 (1 trade): [{vc}]%{var_pct:.2f}[/{vc}]  [dim](sermayenin %1 risk ile)[/dim]")
    lines.append("")
    lines.append("[bold]━━━  KELLY CRITERION  ━━━[/bold]")
    lines.append("")
    kc="bright_green" if 5<=kelly<=15 else "yellow" if kelly<=25 else "bright_red"
    lines.append(f"  Önerilen pozisyon boyutu : [{kc}]%{kelly:.1f}[/{kc}]  [dim](1/2 Kelly = %{kelly/2:.1f} tavsiye edilir)[/dim]")
    lines.append(f"  Ort kazanç R:R  : [bright_green]+{avg_win:.2f}R[/bright_green]")
    lines.append(f"  Ort kayıp R:R   : [bright_red]-{avg_loss:.2f}R[/bright_red]")
    lines.append(f"  Win Rate        : {wr:.1f}%  |  {total} trade")
    lines.append("")

    # Monte Carlo
    mc=st.get("mc")
    lines.append("[bold]━━━  MONTE CARLO SİMÜLASYONU  ━━━[/bold]")
    lines.append("")
    if not mc:
        lines.append("  [dim]Simülasyon için en az 5 kapanmış trade gerekiyor.[/dim]")
    else:
        ns=mc["n_sims"]; nt=mc["n_trades"]
        med=mc["median"]; best=mc["best"]; worst=mc["worst"]; pr=mc["p_ruin"]
        lines.append(f"  [dim]{ns:,} simülasyon · {nt} trade · %1 risk/trade[/dim]")
        lines.append("")

        def eq_bar(v, width=20):
            pct=min(max((v-0.5)/1.5,0),1)
            filled=int(pct*width)
            c="bright_green" if v>=1.1 else "bright_red" if v<1.0 else "yellow"
            bar="█"*filled+"░"*(width-filled)
            return f"[{c}]{bar}[/{c}]"

        med_pct=(med-1)*100; best_pct=(best-1)*100; worst_pct=(worst-1)*100
        mc_="bright_green" if med>=1.0 else "bright_red"
        bc_="bright_green" if best>=1.0 else "yellow"
        wc_="bright_red" if worst<1.0 else "yellow"

        lines.append(f"  Medyan sonuç   : {eq_bar(med)}  [{mc_}]{med:.4f}x[/{mc_}]  [{mc_}]{med_pct:+.1f}%[/{mc_}]")
        lines.append(f"  En iyi durum   : {eq_bar(best)}  [{bc_}]{best:.4f}x[/{bc_}]  [{bc_}]{best_pct:+.1f}%[/{bc_}]  [dim](üst %5)[/dim]")
        lines.append(f"  En kötü durum  : {eq_bar(worst)}  [{wc_}]{worst:.4f}x[/{wc_}]  [{wc_}]{worst_pct:+.1f}%[/{wc_}]  [dim](alt %5)[/dim]")
        lines.append("")
        pc_="bold bright_red" if pr>=20 else "bright_red" if pr>=10 else "yellow" if pr>=5 else "bright_green"
        lines.append(f"  Çöküş ihtimali : [{pc_}]%{pr:.1f}[/{pc_}]  [dim](öz sermaye %50 altına düşme)[/dim]")
        if pr>=20:
            lines.append("  [bold bright_red]⚠  YÜKSEKRİSK — pozisyon boyutunu küçült![/bold bright_red]")
        elif pr<=5:
            lines.append("  [bright_green]✓  Çöküş riski düşük — strateji tutarlı.[/bright_green]")
    lines.append("")

    # Seans analizi
    sess=st.get("sess_stats",{})
    if sess:
        lines.append("[bold]━━━  SEANS PERFORMANSI  ━━━[/bold]")
        lines.append("")
        for s,d in sorted(sess.items(),key=lambda x:-(x[1]["w"]/x[1]["t"] if x[1]["t"] else 0)):
            t=d["t"]; w=d["w"]
            swr=round(w/t*100,0) if t else 0
            sc="bright_green" if swr>=60 else "bright_red" if swr<40 else "yellow"
            bar="█"*int(swr/10)+"░"*(10-int(swr/10))
            lines.append(f"  [bold]{s:<10}[/bold]  [{sc}]{bar}[/{sc}] [{sc}]{swr:.0f}%[/{sc}]  [dim]{w}/{t} trade[/dim]")
        lines.append("")

    # Volatilite rejimleri
    lines.append("[bold]━━━  VOLATİLİTE REJİMLERİ  ━━━[/bold]")
    lines.append("")
    with lock: snap=dict(market)
    vols=[]
    for name,md in snap.items():
        if md.candles and len(md.candles)>=25:
            cl=[c[4] for c in md.candles[-25:]]
            v=rolling_vol(cl,20)
            if v: vols.append((name,v,vol_regime(cl)))
    vols.sort(key=lambda x:-x[1])
    for name,v,regime in vols[:12]:
        rc={"YÜKSEK":"bright_red","ORTA":"yellow","DÜŞÜK":"bright_green"}.get(regime,"dim")
        lines.append(f"  [dim]{name:<10}[/dim]  [{rc}]{regime:<7}[/{rc}]  [dim]Ann.vol %{v:.1f}[/dim]")
    lines.append("")

    # Sembol win rate tablosu
    sym_perf=st.get("sym_perf",{})
    if sym_perf:
        lines.append("[bold]━━━  SEMBOL BAZLI PERFORMANS  ━━━[/bold]")
        lines.append("")
        lines.append(f"  [bold dim]{'SEMBOL':<12} {'W/T':>6} {'WIN%':>6} {'AVG R:R':>8}[/bold dim]")
        lines.append("  " + "─"*36)
        for sym,d in sorted(sym_perf.items(),key=lambda x:-x[1].get("wr",0)):
            t=d["t"]; w=d["w"]
            if t<2: continue
            swr=d.get("wr",round(w/t*100,1) if t else 0)
            avg_r=d.get("avg_rr",round(sum(d["rr"])/len(d["rr"]),2) if d["rr"] else 0)
            sc="bright_green" if swr>=60 else "bright_red" if swr<40 else "yellow"
            rc="bright_green" if avg_r>0 else "bright_red"
            lines.append(f"  [bold white]{sym:<12}[/bold white] [{sc}]{w}/{t}[/{sc}] [{sc}]{swr:>5.1f}%[/{sc}] [{rc}]{avg_r:>+7.2f}R[/{rc}]")

    return Panel("\n".join(lines),
                 title="[bold bright_magenta]● QUANT ANALİTİK — RİSK METRİKLERİ[/bold bright_magenta]",
                 border_style="bright_magenta",box=box.ROUNDED,
                 subtitle="[dim]Sharpe · Sortino · Calmar · VaR · Kelly · Seans · Sembol[/dim]")

def panel_risk():
    with lock:
        n_live=sum(1 for md in market.values() if md.price)
        n_setup=len(setups)
    try:
        with db() as c: n_open=c.execute("SELECT COUNT(*) FROM signals WHERE status='OPEN'").fetchone()[0]
    except: n_open=0
    return Panel(
        f"[bold bright_red]RİSK PROTOKOLÜ[/bold bright_red]\n\n"
        f"[dim]Canlı sembol  :[/dim] [bold]{n_live}[/bold]\n"
        f"[dim]Aktif setup   :[/dim] [bold bright_yellow]{n_setup}[/bold bright_yellow]\n"
        f"[dim]Açık sinyal   :[/dim] [bold]{n_open}[/bold]\n\n"
        f"[dim]Max risk/trade:[/dim] [bold]%1-2[/bold]\n"
        f"[dim]Min R:R       :[/dim] [bold]1:2.5[/bold]\n"
        f"[dim]Filtre        :[/dim] [bold]A+ · A · B+[/bold]\n"
        f"[dim]Analiz periyot:[/dim] [bold]30 sn[/bold]\n\n"
        f"[dim italic]Stop = final.\nAveraging yasak.\nSermaye önce gelir.[/dim italic]",
        border_style="bright_red",box=box.ROUNDED,title="[bold bright_red]RİSK[/bold bright_red]")

# ═══════════════════════════════════════════════════════════════
# PAGE SYSTEM
# ═══════════════════════════════════════════════════════════════
current_page = 1
_last_keypress = time.time()
AUTO_SCROLL_SEC = 5   # sayfa geçiş süresi (boşta)
AUTO_SCROLL_IDLE= 30  # kaç saniye sonra oto-scroll başlar

PAGE_NAMES = {1:"PİYASA",2:"SETUPLAR",3:"DETAY",4:"COT",5:"JOURNAL",6:"HABERLER",7:"QUANT",8:"PORTFÖY",9:"İZLEME"}

def nav_bar():
    parts=[]
    for k,v in PAGE_NAMES.items():
        if k==current_page:
            parts.append(f"[bold bright_yellow on grey23] {k}:{v} [/bold bright_yellow on grey23]")
        else:
            parts.append(f"[dim] {k}:{v} [/dim]")
    return "  ".join(parts)+"   [dim]( tuş 1-6 ile geç · Ctrl+C çıkış )[/dim]"

def key_listener():
    global current_page, _last_keypress
    try:
        import msvcrt
        while True:
            if msvcrt.kbhit():
                ch=msvcrt.getch()
                _last_keypress=time.time()
                if ch in (b'1',b'2',b'3',b'4',b'5',b'6',b'7',b'8',b'9'):
                    current_page=int(ch.decode())
            time.sleep(0.05)
    except: pass

def auto_scroll_loop():
    global current_page
    total=len(PAGE_NAMES)
    while True:
        time.sleep(AUTO_SCROLL_SEC)
        if time.time()-_last_keypress >= AUTO_SCROLL_IDLE:
            current_page=(current_page % total)+1

def panel_watchlist():
    lines=[]
    DIV="─"*106
    now=datetime.now()

    def age_str(dt):
        if not dt: return "—"
        h=(now-dt).total_seconds()/3600
        return f"{h:.1f}sa" if h<24 else f"{h/24:.1f}g"

    def trade_row(s, badge, badge_col, extra=""):
        q=s.get("quality","?"); sc=s.get("score",0); rr=s.get("rr",0)
        added=s.get("_wl_added") or s.get("_trade_entered")
        lines.append(
            f"  [{badge_col}]{badge:<14}[/{badge_col}]  "
            f"{qc(q)}  {dc(s['direction'])}  "
            f"[bold white]{s['sym']:<10}[/bold white]  "
            f"[dim]Skor:[/dim][bold]{sc:.0f}[/bold]  "
            f"[dim]R:R:[/dim][bold]1:{rr}[/bold]  "
            f"[dim]SL:[/dim][bright_red]{fp_plain(s.get('sl',0))}[/bright_red]  "
            f"[dim]TP:[/dim][bright_green]{fp_plain(s.get('tp',0))}[/bright_green]  "
            f"[dim]{age_str(added)}[/dim]{extra}")

    # ── AKTİF İŞLEMLER ──
    lines.append(f"[bold bright_green]▶ AKTİF İŞLEMLER  ({len(active_trades)})[/bold bright_green]")
    lines.append(f"[bright_green]{DIV}[/bright_green]")
    if not active_trades:
        lines.append("  [dim]Henüz açık işlem yok. İzleme listesinden giriş yapın.[/dim]")
    else:
        for k,t in active_trades.items():
            ep=t.get("_trade_entry_price",t.get("price",0))
            cur_md=market.get(t["sym"]); cur=cur_md.price if cur_md else ep
            sl=t["sl"]; risk=abs(ep-sl) if abs(ep-sl)>0 else 1
            if t["direction"]=="LONG": live_rr=round((cur-ep)/risk,2)
            else: live_rr=round((ep-cur)/risk,2)
            pnl_col="bright_green" if live_rr>=0 else "bright_red"
            trade_row(t,"▶ AKTİF","bright_green",
                      f"  Giriş:[dim]{fp_plain(ep)}[/dim]  Şu An:[bright_white]{fp_plain(cur)}[/bright_white]  "
                      f"Canlı R:R:[{pnl_col}]{live_rr:+.2f}R[/{pnl_col}]")
    lines.append("")

    # ── AKTİF İZLEME ──
    active=sorted(_wl_active.values(),
                  key=lambda x:({"A+":0,"A":1,"B+":2,"WATCH":3}.get(x.get("quality","?"),4),-x.get("rr",0)))
    lines.append(f"[bold bright_cyan]◎ AKTİF İZLEME  ({len(active)})  [dim]— girmek için: aktif setuplarda E tuşu planlaniyor[/dim][/bold bright_cyan]")
    lines.append(f"[bright_cyan]{DIV}[/bright_cyan]")
    if not active:
        lines.append("  [dim]İzleme listesi boş.[/dim]")
    else:
        for s in active:
            sm=s.get("sm_notes",[]); traps=s.get("trap_warnings",[])
            extra=""
            if traps: extra=f"  [bright_red]{traps[0][:50]}[/bright_red]"
            elif sm:  extra=f"  [dim cyan]{sm[0][:50]}[/dim cyan]"
            trade_row(s,"◎ İZLEME","bright_cyan",extra)
    lines.append("")

    # ── TETİKLENEN ──
    lines.append(f"[bold bright_yellow]✓ TETİKLENEN İŞLEMLER  ({len(_wl_triggered)})[/bold bright_yellow]")
    lines.append(f"[yellow]{DIV}[/yellow]")
    if not _wl_triggered:
        lines.append("  [dim]Henüz tetiklenen setup yok.[/dim]")
    else:
        for s in _wl_triggered[:6]:
            reason=s.get("_wl_reason","")
            trade_row(s,"✓ TETİKLENDİ","bright_yellow",f"  [dim]{reason}[/dim]")
    lines.append("")

    # ── İPTAL / SÜRESI DOLDU ──
    lines.append(f"[bold bright_red]✗ İPTAL  ({len(_wl_invalidated)})  [dim]|[/dim]  [dim]⏱ SÜRESI DOLDU  ({len(_wl_expired)})[/dim][/bold bright_red]")
    lines.append(f"[bright_red]{DIV}[/bright_red]")
    combined=sorted(_wl_invalidated[:4]+_wl_expired[:3],
                    key=lambda x: x.get("_wl_updated",datetime.min), reverse=True)
    if not combined:
        lines.append("  [dim]Henüz iptal edilen veya süresi dolan setup yok.[/dim]")
    else:
        for s in combined:
            st2=s.get("_wl_status","?")
            badge="✗ İPTAL" if st2=="INVALIDATED" else "⏱ SÜRESI DOLDU"
            bc="bright_red" if st2=="INVALIDATED" else "dim"
            fail=s.get("_wl_fail_condition",""); reason=s.get("_wl_reason","")
            note=f"  [dim]{reason}[/dim]"
            if fail: note+=f"  [bright_red]({fail})[/bright_red]"
            trade_row(s,badge,bc,note)

    total=len(active_trades)+len(active)+len(_wl_triggered)+len(_wl_invalidated)+len(_wl_expired)
    return Panel("\n".join(lines),
                 title="[bold bright_cyan]● WATCHLIST YÖNETİM SİSTEMİ — Titan Prime Elite[/bold bright_cyan]",
                 border_style="bright_cyan",box=box.ROUNDED,
                 subtitle=f"[dim]Toplam: {total}  ·  Aktif işlem: {len(active_trades)}  ·  İzleme: {len(active)}  ·  Hiçbir setup sessizce silinmez[/dim]")


def render():
    run_analysis()
    nav=Panel(nav_bar(),box=box.SIMPLE,border_style="dim",height=3)
    hdr=panel_header()

    if current_page==1:
        lo=Layout(); lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),Layout(panel_market()))
    elif current_page==2:
        lo=Layout(); lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),Layout(panel_setups()))
    elif current_page==3:
        lo=Layout(); lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),Layout(panel_details()))
    elif current_page==4:
        lo=Layout(); lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),Layout(panel_cot()))
    elif current_page==5:
        lo=Layout()
        body=Layout(); body.split_row(Layout(panel_journal()),Layout(panel_stats()))
        lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),body)
    elif current_page==6:
        lo=Layout()
        body=Layout(); body.split_row(Layout(panel_news(),ratio=3),Layout(panel_risk(),ratio=1))
        lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),body)
    elif current_page==7:
        lo=Layout(); lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),Layout(panel_quant()))
    elif current_page==8:
        lo=Layout(); lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),Layout(panel_portfolio()))
    else:
        lo=Layout(); lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),Layout(panel_watchlist()))
    return lo

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    console.print(Panel.fit(
        "[bold bright_yellow]TITAN FLOW[/bold bright_yellow] — Başlatılıyor...\n"
        "[dim]WebSocket (crypto) · yfinance (forex/metals/oil) · Finnhub (hisseler) · COT · Haberler · Journal[/dim]",
        border_style="bright_yellow"))
    threading.Thread(target=ws_loop,               daemon=True).start()
    threading.Thread(target=background_loop,        daemon=True).start()
    threading.Thread(target=cot_loop,               daemon=True).start()
    threading.Thread(target=monitor_loop,           daemon=True).start()
    threading.Thread(target=stats_loop,             daemon=True).start()
    threading.Thread(target=key_listener,            daemon=True).start()
    threading.Thread(target=auto_scroll_loop,        daemon=True).start()
    threading.Thread(target=portfolio_loop,          daemon=True).start()
    threading.Thread(target=performance_report_loop, daemon=True).start()
    # Açılışta mevcut sinyalleri hemen kontrol et
    try: _check_open()
    except: pass
    try: compute_stats()
    except: pass
    time.sleep(5)
    try:
        with Live(render(),refresh_per_second=1/REFRESH_SEC,screen=True,console=console) as live:
            while True:
                live.update(render())
                time.sleep(REFRESH_SEC)
    except KeyboardInterrupt:
        console.print("\n[bold bright_yellow]TITAN FLOW kapatıldı. Sermaye korundu.[/bold bright_yellow]")

if __name__=="__main__":
    main()
