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
MIN_TRADES_ADAPT  = 20

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
    {"label":"Risk-On",       "syms":["BTCUSDT","ETHUSDT","SPY","QQQ","AUD/USD"]},
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

# Finnhub WebSocket (crypto only — free plan)
WS_SYMBOLS = [
    "BINANCE:BTCUSDT","BINANCE:ETHUSDT","BINANCE:SOLUSDT",
    "BINANCE:BNBUSDT","BINANCE:XRPUSDT",
]

# Finnhub REST equities
EQ_SYMBOLS = ["NVDA","AAPL","SPY","QQQ","MSFT","TSLA"]

ALL_SYMBOLS = list(YF_SYMBOLS.keys()) + \
              [s.replace("BINANCE:","") for s in WS_SYMBOLS] + \
              EQ_SYMBOLS

DISPLAY_GROUPS = [
    ("FOREX MAJORS",  ["EUR/USD","GBP/USD","USD/JPY","USD/CHF","AUD/USD","USD/CAD","NZD/USD"]),
    ("FOREX CROSSES", ["EUR/GBP","EUR/JPY","GBP/JPY","EUR/CHF","AUD/JPY","GBP/CHF"]),
    ("METALS & OIL",  ["XAU/USD","XAG/USD","WTI","BRENT"]),
    ("CRYPTO",        ["BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT"]),
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
    "BTCUSDT":"BITCOIN - CHICAGO MERCANTILE EXCHANGE",
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
    "BTCUSDT":["bitcoin","btc","crypto"],
    "ETHUSDT":["ethereum","eth"],
    "SOLUSDT":["solana","sol"],
    "BNBUSDT":["binance","bnb"],
    "XRPUSDT":["xrp","ripple"],
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

# ── Telegram (optional — set your credentials to enable alerts) ──────────────
TELEGRAM_TOKEN   = ""   # e.g. "123456:ABCDef..."
TELEGRAM_CHAT_ID = ""   # e.g. "-1001234567890"

# ── High-impact keyword → base importance score ───────────────────────────────
HIGH_IMP_KW = {
    "fomc":95,"federal reserve":90,"rate decision":90,"rate hike":88,"rate cut":88,
    "emergency meeting":95,"quantitative tightening":80,"quantitative easing":82,
    "powell":72,"lagarde":72,"ueda":72,"bailey":72,"jordan":70,
    "ecb decision":90,"boe decision":90,"boj decision":90,"snb decision":85,
    "rba decision":82,"boc decision":82,"rbnz decision":80,
    "cpi":85,"inflation":72,"core inflation":85,"pce":83,"deflation":80,
    "non-farm payroll":90,"nfp":90,"unemployment rate":82,"jobless claims":68,
    "gdp":80,"recession":85,"stagflation":88,
    "retail sales":65,"pmi":60,"ism":62,"trade balance":58,
    "producer price":65,"ppi":65,"consumer confidence":55,
    "war":88,"military":75,"sanctions":82,"nuclear":95,"attack":80,"invasion":90,
    "conflict":72,"ceasefire":70,"tariff":75,"trade war":82,
    "default":90,"bankruptcy":85,"collapse":88,"crisis":85,"contagion":88,
    "bank run":92,"bailout":85,"systemic":88,"flash crash":90,"margin call":80,
    "earnings miss":65,"earnings beat":62,"guidance cut":68,"guidance raise":60,
    "sec investigation":75,"fraud":80,"circuit breaker":88,
}
MED_IMP_KW = {
    "interest rate":55,"monetary policy":58,"fiscal":52,"stimulus":60,
    "upgrade":42,"downgrade":45,"buy rating":38,"sell rating":40,
    "geopolitical":55,"election":58,"debt ceiling":65,"budget":50,
    "opec":62,"production cut":60,"merger":48,"acquisition":50,
    "regulatory":52,"antitrust":55,"lawsuit":48,
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

console = Console()
lock    = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# NEWS ANALYSIS ENGINE
# ─────────────────────────────────────────────────────────────────────────────
analyzed_news: list = []
_tg_sent: set      = set()

def _importance(text):
    score = 0
    for kw,pts in HIGH_IMP_KW.items():
        if kw in text: score = max(score, pts)
    for kw,pts in MED_IMP_KW.items():
        if kw in text: score = max(score, pts)
    hits = sum(1 for kw in HIGH_IMP_KW if kw in text)
    return min(score + hits*2, 100)

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
    return {**article,"importance":imp,"risk_level":rl,
            "asset_sent":asent,"regimes":regs,
            "vol":vol,"vol_dur":vd,"sym_impacts":simp,"correlations":corr}

def send_telegram(article):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    h=article.get("headline","")
    if h in _tg_sent: return
    _tg_sent.add(h)
    imp=article["importance"]
    impacts_txt="\n".join(f"  {sym} {d}" for sym,(d,_,s) in list(article.get("sym_impacts",{}).items())[:6])
    block_min=60 if imp>=80 else 30
    msg=(f"🚨 <b>HIGH IMPACT NEWS</b>\n\n"
         f"<b>Headline:</b>\n{h}\n\n"
         f"<b>Impact Score:</b> {imp}/100\n"
         f"<b>Risk Level:</b> {article['risk_level']}\n"
         f"<b>Volatility:</b> {article['vol']} ({article['vol_dur']})\n"
         f"<b>Regime:</b> {', '.join(article['regimes'])}\n\n"
         f"<b>Affected Symbols:</b>\n{impacts_txt}\n\n"
         f"⛔ <b>Trading Restriction:</b> New positions blocked for {block_min} minutes.")
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"HTML"},timeout=5)
    except: pass

def news_risk_for_sym(sym, max_age_min=90):
    """Returns (importance, risk_level, block_trade) for a symbol from recent news."""
    now=time.time()
    with lock: arts=list(analyzed_news)
    best=(0,"NOISE",False)
    for a in arts:
        ts=a.get("datetime",0)
        age_min=(now-ts)/60 if ts else 999
        if age_min>max_age_min: continue
        imp=a.get("importance",0)
        if sym not in a.get("sym_impacts",{}): continue
        block=(imp>=80 and age_min<=60) or (imp>=60 and age_min<=30)
        if imp>best[0]: best=(imp,a.get("risk_level","NOISE"),block)
    return best


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
            entry REAL, sl REAL, tp1 REAL, tp2 REAL, tp3 REAL,
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
            entry REAL, sl REAL, tp3 REAL, rr REAL,
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

# ═══════════════════════════════════════════════════════════════
# PORTFOLIO ENGINE
# ═══════════════════════════════════════════════════════════════
def calc_sizing(setup):
    """
    Calculate position sizing for a setup using Trade212 CFD rules.
    Returns dict with capital, risk_amount, expected_loss/profit, margin.
    """
    sym=setup["sym"]; entry=setup["price"]; sl=setup["sl"]
    tp2=setup["tp2"]; tp3=setup["tp3"]; rr=setup["rr"]
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
    exp_profit_tp2=round(actual_risk*rr*0.6,2)
    exp_profit_tp3=round(actual_risk*rr,2)
    return {
        "margin":margin,"notional":round(actual_notional,2),
        "risk_amt":actual_risk,"leverage":lev,
        "exp_loss":actual_risk,
        "exp_profit_tp2":exp_profit_tp2,"exp_profit_tp3":exp_profit_tp3,
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

    # ── News Risk Check (auto-reject on high-impact fresh news) ──
    n_imp, n_rl, n_block = news_risk_for_sym(sym)
    if n_block:
        return None   # HARD REJECT — critical/high news active

    # ── News Sentiment (0-10) ────────────────────────────────────
    news_score=0; news_rl=[]; news_rs=[]
    news_penalty=0
    if n_imp>=60:
        news_penalty=8   # medium news → -8 pts confidence
        neg_l.append(f"MEDIUM NEWS RISK (impact {n_imp}/100) — confidence reduced")
        neg_s.append(f"MEDIUM NEWS RISK (impact {n_imp}/100) — confidence reduced")
    elif n_imp>=40:
        news_penalty=4
        neg_l.append(f"Low-medium news risk (impact {n_imp}/100)")
        neg_s.append(f"Low-medium news risk (impact {n_imp}/100)")

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

    # ── Compute entry/SL/TP ──────────────────────────────────────
    if direction=="LONG":
        el=price-av*0.3; eh=price+av*0.15
        sl=price-av*1.8; tp1=price+av*1.5
        tp2=price+av*3.0; tp3=price+av*5.0
    else:
        el=price-av*0.15; eh=price+av*0.3
        sl=price+av*1.8; tp1=price-av*1.5
        tp2=price-av*3.0; tp3=price-av*5.0

    risk=abs(price-sl); reward=abs(tp3-price)
    rr=round(reward/risk,2) if risk>0 else 0

    # ── HARD REJECT: RR < 2.0 ───────────────────────────────────
    if rr<2.0:
        return None

    # ── RR bonus (10-15 pts) ─────────────────────────────────────
    rr_bonus = 15 if rr>=3.0 else 12 if rr>=2.5 else 10
    if rr<2.5: neg_factors.append(f"RR {rr} below preferred 1:2.5 threshold")

    # ── Normalise to 100 then blend RR bonus ─────────────────────
    score_100=round(min(max(raw/MAX_RAW*85+rr_bonus-news_penalty,0),100),1)

    # ── Expected hold time (TP2 distance ÷ ATR = hours) ──────────
    hold_h=round(abs(tp2-price)/av,1) if av else 8.0
    time_bonus=(2 if hold_h<=4 else 1 if hold_h<=8 else 0 if hold_h<=24 else -3)
    score_100=round(min(max(score_100+time_bonus,0),100),1)

    if hold_h>24: neg_factors.append(f"Hold time ~{hold_h:.0f}h — capital locked overnight+")
    elif hold_h>8: neg_factors.append(f"Hold time ~{hold_h:.0f}h — crosses session boundary")

    # ── HARD REJECT: confidence < 55 ─────────────────────────────
    if score_100<55: return None

    # ── Quality thresholds ───────────────────────────────────────
    if   score_100>=85: quality="A+"
    elif score_100>=75: quality="A"
    elif score_100>=65: quality="B+"
    elif score_100>=55: quality="WATCH"
    else: return None

    # A+ requires liquidity sweep
    if quality=="A+" and not sweep_confirmed:
        quality="A"

    # Status string
    if score_100>=75:
        status="APPROVED"
    elif score_100>=55:
        status="WATCHLIST"
    else:
        status="REJECTED"

    confidence=score_100

    news_risk_label=("NO RISK" if n_imp<20 else "LOW" if n_imp<40
                     else "MEDIUM" if n_imp<60 else "HIGH" if n_imp<80 else "CRITICAL")

    # Build placeholder setup for sizing (fill in entry/sl/tp before calling)
    _tmp={"sym":sym,"price":price,"sl":sl,"tp2":tp2,"tp3":tp3,"rr":rr}
    sizing=calc_sizing(_tmp) or {}

    # Institutional risk score
    inst_rs=portfolio_state.get("inst_risk_score",100)

    return {
        "sym":sym,"quality":quality,"direction":direction,"status":status,
        "price":price,"el":el,"eh":eh,"sl":sl,
        "tp1":tp1,"tp2":tp2,"tp3":tp3,"rr":rr,
        "score":score_100,"confidence":confidence,"hold_h":hold_h,
        "reasons":reasons,"neg_factors":neg_factors,
        "flags":fl,"rsi":rv,"atr":av,"cot":cot,
        "news_score":news_score,"news_imp":n_imp,"news_risk":news_risk_label,
        "sizing":sizing,"inst_risk_score":inst_rs,"portfolio_heat":heat,
        "time":datetime.now().strftime("%H:%M:%S"),
        "narrative": _narrative(sym,direction,price,el,eh,sl,tp1,tp2,tp3,rr,av,rv,mh,st,sw,bOB,beOB,bFVG,beFVG,cot,news_rl+news_rs,news_score),
    }

def _narrative(sym,dirn,price,el,eh,sl,tp1,tp2,tp3,rr,av,rv,mh,st,sw,bOB,beOB,bFVG,beFVG,cot,news,ns):
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
         f"  TP1 25% : {tp1:.5f}  → move SL to breakeven",
         f"  TP2 50% : {tp2:.5f}  → reduce size",
         f"  TP3 25% : {tp3:.5f}  → final runner",
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
    """Fetch equity + crypto candles from Finnhub."""
    all_syms=EQ_SYMBOLS+[s.replace("BINANCE:","") for s in WS_SYMBOLS[:2]]
    for sym in all_syms:
        fh_sym=f"BINANCE:{sym}" if sym.endswith("USDT") else sym
        try:
            now=int(time.time()); frm=now-200*3600
            p={"symbol":fh_sym,"resolution":"60","from":frm,"to":now,"token":API_KEY}
            ep=f"{BASE_URL}/crypto/candle" if sym.endswith("USDT") else f"{BASE_URL}/stock/candle"
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
    for cat in ["general","crypto","forex","merger"]:
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
            if a["importance"]>=80 and age_min<=60:
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
    while True:
        try:
            websocket.WebSocketApp(WS_URL,on_message=ws_msg,on_open=ws_open,
                on_close=ws_close,on_error=ws_err).run_forever(ping_interval=20)
        except: pass
        time.sleep(5)

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
    fl=s["flags"]
    sz=s.get("sizing",{})
    created=datetime.utcnow().isoformat(timespec="seconds")
    with db() as c:
        cur=c.execute("""INSERT INTO signals(sym,quality,direction,entry,sl,tp1,tp2,tp3,
            rr_t,score,f_ema,f_rsi,f_macd,f_sweep,f_ob,f_fvg,f_struct,f_cot,f_news,
            status,created)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)""",
            (s["sym"],s["quality"],s["direction"],s["price"],s["sl"],
             s["tp1"],s["tp2"],s["tp3"],s["rr"],s["score"],
             fl["f_ema"],fl["f_rsi"],fl["f_macd"],fl["f_sweep"],
             fl["f_ob"],fl["f_fvg"],fl["f_struct"],fl["f_cot"],fl["f_news"],
             created))
        sig_id=cur.lastrowid
        # Shadow trade
        if sz:
            c.execute("""INSERT INTO shadow_trades(signal_id,sym,direction,
                entry,sl,tp3,rr,capital,risk_amount,status,created)
                VALUES(?,?,?,?,?,?,?,?,?,'OPEN',?)""",
                (sig_id,s["sym"],s["direction"],s["price"],s["sl"],
                 s["tp3"],s["rr"],sz.get("margin",0),sz.get("risk_amt",0),created))
            # Update shadow balance state
            c.execute("INSERT OR REPLACE INTO account_state(key,value,updated) VALUES('shadow_balance',?,?)",
                      (portfolio_state["shadow_balance"],created))
            c.execute("INSERT OR IGNORE INTO account_state(key,value,updated) VALUES('daily_start',?,?)",
                      (portfolio_state["shadow_balance"],created))
        c.commit()

def monitor_loop():
    while True:
        try: _check_open()
        except: pass
        time.sleep(60)

def _check_open():
    now=datetime.utcnow().isoformat(timespec="seconds")
    with db() as c:
        rows=c.execute("SELECT * FROM signals WHERE status='OPEN'").fetchall()
        for r in rows:
            sym=r["sym"]; ep=r["entry"]; sl=r["sl"]
            tp1=r["tp1"]; tp2=r["tp2"]; tp3=r["tp3"]
            with lock: md=market.get(sym); cur=md.price if md else None
            if cur is None: continue
            risk=abs(ep-sl)
            if risk==0: continue
            ns=None; arr=None
            if r["direction"]=="LONG":
                if cur<=sl:  ns="SL";  arr=-1.0
                elif cur>=tp3: ns="TP3"; arr=round(r["rr_t"],2)
                elif cur>=tp2: ns="TP2"; arr=round(r["rr_t"]*0.6,2)
                elif cur>=tp1: ns="TP1"; arr=round(r["rr_t"]*0.3,2)
            else:
                if cur>=sl:  ns="SL";  arr=-1.0
                elif cur<=tp3: ns="TP3"; arr=round(r["rr_t"],2)
                elif cur<=tp2: ns="TP2"; arr=round(r["rr_t"]*0.6,2)
                elif cur<=tp1: ns="TP1"; arr=round(r["rr_t"]*0.3,2)
            try:
                age=(datetime.utcnow()-datetime.fromisoformat(r["created"])).days
                if age>=7 and ns is None:
                    ns="EXPIRED"; arr=round((cur-ep)/risk,2) if r["direction"]=="LONG" else round((ep-cur)/risk,2)
            except: pass
            if ns:
                c.execute("UPDATE signals SET status=?,out_price=?,out_at=?,act_rr=? WHERE id=?",
                          (ns,cur,now,arr,r["id"]))
                # Update shadow trade
                st_row=c.execute("SELECT * FROM shadow_trades WHERE signal_id=? AND status='OPEN'",
                                 (r["id"],)).fetchone()
                if st_row:
                    cap=st_row["capital"] or 0
                    risk=st_row["risk_amount"] or 0
                    pnl=round(arr*risk,2) if arr is not None else 0
                    bal_row=c.execute("SELECT value FROM account_state WHERE key='shadow_balance'").fetchone()
                    new_bal=round((bal_row["value"] if bal_row else ACCOUNT["balance"])+pnl,2)
                    c.execute("UPDATE shadow_trades SET status=?,out_price=?,out_at=?,pnl=?,pnl_pct=? WHERE id=?",
                              (ns,cur,now,pnl,round(pnl/cap*100,1) if cap else 0,st_row["id"]))
                    c.execute("INSERT OR REPLACE INTO account_state(key,value,updated) VALUES('shadow_balance',?,?)",
                              (new_bal,now))
                    with lock:
                        portfolio_state["shadow_balance"]=new_bal
                        portfolio_state["shadow_equity"].append(new_bal)
                        if ns in ("TP1","TP2","TP3"): portfolio_state["shadow_wins"]+=1
                        elif ns=="SL": portfolio_state["shadow_losses"]+=1
        c.commit()

def compute_stats():
    global stats_cache, adap_weights
    with db() as c:
        closed=c.execute("SELECT * FROM signals WHERE status!='OPEN'").fetchall()
        if not closed: return
        total=len(closed)
        wins=[r for r in closed if r["status"] in ("TP1","TP2","TP3")]
        wr=round(len(wins)/total*100,1)
        rrs=[r["act_rr"] for r in closed if r["act_rr"] is not None]
        avg_rr=round(sum(rrs)/len(rrs),2) if rrs else 0
        gw=sum(r for r in rrs if r>0); gl=abs(sum(r for r in rrs if r<0))
        pf=round(gw/gl,2) if gl else 99.0
        bq={}
        for q in ("A+","A","B+"):
            qr=[r for r in closed if r["quality"]==q]
            qw=[r for r in qr if r["status"] in ("TP1","TP2","TP3")]
            bq[q]={"t":len(qr),"w":len(qw),"wr":round(len(qw)/len(qr)*100,1) if qr else 0}
        feats=["f_ema","f_rsi","f_macd","f_sweep","f_ob","f_fvg","f_struct","f_cot","f_news"]
        fstats={}; new_w={}
        for f in feats:
            fr=[r for r in closed if r[f]==1]
            fw=[r for r in fr if r["status"] in ("TP1","TP2","TP3")]
            fwr=round(len(fw)/len(fr)*100,1) if fr else None
            fstats[f]={"n":len(fr),"w":len(fw),"wr":fwr}
            if len(fr)>=10 and fwr is not None:
                new_w[f]=round(max(0.5,min(1.5,0.5+fwr/50)),3)
                c.execute("UPDATE weights SET mult=?,win_rate=?,n=?,updated=? WHERE feature=?",
                          (new_w[f],fwr,len(fr),datetime.utcnow().isoformat(timespec="seconds"),f))
            else: new_w[f]=1.0
        c.commit()
        recent=c.execute("SELECT sym,quality,direction,status,act_rr,rr_t,entry,created FROM signals ORDER BY id DESC LIMIT 12").fetchall()

        # ── Quant metrics ──
        trade_rets=[r["act_rr"]*0.01 for r in closed if r["act_rr"] is not None]
        equity=[1.0]
        for r in trade_rets: equity.append(equity[-1]*(1+r))
        wins_rr=[r["act_rr"] for r in wins if r["act_rr"]]
        loss_rr=[abs(r["act_rr"]) for r in closed if r["status"]=="SL" and r["act_rr"]]
        avg_win=sum(wins_rr)/len(wins_rr) if wins_rr else 0
        avg_loss=sum(loss_rr)/len(loss_rr) if loss_rr else 1
        sharpe=sharpe_ratio(trade_rets)
        sortino=sortino_ratio(trade_rets)
        mdd=max_drawdown_pct(equity)
        var95=historical_var(trade_rets)
        kelly=kelly_pct(wr,avg_win,avg_loss)
        calmar=round(avg_rr/mdd,2) if mdd>0 else None

        # Monte Carlo
        mc=None
        if total>=5:
            try: mc=monte_carlo(wr, avg_win, avg_loss, n_trades=max(total,163))
            except: pass

        # Seans analizi
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
            if r["status"] in ("TP1","TP2","TP3"): sess_stats[sess]["w"]+=1

        # Sembol bazlı performans
        sym_perf={}
        for r in closed:
            s=r["sym"]
            sym_perf.setdefault(s,{"t":0,"w":0,"rr":[]})
            sym_perf[s]["t"]+=1
            if r["status"] in ("TP1","TP2","TP3"):
                sym_perf[s]["w"]+=1
            if r["act_rr"]: sym_perf[s]["rr"].append(r["act_rr"])

        with lock:
            stats_cache={"total":total,"wins":len(wins),"wr":wr,"avg_rr":avg_rr,"pf":pf,
                         "tp3":sum(1 for r in closed if r["status"]=="TP3"),
                         "tp2":sum(1 for r in closed if r["status"]=="TP2"),
                         "tp1":sum(1 for r in closed if r["status"]=="TP1"),
                         "sl":sum(1 for r in closed if r["status"]=="SL"),
                         "by_q":bq,"fstats":fstats,
                         "recent":[dict(r) for r in recent],
                         "sharpe":sharpe,"sortino":sortino,"calmar":calmar,
                         "mdd":mdd,"var95":var95,"kelly":kelly,
                         "avg_win":avg_win,"avg_loss":avg_loss,
                         "sess_stats":sess_stats,"sym_perf":sym_perf,"mc":mc}
        if total>=MIN_TRADES_ADAPT:
            adap_weights.update(new_w)

def stats_loop():
    while True:
        try: compute_stats()
        except: pass
        time.sleep(300)

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
                try: log_signal(r)
                except: pass
        except: pass
    results.sort(key=lambda x:({"A+":0,"A":1,"B+":2}.get(x["quality"],9),-x["rr"]))
    setups=results

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
    c={"TP3":"bold bright_green","TP2":"bright_green","TP1":"green",
       "SL":"bold bright_red","OPEN":"bright_yellow","EXPIRED":"dim"}.get(s,"white")
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

    # Shadow trades
    try:
        with db() as c:
            st_rows=c.execute("SELECT sym,direction,capital,risk_amount,pnl,status FROM shadow_trades ORDER BY id DESC LIMIT 10").fetchall()
    except: st_rows=[]
    if st_rows:
        lines.append("[bold]━━━  SHADOW PORTFÖY POZİSYONLARI  ━━━[/bold]")
        lines.append("")
        lines.append(f"  [bold dim]{'SEMBOL':<10} {'YÖN':<7} {'MARJ':>7} {'RİSK':>7} {'P&L':>8} {'DURUM':<8}[/bold dim]")
        lines.append("  " + "─"*52)
        for r in st_rows:
            pnl_v=r["pnl"]
            pnl_s=(f"[bright_green]£{pnl_v:+.2f}[/bright_green]" if pnl_v and pnl_v>0
                   else f"[bright_red]£{pnl_v:+.2f}[/bright_red]" if pnl_v and pnl_v<0 else "[dim]—[/dim]")
            st2=r["status"]
            sc={"TP3":"bold bright_green","TP2":"bright_green","TP1":"green",
                "SL":"bold bright_red","OPEN":"bright_yellow","EXPIRED":"dim"}.get(st2,"white")
            dir_s="[bright_green]L[/bright_green]" if r["direction"]=="LONG" else "[bright_red]S[/bright_red]"
            lines.append(f"  [bold white]{r['sym']:<10}[/bold white] {dir_s}  "
                        f"[dim]£{r['capital'] or 0:.2f}[/dim]  "
                        f"[dim]£{r['risk_amount'] or 0:.2f}[/dim]  "
                        f"{pnl_s}  [{sc}]{st2}[/{sc}]")

    return Panel("\n".join(lines),
                 title="[bold bright_green]● EXECUTIVE DASHBOARD — Trade212 CFD Portfolio Engine[/bold bright_green]",
                 border_style="bright_green",box=box.HEAVY,
                 subtitle=f"[dim]Balance: £{bal:.2f}  |  Heat: {heat:.1f}%  |  IRS: {irs:.0f}/100[/dim]")

def panel_market():
    t=Table(title="[bold]● LIVE MARKET FEED[/bold]",box=box.SIMPLE_HEAVY,
            border_style="bright_blue",header_style="bold bright_blue",show_lines=True)
    t.add_column("SYMBOL", width=12,style="bold white")
    t.add_column("PRICE",  width=13,justify="right")
    t.add_column("CHG",    width=9, justify="right")
    t.add_column("HIGH",   width=12,justify="right",style="green")
    t.add_column("LOW",    width=12,justify="right",style="red")
    t.add_column("UPDATED",width=9, style="dim")
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
    ss=list(setups)
    if not ss:
        return Panel(Align.center(
            "[bold bright_yellow]KURUMSAL SETUP TARANIYOR...[/bold bright_yellow]\n"
            "[dim]Min R:R 1:2.0  ·  Skor ≥55/100  ·  Her 30 saniyede güncellenir[/dim]"),
            title="[bold bright_yellow]● AKTİF SETUPLАР[/bold bright_yellow]",
            border_style="bright_yellow",box=box.HEAVY)
    t=Table(title="[bold bright_yellow]● AKTİF SETUPLАР — KURUMSAL KALİTE FİLTRESİ[/bold bright_yellow]",
            box=box.SIMPLE_HEAVY,border_style="bright_yellow",
            header_style="bold bright_yellow",show_lines=True)
    t.add_column("GRADE",  width=6,  justify="center")
    t.add_column("STATUS", width=10, justify="center")
    t.add_column("SYMBOL", width=10, style="bold white")
    t.add_column("DIR",    width=8,  justify="center")
    t.add_column("SKOR",   width=8,  justify="center")
    t.add_column("GÜVEN",  width=7,  justify="center")
    t.add_column("FİYAT",  width=12, justify="right")
    t.add_column("GİRİŞ ZONU",width=22,justify="right")
    t.add_column("STOP",   width=12, justify="right",style="bright_red")
    t.add_column("TP1",    width=12, justify="right",style="green")
    t.add_column("TP2",    width=12, justify="right",style="bright_green")
    t.add_column("TP3",    width=12, justify="right",style="bright_yellow")
    t.add_column("R:R",    width=7,  justify="center")
    t.add_column("HOLD",   width=7,  justify="center",style="dim")
    t.add_column("SAAT",   width=7)
    for s in ss:
        rv=s.get("rsi"); rc="bright_green" if rv and rv<40 else "bright_red" if rv and rv>65 else "white"
        sc=s["score"]; sk_c="bold bright_yellow" if sc>=85 else "bold green" if sc>=75 else "cyan" if sc>=65 else "dim"
        st2=s.get("status","?")
        st_c="bold bright_green" if st2=="APPROVED" else "yellow" if st2=="WATCHLIST" else "bright_red"
        hold=s.get("hold_h",0)
        hold_s=f"{hold:.0f}h" if hold else "—"
        t.add_row(
            qc(s["quality"]),
            f"[{st_c}]{st2}[/{st_c}]",
            s["sym"], dc(s["direction"]),
            f"[{sk_c}]{sc:.0f}/100[/{sk_c}]",
            f"{s.get('confidence',sc):.0f}%",
            f"[bright_white]{fp(s['price'])}[/bright_white]",
            f"{fp(s['el'])} – {fp(s['eh'])}",
            fp(s["sl"]),fp(s["tp1"]),fp(s["tp2"]),fp(s["tp3"]),
            f"[bold]1:{s['rr']}[/bold]",
            hold_s, s["time"])
    return Panel(t,border_style="bright_yellow",box=box.HEAVY)

def panel_details():
    ss=list(setups)[:3]
    if not ss:
        return Panel("[dim]Setup oluşunca burada detay görünür.[/dim]",
                     title="[bold bright_yellow]● SETUP DETAY[/bold bright_yellow]",
                     border_style="bright_yellow",box=box.ROUNDED)
    panels=[]
    for s in ss:
        sc=s["score"]; conf=s.get("confidence",sc); hold=s.get("hold_h",0)
        st2=s.get("status","?"); rr=s["rr"]
        st_c="bold bright_green" if st2=="APPROVED" else "yellow" if st2=="WATCHLIST" else "bold bright_red"
        dec=("Execute" if st2=="APPROVED" else "Watchlist" if st2=="WATCHLIST" else "Reject")
        dec_c="bright_green" if dec=="Execute" else "yellow" if dec=="Watchlist" else "bright_red"

        # Header block — matches requested output format
        nr=s.get("news_risk","NO RISK"); ni=s.get("news_imp",0)
        nr_c={"CRITICAL":"bold bright_red","HIGH":"bright_red","MEDIUM":"yellow","LOW":"dim","NO RISK":"bright_green"}.get(nr,"dim")
        sz=s.get("sizing",{}); irs=s.get("inst_risk_score",100); ph=s.get("portfolio_heat",0)
        ic2="bright_green" if irs>=80 else "yellow" if irs>=60 else "bright_red"
        hc2="bright_green" if ph<5 else "yellow" if ph<10 else "bright_red"
        header=(
            f"{qc(s['quality'])}  {dc(s['direction'])}  [bold white]{s['sym']}[/bold white]\n\n"
            f"  [bold]TRADE SCORE        :[/bold] [bold bright_yellow]{sc:.0f}/100[/bold bright_yellow]\n"
            f"  [bold]STATUS             :[/bold] [{st_c}]{st2}[/{st_c}]\n"
            f"  [bold]CONFIDENCE         :[/bold] {conf:.0f}/100\n"
            f"  [bold]HOLD TIME          :[/bold] ~{hold:.1f} saat\n"
            f"  [bold]RISK REWARD        :[/bold] 1:{rr}\n"
            f"  [bold]NEWS RISK          :[/bold] [{nr_c}]{nr}[/{nr_c}]  [dim](impact {ni}/100)[/dim]\n"
            f"  [bold]PORTFOLIO HEAT     :[/bold] [{hc2}]{ph:.1f}%[/{hc2}]\n"
            f"  [bold]INST. RISK SCORE   :[/bold] [{ic2}]{irs:.0f}/100[/{ic2}]\n"
            +(f"\n  [bold dim]── POZİSYON BOYUTU (Trade212 CFD) ──[/bold dim]\n"
              f"  [bold]Önerilen Marj      :[/bold] [bright_white]£{sz.get('margin',0):.2f}[/bright_white]  [dim]({sz.get('leverage',1)}:1 kaldıraç → £{sz.get('notional',0):.2f} pozisyon)[/dim]\n"
              f"  [bold]Beklenen Kayıp     :[/bold] [bright_red]£{sz.get('exp_loss',0):.2f}[/bright_red]\n"
              f"  [bold]Beklenen Kâr TP2   :[/bold] [bright_green]£{sz.get('exp_profit_tp2',0):.2f}[/bright_green]\n"
              f"  [bold]Beklenen Kâr TP3   :[/bold] [bold bright_green]£{sz.get('exp_profit_tp3',0):.2f}[/bold bright_green]\n"
              f"  [bold]Risk / Trade       :[/bold] {sz.get('risk_pct',0):.2f}% sermaye\n"
              if sz else ""))

        # Positive factors
        pos="\n".join(f"  [bright_green]✓[/bright_green] {r}" for r in s["reasons"]) or "  [dim]—[/dim]"

        # Negative factors
        neg_list=s.get("neg_factors",[])
        neg="\n".join(f"  [bright_red]✗[/bright_red] {r}" for r in neg_list) or "  [dim]Yok[/dim]"

        # COT block
        cot=s.get("cot",{}); cot_txt=""
        if cot:
            pr=cot.get("pct_rank",50)
            cot_txt=(f"\n[bold dim]── COT (CFTC) ──[/bold dim]\n"
                     f"  {cot.get('date','—')}  |  {bias_c(cot.get('bias',''))}  |  {pbar(pr)} {pr:.0f}%\n"
                     f"  Spec net: {cot.get('spec_net',0):+,}  Δ {cot.get('spec_chg',0):+,}  "
                     f"Comm net: {cot.get('comm_net',0):+,}\n")
            if cot.get("contrarian"):
                cot_txt+=f"  [bold bright_yellow]⚠ CONTRARIAN {cot['contrarian']}[/bold bright_yellow]\n"

        content=(
            f"{header}\n"
            f"[bold dim]── POZİTİF FAKTÖRLER ──[/bold dim]\n{pos}\n\n"
            f"[bold dim]── NEGATİF FAKTÖRLER ──[/bold dim]\n{neg}"
            f"{cot_txt}\n"
            f"[bold dim]── GİRİŞ GEREKÇESİ ──[/bold dim]\n"
            +"\n".join(f"  {l}" for l in s["narrative"].split("\n"))+"\n\n"
            f"  [bold]KARAR:[/bold] [{dec_c}]{dec.upper()}[/{dec_c}]")
        panels.append(Panel(content,
            title=f"[bold bright_yellow]{s['sym']} — {s['time']}[/bold bright_yellow]",
            border_style="bright_yellow",box=box.ROUNDED))
    if len(panels)==3:
        lo=Layout(); lo.split_row(Layout(panels[0]),Layout(panels[1]),Layout(panels[2]))
        return lo
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
    t.add_column("SEMBOL",    width=11,style="bold white")
    t.add_column("TARİH",     width=11,style="dim")
    t.add_column("SPEC NET",  width=11,justify="right")
    t.add_column("HAFTALIK Δ",width=11,justify="right")
    t.add_column("COMM NET",  width=11,justify="right")
    t.add_column("OI",        width=10,justify="right",style="dim")
    t.add_column("12H RANK",  width=14,justify="center")
    t.add_column("BIAS",      width=14,justify="center")
    t.add_column("CONTRARIAN",width=10,justify="center")
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
            open_n=c.execute("SELECT COUNT(*) FROM signals WHERE status='OPEN'").fetchone()[0]
            recent=c.execute("SELECT sym,quality,direction,status,act_rr,rr_t,entry,created FROM signals ORDER BY id DESC LIMIT 12").fetchall()
    except: open_n=0; recent=[]
    t=Table(title=f"[bold bright_cyan]● TRADE JOURNAL  [dim](Açık: {open_n})[/dim][/bold bright_cyan]",
            box=box.SIMPLE_HEAVY,border_style="bright_cyan",
            header_style="bold bright_cyan",show_lines=True)
    t.add_column("SAAT",   width=7, style="dim")
    t.add_column("SEMBOL", width=11,style="bold white")
    t.add_column("GR",     width=5, justify="center")
    t.add_column("DIR",    width=7, justify="center")
    t.add_column("GİRİŞ",  width=12,justify="right",style="dim")
    t.add_column("DURUM",  width=8, justify="center")
    t.add_column("GERÇEK R:R",width=10,justify="right")
    t.add_column("HEDEF R:R", width=10,justify="right",style="dim")
    if not recent:
        t.add_row("—","—","—","—","—","[dim]Henüz sinyal yok[/dim]","—","—")
    else:
        for r in recent:
            arr=r["act_rr"]
            arr_s=(f"[bright_green]1:{arr:.2f}[/bright_green]" if arr and arr>0
                   else f"[bright_red]{arr:.2f}[/bright_red]" if arr and arr<0 else "[dim]—[/dim]")
            ts=str(r["created"])[-8:-3] if r["created"] else "—"
            t.add_row(ts,r["sym"],qc(r["quality"]),dc(r["direction"]),
                     fp(r["entry"]),oc(r["status"]),arr_s,
                     f"1:{r['rr_t']:.2f}" if r["rr_t"] else "—")
    return Panel(t,border_style="bright_cyan",box=box.ROUNDED)

def panel_stats():
    with lock: st=dict(stats_cache)
    if not st:
        return Panel(Align.center("[dim]İstatistik bekleniyor — ilk sinyaller kapanınca görünür.[/dim]"),
                     title="[bold bright_cyan]● PERFORMANS & ADAPTİF ÖĞRENME[/bold bright_cyan]",
                     border_style="bright_cyan",box=box.ROUNDED)
    total=st.get("total",0); wr=st.get("wr",0); pf=st.get("pf",0); avg=st.get("avg_rr",0)
    wc="bright_green" if wr>=55 else "bright_red" if wr<45 else "yellow"
    adap=len(adap_weights)>0 and total>=MIN_TRADES_ADAPT
    lines=[
        f"[bold]KPI[/bold]  [dim]{total} kapanmış[/dim]  [{wc}]{wr:.1f}% WR[/{wc}]  "
        f"[bold]PF {pf:.2f}[/bold]  Avg R:R [bold]{avg:+.2f}[/bold]",
        f"TP3 [bright_green]{st.get('tp3',0)}[/bright_green]  "
        f"TP2 [green]{st.get('tp2',0)}[/green]  TP1 {st.get('tp1',0)}  "
        f"SL [bright_red]{st.get('sl',0)}[/bright_red]  |  "
        f"Adaptif: " + ("[bright_green]AKTİF[/bright_green]" if adap
                        else f"[dim]{max(0,MIN_TRADES_ADAPT-total)} trade daha[/dim]"),
        "",
    ]
    bq=st.get("by_q",{})
    row="[bold dim]KALİTE:[/bold dim]  "
    for q in ("A+","A","B+"):
        d=bq.get(q,{"t":0,"w":0,"wr":0})
        if d["t"]:
            wc2="bright_green" if d["wr"]>=55 else "bright_red" if d["wr"]<45 else "yellow"
            row+=f"{qc(q)} [{wc2}]{d['wr']:.0f}%[/{wc2}] {d['w']}/{d['t']}   "
    lines.append(row); lines.append("")
    lines.append("[bold dim]ÖZELLİK SIRALAMASI  (kazanma oranı → adaptif ağırlık)[/bold dim]")
    feat_names={"f_ema":"EMA Stack","f_rsi":"RSI Extreme","f_macd":"MACD Cross",
                "f_sweep":"Liq Sweep","f_ob":"Order Block","f_fvg":"FVG",
                "f_struct":"Structure","f_cot":"COT Signal","f_news":"News Cat"}
    fst=st.get("fstats",{})
    sorted_f=sorted(fst.items(),key=lambda x:(x[1].get("wr") or 0) if x[1]["n"]>=5 else -1,reverse=True)
    for col,fd in sorted_f:
        n=fd["n"]; fw=fd.get("wr"); wt=adap_weights.get(col,1.0)
        if n<3: bar="[dim]veri yok[/dim]"
        else:
            wc3="bright_green" if (fw or 0)>=60 else "bright_red" if (fw or 0)<40 else "yellow"
            bar=f"[{wc3}]{fw:.0f}%[/{wc3}] [dim]({fd['w']}/{n})[/dim]"
        wts=(f"[bright_green]↑×{wt:.2f}[/bright_green]" if wt>1.05
             else f"[bright_red]↓×{wt:.2f}[/bright_red]" if wt<0.95
             else f"[dim]×{wt:.2f}[/dim]")
        lines.append(f"  [dim]{feat_names.get(col,col):<14}[/dim] {bar}  {wts}")

    # ── Kapanmış pozisyonlar tablosu ──
    lines.append("")
    lines.append("[bold dim]━━━  KAPANMIŞ POZİSYONLAR  ━━━[/bold dim]")
    try:
        with db() as c:
            rows=c.execute("""SELECT sym,quality,direction,entry,sl,tp1,tp3,
                               out_price,act_rr,rr_t,status,created
                               FROM signals WHERE status!='OPEN'
                               ORDER BY id DESC LIMIT 30""").fetchall()
    except: rows=[]

    if not rows:
        lines.append("  [dim]Henüz kapanmış pozisyon yok.[/dim]")
    else:
        # Başlık satırı
        lines.append(
            f"  [bold dim]{'SAAT':<7} {'SEMBOL':<10} {'GR':<4} {'YÖN':<6} "
            f"{'GİRİŞ':>10} {'STOP':>10} {'TP3':>10} "
            f"{'ÇIKIŞ':>10} {'DURUM':<7} {'GERÇEK R:R':>10} {'HEDEF R:R':>9}[/bold dim]")
        lines.append("  [dim]" + "─"*105 + "[/dim]")
        for r in rows:
            ts=str(r["created"])[-8:-3] if r["created"] else "—"
            st2=r["status"]
            sc={"TP3":"bold bright_green","TP2":"bright_green","TP1":"green",
                "SL":"bold bright_red","EXPIRED":"dim"}.get(st2,"white")
            arr=r["act_rr"]
            arr_s=(f"[bright_green]+{arr:.2f}R[/bright_green]" if arr and arr>0
                   else f"[bright_red]{arr:.2f}R[/bright_red]" if arr and arr<0 else "[dim]—[/dim]")
            dir_s="[bright_green]▲ LONG[/bright_green]" if r["direction"]=="LONG" else "[bright_red]▼ SHORT[/bright_red]"
            lines.append(
                f"  [dim]{ts:<7}[/dim] [bold white]{r['sym']:<10}[/bold white] {qc(r['quality']):<4} "
                f"{dir_s:<6} "
                f"[dim]{fp(r['entry']):>10}[/dim] "
                f"[red]{fp(r['sl']):>10}[/red] "
                f"[green]{fp(r['tp3']):>10}[/green] "
                f"[dim]{fp(r['out_price']):>10}[/dim] "
                f"[{sc}]{st2:<7}[/{sc}] "
                f"{arr_s:>10}  "
                f"[dim]1:{r['rr_t']:.2f}[/dim]")

    return Panel("\n".join(lines),
                 title="[bold bright_cyan]● PERFORMANS & ADAPTİF ÖĞRENME[/bold bright_cyan]",
                 border_style="bright_cyan",box=box.ROUNDED,
                 subtitle=f"[dim]{DB_PATH}[/dim]")

def panel_news():
    with lock: arts=list(analyzed_news) or list(news_cache)
    if not arts:
        return Panel(Align.center("[dim]Haberler yükleniyor...[/dim]"),
                     title="[bold bright_blue]● KURUMSAL MAKRO İSTİHBARAT[/bold bright_blue]",
                     border_style="bright_blue",box=box.ROUNDED)
    lines=[]
    for x in arts[:10]:
        imp   =x.get("importance",0)
        rl    =x.get("risk_level","NOISE")
        regs  =x.get("regimes",["NEUTRAL"])
        vol   =x.get("vol","—")
        vol_d =x.get("vol_dur","")
        simp  =x.get("sym_impacts",{})
        asent =x.get("asset_sent",{})
        ts    =x.get("datetime",0)
        age_m =(time.time()-ts)/60 if ts else 0
        age_s =f"{age_m:.0f}dk" if age_m<60 else f"{age_m/60:.1f}sa"

        # Colour by risk level
        rc={"CRITICAL":"bold bright_red","HIGH":"bright_red",
            "MEDIUM":"yellow","LOW":"dim green","NOISE":"dim"}.get(rl,"dim")
        vc={"EXTREME":"bold bright_red","HIGH":"bright_red","NORMAL":"yellow","LOW":"dim"}.get(vol,"dim")

        # Score bar
        bar_w=12; filled=int(imp/100*bar_w)
        bar="█"*filled+"░"*(bar_w-filled)
        bc="bright_red" if imp>=80 else "yellow" if imp>=60 else "green" if imp>=40 else "dim"

        lines.append(f"[{rc}]{'━'*96}[/{rc}]")
        lines.append(
            f"  [{bc}]{bar}[/{bc}] [bold {rc}]{imp:>3}/100[/bold {rc}]  "
            f"[bold white]{x.get('headline','')[:80]}[/bold white]")
        lines.append(
            f"  [dim]{x.get('source','')[:14]}  ·  {age_s}[/dim]  "
            f"[{rc}]{rl}[/{rc}]  "
            f"[{vc}]VOL: {vol}[/{vc}]  [dim]{vol_d}[/dim]  "
            f"[dim]Regime: {', '.join(regs[:2])}[/dim]")

        # Summary snippet
        summ=x.get("summary","").replace("\n"," ").strip()
        if summ:
            lines.append(f"  [dim]{summ[:130]}{'...' if len(summ)>130 else ''}[/dim]")

        # Asset sentiment row
        if asent:
            sent_parts=[]
            for asset,(s,strength) in list(asent.items())[:6]:
                ac="bright_green" if s=="BULLISH" else "bright_red"
                arr="↑" if s=="BULLISH" else "↓"
                sent_parts.append(f"[{ac}]{asset} {arr}{strength:.0f}[/{ac}]")
            lines.append("  " + "  ".join(sent_parts))

        # Symbol impact row
        if simp:
            imp_parts=[]
            for sym,(d,strength,s) in list(simp.items())[:8]:
                ic="bright_green" if d=="↑" else "bright_red"
                imp_parts.append(f"[{ic}]{sym}{d}[/{ic}]")
            lines.append("  " + "  ".join(imp_parts))

        lines.append("")

    return Panel("\n".join(lines).rstrip(),
                 title="[bold bright_blue]● KURUMSAL MAKRO İSTİHBARAT — Haber Etki Analizi[/bold bright_blue]",
                 border_style="bright_blue",box=box.ROUNDED,
                 subtitle="[dim]Impact 80+ → Telegram · Impact 60+/30dk → TRADE BLOCK · Impact 80+/60dk → TRADE BLOCK[/dim]")

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

    lines=["[bold]━━━  TEMEL ORANLAR  ━━━[/bold]",""]
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
        for sym,d in sorted(sym_perf.items(),key=lambda x:-(x[1]["w"]/x[1]["t"] if x[1]["t"] else 0)):
            t=d["t"]; w=d["w"]
            if t<2: continue
            swr=round(w/t*100,1)
            avg_r=round(sum(d["rr"])/len(d["rr"]),2) if d["rr"] else 0
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

PAGE_NAMES = {1:"PİYASA",2:"SETUPLAR",3:"DETAY",4:"COT",5:"JOURNAL",6:"HABERLER",7:"QUANT",8:"PORTFÖY"}

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
                if ch in (b'1',b'2',b'3',b'4',b'5',b'6',b'7',b'8'):
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
    else:
        lo=Layout(); lo.split_column(Layout(hdr,size=3),Layout(nav,size=3),Layout(panel_portfolio()))
    return lo

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    console.print(Panel.fit(
        "[bold bright_yellow]TITAN FLOW[/bold bright_yellow] — Başlatılıyor...\n"
        "[dim]WebSocket (crypto) · yfinance (forex/metals/oil) · Finnhub (hisseler) · COT · Haberler · Journal[/dim]",
        border_style="bright_yellow"))
    threading.Thread(target=ws_loop,        daemon=True).start()
    threading.Thread(target=background_loop, daemon=True).start()
    threading.Thread(target=cot_loop,        daemon=True).start()
    threading.Thread(target=monitor_loop,    daemon=True).start()
    threading.Thread(target=stats_loop,      daemon=True).start()
    threading.Thread(target=key_listener,     daemon=True).start()
    threading.Thread(target=auto_scroll_loop, daemon=True).start()
    threading.Thread(target=portfolio_loop,   daemon=True).start()
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
