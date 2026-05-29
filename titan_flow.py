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
DB_PATH  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "titan_journal.db")

REFRESH_SEC       = 2
ANALYSIS_SEC      = 30
DEDUP_SEC         = 7200
MIN_TRADES_ADAPT  = 20

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

console = Console()
lock    = threading.Lock()

# ═══════════════════════════════════════════════════════════════
# DATA STORE
# ═══════════════════════════════════════════════════════════════
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
        CREATE INDEX IF NOT EXISTS i1 ON signals(status);
        CREATE INDEX IF NOT EXISTS i2 ON signals(sym);
        """)
        for f in ["f_ema","f_rsi","f_macd","f_sweep","f_ob","f_fvg","f_struct","f_cot","f_news"]:
            c.execute("INSERT OR IGNORE INTO weights(feature,mult) VALUES(?,1.0)",(f,))
        c.commit()

db_init()

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
# SCORING ENGINE
# ═══════════════════════════════════════════════════════════════
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

    sl_=sr_=0
    rl=[]  # long reasons
    rs=[]  # short reasons
    fl={"f_ema":0,"f_rsi":0,"f_macd":0,"f_sweep":0,
        "f_ob":0,"f_fvg":0,"f_struct":0,"f_cot":0,"f_news":0}

    # EMA stack
    if e9 and e20 and e50:
        if e9[-1]>e20[-1]>e50[-1]:
            sl_+=2; fl["f_ema"]=1
            rl.append(f"EMA 9>20>50 bullish stack ({e9[-1]:.5f})")
        elif e9[-1]<e20[-1]<e50[-1]:
            sr_+=2; fl["f_ema"]=1
            rs.append(f"EMA 9<20<50 bearish stack")
    if e200:
        if price>e200[-1]: sl_+=1; rl.append(f"Above EMA200 — macro bullish")
        else:              sr_+=1; rs.append(f"Below EMA200 — macro bearish")

    # RSI
    if rv is not None:
        fl["f_rsi"]=1 if rv<35 or rv>65 else 0
        if rv<30:   sl_+=3; rl.append(f"RSI extreme oversold ({rv})")
        elif rv<40: sl_+=2; rl.append(f"RSI oversold ({rv})")
        elif rv>70: sr_+=3; rs.append(f"RSI extreme overbought ({rv})")
        elif rv>60: sr_+=2; rs.append(f"RSI overbought ({rv})")
        else:
            sl_+=1; sr_+=1
            rl.append(f"RSI neutral ({rv}) — room to run")
            rs.append(f"RSI neutral ({rv}) — room to run")

    # MACD
    if mh is not None:
        fl["f_macd"]=1 if abs(mh)>0 else 0
        if mh>0 and ml>ms: sl_+=2; rl.append(f"MACD bullish crossover (hist {mh:+.5f})")
        elif mh<0 and ml<ms: sr_+=2; rs.append(f"MACD bearish crossover (hist {mh:+.5f})")
        elif mh>0: sl_+=1; rl.append(f"MACD histogram positive ({mh:+.5f})")
        elif mh<0: sr_+=1; rs.append(f"MACD histogram negative ({mh:+.5f})")

    # VWAP
    if vw:
        dev=(price-vw)/vw*100
        if price>vw*1.001: sl_+=1; rl.append(f"Above VWAP {dev:+.2f}% — institutional bid")
        elif price<vw*0.999: sr_+=1; rs.append(f"Below VWAP {dev:+.2f}% — sell pressure")

    # Bollinger
    if bbl and bbh:
        if price<=bbl*1.001: sl_+=2; rl.append(f"At lower Bollinger ({bbl:.5f}) — mean reversion")
        elif price>=bbh*0.999: sr_+=2; rs.append(f"At upper Bollinger ({bbh:.5f}) — mean reversion")

    # Liquidity sweep
    if sw:
        fl["f_sweep"]=1
        if sw[0]=="BULL": sl_+=3; rl.append(f"BULLISH LIQUIDITY SWEEP below {sw[1]:.5f} — stops taken")
        else:             sr_+=3; rs.append(f"BEARISH LIQUIDITY SWEEP above {sw[1]:.5f} — stops taken")

    # Order block
    if bOB and abs(price-(bOB[0]+bOB[1])/2)/(bOB[0]+bOB[1])*2<0.015:
        fl["f_ob"]=1; sl_+=2; rl.append(f"At BULLISH ORDER BLOCK {bOB[0]:.5f}–{bOB[1]:.5f}")
    if beOB and abs(price-(beOB[0]+beOB[1])/2)/(beOB[0]+beOB[1])*2<0.015:
        fl["f_ob"]=1; sr_+=2; rs.append(f"At BEARISH ORDER BLOCK {beOB[0]:.5f}–{beOB[1]:.5f}")

    # FVG
    if bFVG and bFVG[0]<=price<=bFVG[1]:
        fl["f_fvg"]=1; sl_+=2; rl.append(f"Inside BULLISH FVG {bFVG[0]:.5f}–{bFVG[1]:.5f}")
    if beFVG and beFVG[1]<=price<=beFVG[0]:
        fl["f_fvg"]=1; sr_+=2; rs.append(f"Inside BEARISH FVG {beFVG[1]:.5f}–{beFVG[0]:.5f}")

    # Market structure
    if st=="BULL": fl["f_struct"]=1; sl_+=2; rl.append("HH+HL bullish structure — trend continuation")
    elif st=="BEAR": fl["f_struct"]=1; sr_+=2; rs.append("LH+LL bearish structure — trend continuation")

    # Volume spike
    if vol_sp:
        sl_+=1; sr_+=1
        rl.append(f"Volume spike {vo[-1]/avg_v:.1f}x avg — institutional activity")
        rs.append(f"Volume spike {vo[-1]/avg_v:.1f}x avg — institutional activity")

    # COT
    cot=cot_cache.get(sym,{})
    if cot:
        cs=cot.get("cot_score",0)
        if cs>0:  fl["f_cot"]=1; sl_+=cs; rl.append(f"COT bullish ({cot.get('bias','')}, {cot.get('pct_rank',50):.0f}th pct)")
        elif cs<0: fl["f_cot"]=1; sr_+=abs(cs); rs.append(f"COT bearish ({cot.get('bias','')}, {cot.get('pct_rank',50):.0f}th pct)")
        ct=cot.get("contrarian")
        if ct:
            if ct=="LONG":  fl["f_cot"]=1; sl_+=2; rl.append(f"COT CONTRARIAN LONG — specs at extreme short")
            elif ct=="SHORT": fl["f_cot"]=1; sr_+=2; rs.append(f"COT CONTRARIAN SHORT — specs at extreme long")

    # News sentiment
    news_score=0
    news_rl=[]; news_rs=[]
    if news_items:
        for n in news_items:
            t=(n.get("headline","")+n.get("summary","")).lower()
            b=sum(1 for w in BULL_W if w in t)
            be=sum(1 for w in BEAR_W if w in t)
            news_score+=(b-be)
        if news_score>=2:
            fl["f_news"]=1; sl_+=min(news_score,3)
            news_rl=[f"NEWS BULLISH (score +{news_score}): {news_items[0].get('headline','')[:70]}"]
        elif news_score<=-2:
            fl["f_news"]=1; sr_+=min(abs(news_score),3)
            news_rs=[f"NEWS BEARISH (score {news_score}): {news_items[0].get('headline','')[:70]}"]

    # Apply adaptive weights
    for feat,val in fl.items():
        if val:
            w=adap_weights.get(feat,1.0)
            if sl_>=sr_: sl_+=(w-1.0)
            else:        sr_+=(w-1.0)

    if sl_>=sr_:
        direction="LONG"; score=sl_; reasons=rl+news_rl
    else:
        direction="SHORT"; score=sr_; reasons=rs+news_rs

    if   score>=10: quality="A+"
    elif score>=8:  quality="A"
    elif score>=6:  quality="B+"
    else: return None

    if not av or av==0: return None

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
    if rr<2.5: return None

    return {
        "sym":sym,"quality":quality,"direction":direction,
        "price":price,"el":el,"eh":eh,"sl":sl,
        "tp1":tp1,"tp2":tp2,"tp3":tp3,"rr":rr,
        "score":round(score,1),"reasons":reasons,
        "flags":fl,"rsi":rv,"atr":av,"cot":cot,
        "news_score":news_score,
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
def load_yfinance():
    """Fetch quotes + candles for all YF symbols."""
    tickers=list(YF_SYMBOLS.values())
    try:
        # quotes (latest 2 days hourly)
        raw=yf.download(" ".join(tickers),period="5d",interval="1h",
                        group_by="ticker",auto_adjust=True,progress=False,threads=True)
    except: return
    for name,ticker in YF_SYMBOLS.items():
        try:
            df=(raw[ticker] if len(tickers)>1 and ticker in raw.columns.get_level_values(0)
                else raw if len(tickers)==1 else None)
            if df is None or df.empty: continue
            df=df.dropna(subset=["Close"])
            if df.empty: continue
            row=df.iloc[-1]; prev=df.iloc[-2]["Close"] if len(df)>1 else row["Close"]
            candles=[]
            for ts,r in df.iterrows():
                t=int(ts.timestamp()) if hasattr(ts,"timestamp") else 0
                candles.append((t,float(r["Open"]),float(r["High"]),
                                float(r["Low"]),float(r["Close"]),float(r.get("Volume",0) or 0)))
            with lock:
                md=market[name]
                md.price=float(row["Close"]); md.prev=float(prev)
                md.high=float(row["High"]);   md.low=float(row["Low"])
                md.volume=float(row.get("Volume",0) or 0)
                md.candles=candles; md.updated=datetime.now().strftime("%H:%M:%S")
        except: pass

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
    global news_cache, cat_news
    items=[]
    for cat in ["general","crypto","forex","merger"]:
        try:
            r=requests.get(f"{BASE_URL}/news",params={"category":cat,"token":API_KEY},timeout=5)
            d=r.json()
            if isinstance(d,list): items.extend(d[:10])
        except: pass
    # company news
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
    new_cat={}
    for sym in list(market.keys()):
        kws=NEWS_KW.get(sym,[])
        if not kws: continue
        matched=[]
        for x in unique:
            txt=(x.get("headline","")+x.get("summary","")).lower()
            if any(k in txt for k in kws):
                ts=x.get("datetime",0)
                age=(time.time()-ts)/3600 if ts else 99
                matched.append({**x,"age_h":round(age,1)})
        matched.sort(key=lambda x:x["age_h"])
        if matched: new_cat[sym]=matched[:4]
    with lock:
        news_cache=unique[:25]; cat_news=new_cat

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
    with db() as c:
        c.execute("""INSERT INTO signals(sym,quality,direction,entry,sl,tp1,tp2,tp3,
            rr_t,score,f_ema,f_rsi,f_macd,f_sweep,f_ob,f_fvg,f_struct,f_cot,f_news,
            status,created)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?)""",
            (s["sym"],s["quality"],s["direction"],s["price"],s["sl"],
             s["tp1"],s["tp2"],s["tp3"],s["rr"],s["score"],
             fl["f_ema"],fl["f_rsi"],fl["f_macd"],fl["f_sweep"],
             fl["f_ob"],fl["f_fvg"],fl["f_struct"],fl["f_cot"],fl["f_news"],
             datetime.utcnow().isoformat(timespec="seconds")))
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
        with lock:
            stats_cache={"total":total,"wins":len(wins),"wr":wr,"avg_rr":avg_rr,"pf":pf,
                         "tp3":sum(1 for r in closed if r["status"]=="TP3"),
                         "tp2":sum(1 for r in closed if r["status"]=="TP2"),
                         "tp1":sum(1 for r in closed if r["status"]=="TP1"),
                         "sl":sum(1 for r in closed if r["status"]=="SL"),
                         "by_q":bq,"fstats":fstats,
                         "recent":[dict(r) for r in recent]}
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
    c={"A+":"bold bright_yellow","A":"bold green","B+":"bold cyan"}.get(q,"white")
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
            "[bold bright_yellow]TARAMA DEVAM EDİYOR...[/bold bright_yellow]\n"
            "[dim]A+ / A / B+  ·  Min R:R 1:2.5  ·  Her 30 saniyede güncellenir[/dim]"),
            title="[bold bright_yellow]● AKTİF SETUPLАР[/bold bright_yellow]",
            border_style="bright_yellow",box=box.HEAVY)
    t=Table(title="[bold bright_yellow]● AKTİF SETUPLАР — A+ / A / B+[/bold bright_yellow]",
            box=box.SIMPLE_HEAVY,border_style="bright_yellow",
            header_style="bold bright_yellow",show_lines=True)
    t.add_column("GRADE", width=6, justify="center")
    t.add_column("SYMBOL",width=12,style="bold white")
    t.add_column("DIR",   width=9, justify="center")
    t.add_column("FİYAT", width=13,justify="right")
    t.add_column("GİRİŞ ZONU", width=24,justify="right")
    t.add_column("STOP", width=13,justify="right",style="bright_red")
    t.add_column("TP1",  width=13,justify="right",style="green")
    t.add_column("TP2",  width=13,justify="right",style="bright_green")
    t.add_column("TP3",  width=13,justify="right",style="bright_yellow")
    t.add_column("R:R",  width=7, justify="center")
    t.add_column("RSI",  width=6, justify="center")
    t.add_column("SKOR", width=6, justify="center",style="dim")
    t.add_column("SAAT", width=7)
    for s in ss:
        rv=s.get("rsi"); rc="bright_green" if rv and rv<40 else "bright_red" if rv and rv>65 else "white"
        t.add_row(
            qc(s["quality"]), s["sym"], dc(s["direction"]),
            f"[bright_white]{fp(s['price'])}[/bright_white]",
            f"{fp(s['el'])} – {fp(s['eh'])}",
            fp(s["sl"]),fp(s["tp1"]),fp(s["tp2"]),fp(s["tp3"]),
            f"[bold]1:{s['rr']}[/bold]",
            f"[{rc}]{rv:.0f}[/{rc}]" if rv else "—",
            str(s["score"]), s["time"])
    return Panel(t,border_style="bright_yellow",box=box.HEAVY)

def panel_details():
    ss=list(setups)[:3]
    if not ss:
        return Panel("[dim]Setup oluşunca burada detay görünür.[/dim]",
                     title="[bold bright_yellow]● SETUP DETAY[/bold bright_yellow]",
                     border_style="bright_yellow",box=box.ROUNDED)
    panels=[]
    for s in ss:
        tech="\n".join(f"  [bright_green]{i+1:02d}.[/bright_green] {r}" for i,r in enumerate(s["reasons"]))
        cot=s.get("cot",{})
        cot_txt=""
        if cot:
            pr=cot.get("pct_rank",50)
            cot_txt=(f"\n[bold dim]── COT (CFTC) ──[/bold dim]\n"
                     f"  Tarih: {cot.get('date','—')}  |  {bias_c(cot.get('bias',''))}  |  {pbar(pr)} {pr:.0f}%\n"
                     f"  Spec net: {cot.get('spec_net',0):+,}  (WoW: {cot.get('spec_chg',0):+,})\n"
                     f"  Comm net: {cot.get('comm_net',0):+,}  |  OI: {cot.get('oi',0):,}\n")
            if cot.get("contrarian"):
                cot_txt+=f"  [bold bright_yellow]⚠ CONTRARIAN {cot['contrarian']} — specs aşırı pozisyonda[/bold bright_yellow]\n"
        narr="\n".join(f"  {l}" for l in s["narrative"].split("\n"))
        content=(
            f"{qc(s['quality'])}  {dc(s['direction'])}  [bold white]{s['sym']}[/bold white]  "
            f"[dim]Skor:{s['score']} RSI:{s.get('rsi','—')} ATR:{fp(s.get('atr'))}[/dim]\n\n"
            f"[bold dim]── TEKNİK CONFLUENCE ──[/bold dim]\n{tech}"
            f"{cot_txt}"
            f"\n[bold dim]── GİRİŞ GEREKÇESİ ──[/bold dim]\n{narr}")
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
    return Panel("\n".join(lines),
                 title="[bold bright_cyan]● PERFORMANS & ADAPTİF ÖĞRENME[/bold bright_cyan]",
                 border_style="bright_cyan",box=box.ROUNDED,
                 subtitle=f"[dim]{DB_PATH}[/dim]")

def panel_news():
    with lock: items=list(news_cache)
    t=Table(box=box.SIMPLE,show_header=True,header_style="bold bright_blue",padding=(0,1))
    t.add_column("DUYGU",   width=7,  justify="center")
    t.add_column("YAŞ",     width=5,  justify="right",style="dim")
    t.add_column("KAYNAK",  width=12, style="dim")
    t.add_column("BAŞLIK",  style="white")
    for x in items[:10]:
        txt=(x.get("headline","")+x.get("summary","")).lower()
        b=sum(1 for w in BULL_W if w in txt)
        be=sum(1 for w in BEAR_W if w in txt)
        net=b-be
        lbl="BULL" if net>=2 else "BEAR" if net<=-2 else "NÖTR"
        lc="bright_green" if lbl=="BULL" else "bright_red" if lbl=="BEAR" else "dim"
        ts=x.get("datetime",0); age=(time.time()-ts)/3600 if ts else 0
        age_s=f"{age:.0f}s" if age<24 else f"{age/24:.0f}g"
        t.add_row(f"[{lc}]{lbl}[/{lc}]",age_s,x.get("source","")[:12],x.get("headline","")[:95])
    if not items: t.add_row("[dim]—[/dim]","—","—","[dim]Haberler yükleniyor...[/dim]")
    return Panel(t,title="[bold]● CANLI HABER & DUYGU[/bold]",border_style="bright_blue",box=box.ROUNDED)

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
# RENDER
# ═══════════════════════════════════════════════════════════════
def render():
    run_analysis()
    lo=Layout()
    lo.split_column(
        Layout(name="h",  size=3),
        Layout(name="m",  size=22),
        Layout(name="s",  size=17),
        Layout(name="d",  size=28),
        Layout(name="cot",size=15),
        Layout(name="j",  size=16),
        Layout(name="st", size=18),
        Layout(name="bot",size=14),
    )
    lo["h"].update(panel_header())
    lo["m"].update(panel_market())
    lo["s"].update(panel_setups())
    lo["d"].update(panel_details())
    lo["cot"].update(panel_cot())
    lo["j"].update(panel_journal())
    lo["st"].update(panel_stats())
    lo["bot"].split_row(Layout(panel_news(),ratio=3),Layout(panel_risk(),ratio=1))
    return lo

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    console.print(Panel.fit(
        "[bold bright_yellow]TITAN FLOW[/bold bright_yellow] — Başlatılıyor...\n"
        "[dim]WebSocket (crypto) · yfinance (forex/metals/oil) · Finnhub (hisseler) · COT · Haberler · Journal[/dim]",
        border_style="bright_yellow"))
    threading.Thread(target=ws_loop,       daemon=True).start()
    threading.Thread(target=background_loop,daemon=True).start()
    threading.Thread(target=cot_loop,       daemon=True).start()
    threading.Thread(target=monitor_loop,   daemon=True).start()
    threading.Thread(target=stats_loop,     daemon=True).start()
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
