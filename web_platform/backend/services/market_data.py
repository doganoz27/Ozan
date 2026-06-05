"""
TITAN PRIME — Market Data Service
Fetches from Finnhub REST + yfinance for all symbols.
"""
import asyncio
import time
from datetime import datetime
from typing import Optional

import httpx
import yfinance as yf
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.config import settings, EQ_SYMBOLS, YF_SYMBOLS, ALL_SYMBOLS
from core.database import async_session_factory, MarketData, NewsItem

# In-memory cache
_prices: dict = {}       # sym -> {price, prev, high, low, volume, change_pct, updated_at}
_candles: dict = {}      # sym -> list of (t, o, h, l, c, v)
_news: list = []         # analyzed news items


def get_price_cache() -> dict:
    return _prices


def get_candles(sym: str) -> list:
    return _candles.get(sym, [])


def get_news_cache() -> list:
    return _news


async def fetch_finnhub_quote(sym: str) -> Optional[dict]:
    """Fetch a single equity quote from Finnhub REST."""
    url = f"{settings.FINNHUB_BASE_URL}/quote"
    params = {"symbol": sym, "token": settings.FINNHUB_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url, params=params)
            q = r.json()
            if q.get("c"):
                return {
                    "sym": sym,
                    "price": float(q["c"]),
                    "prev": float(q["pc"]),
                    "high": float(q["h"]),
                    "low": float(q["l"]),
                    "volume": 0.0,
                    "change_pct": round((float(q["c"]) - float(q["pc"])) / float(q["pc"]) * 100, 3) if q["pc"] else 0.0,
                    "updated_at": datetime.utcnow().isoformat(),
                }
    except Exception:
        pass
    return None


async def fetch_finnhub_candles(sym: str, resolution: str = "60", bars: int = 200) -> list:
    """Fetch candles for equity from Finnhub."""
    url = f"{settings.FINNHUB_BASE_URL}/stock/candle"
    now = int(time.time())
    frm = now - bars * 3600
    params = {
        "symbol": sym,
        "resolution": resolution,
        "from": frm,
        "to": now,
        "token": settings.FINNHUB_API_KEY,
    }
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(url, params=params)
            d = r.json()
            if d.get("s") == "ok":
                return list(zip(d["t"], d["o"], d["h"], d["l"], d["c"], d["v"]))
    except Exception:
        pass
    return []


def _fetch_yf_sync(name: str, ticker: str) -> Optional[dict]:
    """Synchronous yfinance fetch (runs in executor)."""
    try:
        tk = yf.Ticker(ticker)
        df = tk.history(period="5d", interval="1h", auto_adjust=True)
        if df is None or df.empty:
            return None
        df = df.dropna(subset=["Close"])
        if df.empty:
            return None
        row = df.iloc[-1]
        prev = float(df.iloc[-2]["Close"]) if len(df) > 1 else float(row["Close"])
        candles = []
        for ts, r in df.iterrows():
            t = int(ts.timestamp()) if hasattr(ts, "timestamp") else 0
            candles.append((
                t,
                float(r["Open"]),
                float(r["High"]),
                float(r["Low"]),
                float(r["Close"]),
                float(r.get("Volume", 0) or 0),
            ))
        price = float(row["Close"])
        chg = round((price - prev) / prev * 100, 3) if prev else 0.0
        return {
            "sym": name,
            "price": price,
            "prev": prev,
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "volume": float(row.get("Volume", 0) or 0),
            "change_pct": chg,
            "updated_at": datetime.utcnow().isoformat(),
            "candles": candles,
        }
    except Exception:
        return None


async def fetch_yfinance_candles(sym: str, period: str = "5d", interval: str = "1h") -> list:
    """Async wrapper around yfinance candle fetch."""
    ticker = YF_SYMBOLS.get(sym)
    if not ticker:
        return []
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _fetch_yf_sync, sym, ticker)
    if result:
        return result.get("candles", [])
    return []


async def fetch_finnhub_news() -> list:
    """Fetch market news from Finnhub categories."""
    items = []
    categories = ["general", "forex", "merger"]
    async with httpx.AsyncClient(timeout=8.0) as client:
        for cat in categories:
            try:
                r = await client.get(
                    f"{settings.FINNHUB_BASE_URL}/news",
                    params={"category": cat, "token": settings.FINNHUB_API_KEY},
                )
                d = r.json()
                if isinstance(d, list):
                    items.extend(d[:8])
            except Exception:
                pass
        # Company news for key stocks
        for sym in ["NVDA", "AAPL", "MSFT", "TSLA"]:
            try:
                today = datetime.utcnow().strftime("%Y-%m-%d")
                frm = datetime.utcfromtimestamp(time.time() - 7 * 86400).strftime("%Y-%m-%d")
                r = await client.get(
                    f"{settings.FINNHUB_BASE_URL}/company-news",
                    params={"symbol": sym, "from": frm, "to": today, "token": settings.FINNHUB_API_KEY},
                )
                d = r.json()
                if isinstance(d, list):
                    items.extend(d[:5])
            except Exception:
                pass
    # Deduplicate
    seen: set = set()
    unique = []
    for x in items:
        h = x.get("headline", "")
        if h and h not in seen:
            seen.add(h)
            unique.append(x)
    return unique[:30]


def _analyze_news_item(article: dict) -> dict:
    """Enrich a news article with importance, bias, bull/bear pct."""
    HIGH_IMP_KW = {
        "fomc": 90, "federal reserve": 82, "rate decision": 88, "rate hike": 85, "rate cut": 85,
        "emergency meeting": 92, "powell": 60, "lagarde": 60,
        "ecb decision": 88, "boe decision": 88, "boj decision": 88,
        "cpi": 80, "core inflation": 82, "pce": 80, "non-farm payroll": 88, "nfp": 88,
        "unemployment rate": 75, "gdp": 72, "recession": 78,
        "war": 82, "nuclear": 90, "invasion": 88, "sanctions": 75,
        "default": 88, "bank run": 90, "flash crash": 88,
        "earnings miss": 52, "earnings beat": 50, "guidance cut": 55,
    }
    MED_IMP_KW = {
        "interest rate": 48, "monetary policy": 50, "inflation": 58,
        "ppi": 52, "retail sales": 48, "pmi": 42,
        "jobless claims": 50, "trade balance": 42,
        "tariff": 60, "trade war": 68, "conflict": 52,
        "regulatory": 40, "bankruptcy": 62, "collapse": 70,
        "crisis": 65, "contagion": 70,
    }
    BULL_W = ["surge", "rally", "soar", "gain", "jump", "rise", "breakout", "bullish", "beat",
              "upgrade", "buy", "inflow", "record", "growth", "recovery", "rebound", "dovish",
              "stimulus", "profit", "approval", "deal", "expansion", "rate cut", "fed pivot"]
    BEAR_W = ["drop", "fall", "crash", "plunge", "decline", "selloff", "bearish", "miss",
              "downgrade", "sell", "outflow", "ban", "hack", "lawsuit", "fine", "hawkish",
              "inflation", "recession", "default", "war", "tariff", "loss", "rate hike", "risk-off"]

    text = (article.get("headline", "") + " " + article.get("summary", "")).lower()

    high_score = max((pts for kw, pts in HIGH_IMP_KW.items() if kw in text), default=0)
    med_score = max((pts for kw, pts in MED_IMP_KW.items() if kw in text), default=0)
    importance = min(high_score or med_score, 100)

    rl = ("CRITICAL" if importance >= 80 else "HIGH" if importance >= 60
          else "MEDIUM" if importance >= 40 else "LOW" if importance >= 20 else "NOISE")

    bull_words = sum(1 for w in BULL_W if w in text)
    bear_words = sum(1 for w in BEAR_W if w in text)
    total_w = bull_words + bear_words + 1
    bull_pct = round(bull_words / total_w * 100)
    bear_pct = round(bear_words / total_w * 100)
    if bull_pct > bear_pct + 15:
        bias = "BULLISH"
    elif bear_pct > bull_pct + 15:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"

    return {
        **article,
        "importance": importance,
        "risk_level": rl,
        "bias": bias,
        "bull_pct": bull_pct,
        "bear_pct": bear_pct,
    }


async def refresh_all_market_data() -> None:
    """
    Main refresh function called every 30s.
    Fetches quotes + candles for all symbols and updates DB.
    """
    global _prices, _candles, _news

    loop = asyncio.get_event_loop()
    tasks = []

    # YF symbols — run in executor pool
    async def _yf_one(name: str, ticker: str):
        result = await loop.run_in_executor(None, _fetch_yf_sync, name, ticker)
        if result:
            _prices[name] = {k: v for k, v in result.items() if k != "candles"}
            if result.get("candles"):
                _candles[name] = result["candles"]

    for name, ticker in YF_SYMBOLS.items():
        tasks.append(_yf_one(name, ticker))

    # Finnhub equity quotes
    async def _fh_equity(sym: str):
        q = await fetch_finnhub_quote(sym)
        if q:
            _prices[sym] = q
        # Also fetch candles for equity
        c = await fetch_finnhub_candles(sym)
        if c:
            _candles[sym] = c
            if c and not _prices.get(sym):
                _prices[sym] = {
                    "sym": sym,
                    "price": c[-1][4],
                    "change_pct": 0.0,
                    "updated_at": datetime.utcnow().isoformat(),
                }

    for sym in EQ_SYMBOLS:
        tasks.append(_fh_equity(sym))

    await asyncio.gather(*tasks, return_exceptions=True)

    # Persist to DB
    async with async_session_factory() as session:
        for sym, data in _prices.items():
            if not data.get("price"):
                continue
            stmt = pg_insert(MarketData).values(
                sym=sym,
                price=data.get("price"),
                change_pct=data.get("change_pct", 0),
                high=data.get("high"),
                low=data.get("low"),
                volume=data.get("volume"),
                updated_at=datetime.utcnow(),
            ).on_conflict_do_update(
                index_elements=["sym"],
                set_={
                    "price": data.get("price"),
                    "change_pct": data.get("change_pct", 0),
                    "high": data.get("high"),
                    "low": data.get("low"),
                    "volume": data.get("volume"),
                    "updated_at": datetime.utcnow(),
                },
            )
            await session.execute(stmt)
        await session.commit()


async def refresh_news() -> None:
    """Fetch and analyze news, persist to DB."""
    global _news
    raw = await fetch_finnhub_news()
    analyzed = [_analyze_news_item(a) for a in raw]
    analyzed.sort(key=lambda x: -x.get("importance", 0))
    _news = analyzed[:30]

    # Persist important news to DB
    async with async_session_factory() as session:
        for article in analyzed:
            if article.get("importance", 0) < 40:
                continue
            import json
            session.add(NewsItem(
                headline=article.get("headline", "")[:500],
                summary=(article.get("summary", "") or "")[:1000],
                importance=article.get("importance", 0),
                risk_level=article.get("risk_level"),
                bias=article.get("bias"),
                bull_pct=article.get("bull_pct", 0),
                bear_pct=article.get("bear_pct", 0),
                affected_assets=json.dumps(list(article.get("sym_impacts", {}).keys())[:10]),
                published_at=datetime.utcfromtimestamp(article["datetime"]) if article.get("datetime") else None,
            ))
        try:
            await session.commit()
        except Exception:
            await session.rollback()
