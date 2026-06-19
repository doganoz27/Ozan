"""
TITAN PRIME — Signal Engine
Ported from titan_flow.py with async support.
"""
import math
from datetime import datetime
from typing import Optional

from core.config import settings, get_leverage, get_asset_class, EQ_SYMBOLS

# ── Scoring constants ─────────────────────────────────────────────────────────
MAX_RAW = 96
BULL_W = [
    "surge", "rally", "soar", "gain", "jump", "rise", "breakout", "bullish", "beat",
    "upgrade", "buy", "inflow", "record", "growth", "recovery", "rebound", "dovish",
    "stimulus", "profit", "approval", "deal", "expansion", "rate cut", "fed pivot",
]
BEAR_W = [
    "drop", "fall", "crash", "plunge", "decline", "selloff", "bearish", "miss",
    "downgrade", "sell", "outflow", "ban", "hack", "lawsuit", "fine", "hawkish",
    "inflation", "recession", "default", "war", "tariff", "loss", "rate hike", "risk-off",
]

# Default adaptive weights (overridden from DB at runtime)
_adap_weights: dict = {
    "f_ema": 1.0, "f_rsi": 1.0, "f_macd": 1.0, "f_sweep": 1.0,
    "f_ob": 1.0, "f_fvg": 1.0, "f_struct": 1.0,
}


def set_adaptive_weights(weights: dict) -> None:
    """Called at startup and periodically from DB."""
    _adap_weights.update(weights)


# ── Technical Indicators ──────────────────────────────────────────────────────

def _ema(prices: list[float], n: int) -> list[float]:
    if len(prices) < n:
        return []
    k = 2 / (n + 1)
    r = [sum(prices[:n]) / n]
    for x in prices[n:]:
        r.append(x * k + r[-1] * (1 - k))
    return r


def _rsi(prices: list[float], n: int = 14) -> Optional[float]:
    if len(prices) < n + 1:
        return None
    d = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
    ag = sum(max(x, 0) for x in d[:n]) / n
    al = sum(max(-x, 0) for x in d[:n]) / n
    for x in d[n:]:
        ag = (ag * (n - 1) + max(x, 0)) / n
        al = (al * (n - 1) + max(-x, 0)) / n
    return round(100 - 100 / (1 + ag / al), 1) if al else 100.0


def _macd(prices: list[float]):
    if len(prices) < 35:
        return None, None, None
    ef = _ema(prices, 12)
    es = _ema(prices, 26)
    n = min(len(ef), len(es))
    ml = [ef[-n + i] - es[-n + i] for i in range(n)]
    sg = _ema(ml, 9)
    if not sg:
        return None, None, None
    h = ml[-1] - sg[-1]
    return round(ml[-1], 6), round(sg[-1], 6), round(h, 6)


def _atr(candles: list, n: int = 14) -> Optional[float]:
    if len(candles) < n + 1:
        return None
    trs = [
        max(
            candles[i][2] - candles[i][3],
            abs(candles[i][2] - candles[i - 1][4]),
            abs(candles[i][3] - candles[i - 1][4]),
        )
        for i in range(1, len(candles))
    ]
    return round(sum(trs[-n:]) / n, 8) if len(trs) >= n else None


def _vwap(candles: list) -> Optional[float]:
    if not candles:
        return None
    tv = sum(((c[2] + c[3] + c[4]) / 3) * c[5] for c in candles)
    v = sum(c[5] for c in candles)
    return tv / v if v else None


def _bbands(prices: list[float], n: int = 20):
    if len(prices) < n:
        return None, None, None
    from statistics import stdev as _std
    mid = sum(prices[-n:]) / n
    s = _std(prices[-n:])
    return round(mid - 2 * s, 8), round(mid, 8), round(mid + 2 * s, 8)


def _swing_lows(candles: list, lb: int = 5) -> list[float]:
    lows = []
    c = [x[3] for x in candles]
    for i in range(lb, len(c) - lb):
        if c[i] == min(c[i - lb: i + lb + 1]):
            lows.append(c[i])
    return lows


def _swing_highs(candles: list, lb: int = 5) -> list[float]:
    highs = []
    c = [x[2] for x in candles]
    for i in range(lb, len(c) - lb):
        if c[i] == max(c[i - lb: i + lb + 1]):
            highs.append(c[i])
    return highs


def _structural_sl(candles: list, direction: str, price: float, av: float) -> Optional[float]:
    if len(candles) < 20 or not av:
        return None
    recent = candles[-40:]
    if direction == "LONG":
        sl_levels = _swing_lows(recent, lb=3)
        candidates = [s for s in sl_levels if s < price - av * 0.1]
        if candidates:
            swing = max(candidates)
            sl = swing - av * 0.25
        else:
            sl = price - av * 2.0
        sl = max(sl, price - av * 3.0)
    else:
        sl_levels = _swing_highs(recent, lb=3)
        candidates = [s for s in sl_levels if s > price + av * 0.1]
        if candidates:
            swing = min(candidates)
            sl = swing + av * 0.25
        else:
            sl = price + av * 2.0
        sl = min(sl, price + av * 3.0)
    return round(sl, 8)


def _structural_tp(candles: list, direction: str, price: float, sl: float, min_rr: float = 1.8):
    if not sl:
        return None, 0
    risk = abs(price - sl)
    if risk == 0:
        return None, 0
    min_dist = risk * min_rr
    recent = candles[-80:]
    if direction == "LONG":
        resistances = _swing_highs(recent, lb=3)
        targets = [r for r in resistances if r > price + min_dist]
        if targets:
            tp = min(targets) * 0.9992
        else:
            tp = price + max(min_dist, abs(price - sl) * 3.0)
    else:
        supports = _swing_lows(recent, lb=3)
        targets = [s for s in supports if s < price - min_dist]
        if targets:
            tp = max(targets) * 1.0008
        else:
            tp = price - max(min_dist, abs(price - sl) * 3.0)
    actual_rr = round(abs(tp - price) / risk, 2) if risk else 0
    return round(tp, 8), actual_rr


def _sweep(candles: list, lb: int = 20):
    if len(candles) < lb + 2:
        return None
    rec = candles[-lb - 2:-2]
    last = candles[-1]
    sh = max(c[2] for c in rec)
    sl = min(c[3] for c in rec)
    if last[2] > sh and last[4] < sh:
        return ("BEAR", sh)
    if last[3] < sl and last[4] > sl:
        return ("BULL", sl)
    return None


def _order_block(candles: list):
    if len(candles) < 10:
        return None, None
    rec = candles[-20:]
    bull = bear = None
    for i in range(len(rec) - 3):
        c = rec[i]
        nx = rec[i + 1: i + 4]
        if c[4] < c[1] and any(n[4] > c[2] for n in nx):
            bull = (c[3], c[2])
        if c[4] > c[1] and any(n[4] < c[3] for n in nx):
            bear = (c[3], c[2])
    return bull, bear


def _fvg(candles: list):
    if len(candles) < 3:
        return None, None
    fb = fb2 = None
    for i in range(len(candles) - 2):
        c1, _, c3 = candles[i], candles[i + 1], candles[i + 2]
        if c1[2] < c3[3]:
            fb = (c1[2], c3[3])
        if c1[3] > c3[2]:
            fb2 = (c1[3], c3[2])
    return fb, fb2


def _structure(highs: list, lows: list, n: int = 8) -> str:
    if len(highs) < n:
        return "?"
    h = highs[-n:]
    l = lows[-n:]
    if h[-1] > h[-2] > h[-3] and l[-1] > l[-2] > l[-3]:
        return "BULL"
    if h[-1] < h[-2] < h[-3] and l[-1] < l[-2] < l[-3]:
        return "BEAR"
    return "RANGE"


# ── Narrative builder ─────────────────────────────────────────────────────────

def _build_narrative(
    sym, dirn, price, el, eh, sl, tp, rr, av, rv, mh, st, sw, bOB, beOB, bFVG, beFVG, news_lines, ns
) -> str:
    L = [f"WHY ENTER {dirn} on {sym}:", ""]
    ow = "upside" if dirn == "LONG" else "downside"
    if st == "BULL" and dirn == "LONG":
        L.append("► HH+HL structure — bulls in control, trend continuation.")
    elif st == "BEAR" and dirn == "SHORT":
        L.append("► LH+LL structure — bears in control, trend continuation.")
    else:
        L.append("► Ranging market — playing boundary extremes.")
    if sw:
        if sw[0] == "BULL" and dirn == "LONG":
            L.append(f"► Liquidity swept below {sw[1]:.5f} — retail stops taken, smart money absorbed.")
        elif sw[0] == "BEAR" and dirn == "SHORT":
            L.append(f"► Liquidity swept above {sw[1]:.5f} — retail longs stopped out, distribution complete.")
    if bOB and dirn == "LONG":
        L.append(f"► Bullish Order Block {bOB[0]:.5f}–{bOB[1]:.5f} — institutional buy zone defended.")
    if beOB and dirn == "SHORT":
        L.append(f"► Bearish Order Block {beOB[0]:.5f}–{beOB[1]:.5f} — institutional sell zone active.")
    if bFVG and dirn == "LONG":
        L.append(f"► Bullish FVG {bFVG[0]:.5f}–{bFVG[1]:.5f} — price filling imbalance.")
    if beFVG and dirn == "SHORT":
        L.append(f"► Bearish FVG {beFVG[1]:.5f}–{beFVG[0]:.5f} — distribution zone.")
    if rv:
        if rv < 30 and dirn == "LONG":
            L.append(f"► RSI {rv} — extreme oversold, statistical reversal edge maximum.")
        elif rv > 70 and dirn == "SHORT":
            L.append(f"► RSI {rv} — extreme overbought, institutional fade zone.")
        else:
            L.append(f"► RSI {rv} — neutral, room to extend {ow}.")
    if mh:
        if mh > 0 and dirn == "LONG":
            L.append(f"► MACD expanding positive — momentum accelerating {ow}.")
        elif mh < 0 and dirn == "SHORT":
            L.append(f"► MACD expanding negative — sellers in control.")
    if news_lines:
        L.extend(["", "MACRO/NEWS:"] + [f"  {n}" for n in news_lines[:2]])
    L += [
        "",
        "RISK MANAGEMENT:",
        f"  Entry   : {el:.5f} – {eh:.5f}",
        f"  Stop    : {sl:.5f}  (close beyond = immediate exit)",
        f"  TP      : {tp:.5f}  → full exit at target",
        f"  R:R     : 1:{rr}  |  Risk max 1-2% of capital",
    ]
    return "\n".join(L)


# ── Main scoring function ─────────────────────────────────────────────────────

def score_setup(
    sym: str,
    candles: list,
    price: float,
    news_items: Optional[list] = None,
    portfolio_heat: float = 0.0,
) -> Optional[dict]:
    """
    Score a trading setup using multi-factor institutional analysis.
    Returns a dict with all signal details, or None if rejected.
    """
    if len(candles) < 40:
        return None

    cl = [c[4] for c in candles]
    hi = [c[2] for c in candles]
    lo = [c[3] for c in candles]
    vo = [c[5] for c in candles]

    e9 = _ema(cl, 9)
    e20 = _ema(cl, 20)
    e50 = _ema(cl, 50)
    e200 = _ema(cl, 200) if len(cl) >= 200 else []
    rv = _rsi(cl[-50:])
    ml, ms, mh = _macd(cl)
    av = _atr(candles)
    vw = _vwap(candles[-24:])
    bbl, bbm, bbh = _bbands(cl)
    sw = _sweep(candles)
    bOB, beOB = _order_block(candles)
    bFVG, beFVG = _fvg(candles[-10:])
    st = _structure(hi, lo)
    avg_v = sum(vo[-20:]) / 20 if vo else 1
    vol_sp = vo[-1] > avg_v * 1.5 if vo else False

    if not av or av == 0:
        return None

    sl_ = sr_ = 0
    rl: list[str] = []
    rs: list[str] = []
    neg_l: list[str] = []
    neg_s: list[str] = []
    fl = {
        "f_ema": 0, "f_rsi": 0, "f_macd": 0, "f_sweep": 0,
        "f_ob": 0, "f_fvg": 0, "f_struct": 0,
    }
    sweep_confirmed = False

    # ── EMA Stack (0-11) ──────────────────────────────────────────
    if e9 and e20 and e50:
        if e9[-1] > e20[-1] > e50[-1]:
            sl_ += 8; fl["f_ema"] = 1; rl.append("EMA 9>20>50 bullish stack")
        elif e9[-1] < e20[-1] < e50[-1]:
            sr_ += 8; fl["f_ema"] = 1; rs.append("EMA 9<20<50 bearish stack")
        else:
            neg_l.append("EMA stack misaligned — no trend conviction")
            neg_s.append("EMA stack misaligned — no trend conviction")
    if e200:
        if price > e200[-1]:
            sl_ += 3; rl.append("Above EMA200 — macro bullish")
        else:
            sr_ += 3; rs.append("Below EMA200 — macro bearish")

    # ── RSI (0-10) ────────────────────────────────────────────────
    if rv is not None:
        fl["f_rsi"] = 1 if rv < 35 or rv > 65 else 0
        if rv < 30:
            sl_ += 10; rl.append(f"RSI extreme oversold ({rv})")
        elif rv < 40:
            sl_ += 7; rl.append(f"RSI oversold ({rv})")
        elif rv > 70:
            sr_ += 10; rs.append(f"RSI extreme overbought ({rv})")
        elif rv > 60:
            sr_ += 7; rs.append(f"RSI overbought ({rv})")
        else:
            sl_ += 3; sr_ += 3
            neg_l.append(f"RSI neutral ({rv}) — weak directional signal")
            neg_s.append(f"RSI neutral ({rv}) — weak directional signal")

    # ── MACD (0-8) ────────────────────────────────────────────────
    if mh is not None:
        fl["f_macd"] = 1 if abs(mh) > 0 else 0
        if mh > 0 and ml > ms:
            sl_ += 8; rl.append(f"MACD bullish crossover (hist {mh:+.5f})")
        elif mh < 0 and ml < ms:
            sr_ += 8; rs.append(f"MACD bearish crossover (hist {mh:+.5f})")
        elif mh > 0:
            sl_ += 4; rl.append(f"MACD histogram positive ({mh:+.5f})")
        elif mh < 0:
            sr_ += 4; rs.append(f"MACD histogram negative ({mh:+.5f})")

    # ── VWAP (0-4) ────────────────────────────────────────────────
    if vw:
        dev = (price - vw) / vw * 100
        if price > vw * 1.001:
            sl_ += 4; rl.append(f"Above VWAP {dev:+.2f}% — institutional bid")
        elif price < vw * 0.999:
            sr_ += 4; rs.append(f"Below VWAP {dev:+.2f}% — sell pressure")

    # ── Bollinger (0-5) ───────────────────────────────────────────
    if bbl and bbh:
        if price <= bbl * 1.001:
            sl_ += 5; rl.append(f"At lower Bollinger ({bbl:.5f}) — mean reversion")
        elif price >= bbh * 0.999:
            sr_ += 5; rs.append(f"At upper Bollinger ({bbh:.5f}) — mean reversion")

    # ── Liquidity Sweep (0-12) — required for A+ ──────────────────
    if sw:
        fl["f_sweep"] = 1; sweep_confirmed = True
        if sw[0] == "BULL":
            sl_ += 12; rl.append(f"BULLISH LIQUIDITY SWEEP below {sw[1]:.5f} — stops taken")
        else:
            sr_ += 12; rs.append(f"BEARISH LIQUIDITY SWEEP above {sw[1]:.5f} — stops taken")
    else:
        neg_l.append("No liquidity sweep confirmed")
        neg_s.append("No liquidity sweep confirmed")

    # ── Order Block (0-8) ─────────────────────────────────────────
    if bOB and abs(price - (bOB[0] + bOB[1]) / 2) / ((bOB[0] + bOB[1]) / 2) < 0.015:
        fl["f_ob"] = 1; sl_ += 8; rl.append(f"At BULLISH ORDER BLOCK {bOB[0]:.5f}–{bOB[1]:.5f}")
    if beOB and abs(price - (beOB[0] + beOB[1]) / 2) / ((beOB[0] + beOB[1]) / 2) < 0.015:
        fl["f_ob"] = 1; sr_ += 8; rs.append(f"At BEARISH ORDER BLOCK {beOB[0]:.5f}–{beOB[1]:.5f}")

    # ── FVG (0-6) ─────────────────────────────────────────────────
    if bFVG and bFVG[0] <= price <= bFVG[1]:
        fl["f_fvg"] = 1; sl_ += 6; rl.append(f"Inside BULLISH FVG {bFVG[0]:.5f}–{bFVG[1]:.5f}")
    if beFVG and beFVG[1] <= price <= beFVG[0]:
        fl["f_fvg"] = 1; sr_ += 6; rs.append(f"Inside BEARISH FVG {beFVG[1]:.5f}–{beFVG[0]:.5f}")

    # ── Market Structure (0-8) ────────────────────────────────────
    if st == "BULL":
        fl["f_struct"] = 1; sl_ += 8; rl.append("HH+HL bullish structure — trend continuation")
        neg_s.append("HTF structure is BULLISH — counter-trend short risk")
    elif st == "BEAR":
        fl["f_struct"] = 1; sr_ += 8; rs.append("LH+LL bearish structure — trend continuation")
        neg_l.append("HTF structure is BEARISH — counter-trend long risk")
    else:
        neg_l.append("Market structure unclear (RANGE) — no institutional bias")
        neg_s.append("Market structure unclear (RANGE) — no institutional bias")

    # ── Volume (0-4) ──────────────────────────────────────────────
    if vol_sp:
        sl_ += 4; sr_ += 4
        rl.append(f"Volume spike {vo[-1]/avg_v:.1f}x avg — institutional activity")
        rs.append(f"Volume spike {vo[-1]/avg_v:.1f}x avg — institutional activity")
    else:
        neg_l.append("No volume spike — weak institutional participation")
        neg_s.append("No volume spike — weak institutional participation")

    # ── Portfolio heat check ──────────────────────────────────────
    if portfolio_heat >= 15:
        return None

    # ── News Sentiment (0-10) ─────────────────────────────────────
    news_score = 0
    news_rl: list[str] = []
    news_rs: list[str] = []
    news_penalty = 0
    n_imp = 0

    if news_items:
        for n in news_items:
            t = (n.get("headline", "") + n.get("summary", "")).lower()
            b = sum(1 for w in BULL_W if w in t)
            be = sum(1 for w in BEAR_W if w in t)
            news_score += b - be
            if n.get("importance", 0) > n_imp:
                n_imp = n.get("importance", 0)
        if n_imp >= 80:
            news_penalty = 5
            neg_l.append(f"High volatility news (impact {n_imp}/100) — exercise caution")
            neg_s.append(f"High volatility news (impact {n_imp}/100) — exercise caution")
        elif n_imp >= 60:
            news_penalty = 2
        news_pts = min(abs(news_score) * 2, 10)
        if news_score >= 2:
            fl["f_news"] = 1 if "f_news" in fl else 1
            sl_ += news_pts
            news_rl = [f"NEWS BULLISH (score +{news_score}): {news_items[0].get('headline','')[:70]}"]
        elif news_score <= -2:
            fl["f_news"] = 1 if "f_news" in fl else 1
            sr_ += news_pts
            news_rs = [f"NEWS BEARISH (score {news_score}): {news_items[0].get('headline','')[:70]}"]
        else:
            neg_l.append("No strong news catalyst")
            neg_s.append("No strong news catalyst")

    # ── Adaptive weight bonus ─────────────────────────────────────
    for feat, val in fl.items():
        if val and feat in _adap_weights:
            w = _adap_weights[feat]
            if sl_ >= sr_:
                sl_ += (w - 1.0) * 2
            else:
                sr_ += (w - 1.0) * 2

    # ── Pick direction ────────────────────────────────────────────
    if sl_ >= sr_:
        direction = "LONG"; raw = sl_; reasons = rl + news_rl; neg_factors = neg_l
    else:
        direction = "SHORT"; raw = sr_; reasons = rs + news_rs; neg_factors = neg_s

    # ── Entry zone ────────────────────────────────────────────────
    if direction == "LONG":
        el = price - av * 0.2; eh = price + av * 0.1
    else:
        el = price - av * 0.1; eh = price + av * 0.2

    # ── Structural SL & TP ────────────────────────────────────────
    sl = _structural_sl(candles, direction, price, av)
    if sl is None:
        return None
    tp, rr = _structural_tp(candles, direction, price, sl, min_rr=1.8)
    if tp is None or rr < 1.8:
        return None

    # ── RR bonus ──────────────────────────────────────────────────
    rr_bonus = 16 if rr >= 3.5 else 14 if rr >= 3.0 else 12 if rr >= 2.5 else 9 if rr >= 2.0 else 6
    if rr >= 3.0:
        reasons.append(f"Excellent RR 1:{rr}")
    elif rr >= 2.5:
        reasons.append(f"Strong RR 1:{rr}")
    elif rr >= 2.0:
        reasons.append(f"Good RR 1:{rr}")
    else:
        neg_factors.append(f"RR 1:{rr} — minimum acceptable")

    # ── Equity bonus ──────────────────────────────────────────────
    eq_bonus = 0
    if get_asset_class(sym) in ("STOCKS",) and fl["f_ema"] and fl["f_struct"]:
        eq_bonus = 4

    # ── Normalize score ───────────────────────────────────────────
    score_100 = round(min(max(raw / MAX_RAW * 85 + rr_bonus + eq_bonus - news_penalty, 0), 100), 1)

    # ── Hold time ─────────────────────────────────────────────────
    hold_h = round(abs(tp - price) / av, 1) if av else 8.0
    time_bonus = 2 if hold_h <= 4 else 1 if hold_h <= 8 else 0 if hold_h <= 24 else -3
    score_100 = round(min(max(score_100 + time_bonus, 0), 100), 1)

    if hold_h > 24:
        neg_factors.append(f"Hold time ~{hold_h:.0f}h — capital locked overnight+")
    elif hold_h > 8:
        neg_factors.append(f"Hold time ~{hold_h:.0f}h — crosses session boundary")

    # ── Hard reject ───────────────────────────────────────────────
    if score_100 < 38:
        return None

    # ── Quality thresholds ────────────────────────────────────────
    if score_100 >= 95:
        quality = "A+"
    elif score_100 >= 90:
        quality = "A+"
    elif score_100 >= 80:
        quality = "A+"
    elif score_100 >= 66:
        quality = "A"
    elif score_100 >= 50:
        quality = "B+"
    elif score_100 >= 38:
        quality = "WATCHLIST"
    else:
        return None

    # A+ requires liquidity sweep
    if quality == "A+" and not sweep_confirmed:
        quality = "A"

    # Status determination
    if score_100 >= 58:
        status = "APPROVED"
    elif score_100 >= 38:
        status = "WATCHLIST"
    else:
        status = "REJECTED"

    confidence = score_100

    # ── Smart Money Analysis ──────────────────────────────────────
    sm_notes: list[str] = []
    trap_warnings: list[str] = []
    recent_hi = max(hi[-20:]) if hi else price
    recent_lo = min(lo[-20:]) if lo else price
    near_top = price > recent_hi * 0.995
    near_bot = price < recent_lo * 1.005

    if direction == "LONG" and near_bot and not sw:
        sm_notes.append("Price at recent low — possible stop-hunt zone or accumulation")
    if direction == "SHORT" and near_top and not sw:
        sm_notes.append("Price at recent high — possible liquidity grab or distribution")
    if sw:
        sm_notes.append(f"Liquidity sweep confirmed at {sw[1]:.5f} — smart money absorbed stops")
    if bOB:
        sm_notes.append(f"Institutional order block present {bOB[0]:.5f}–{bOB[1]:.5f}")
    if bFVG:
        sm_notes.append(f"Fair Value Gap imbalance {bFVG[0]:.5f}–{bFVG[1]:.5f} — likely to fill")

    if near_top and direction == "LONG" and not sw:
        trap_warnings.append("BREAKOUT TRAP: Buying at recent high without sweep — retail longs may be trapped")
    if near_bot and direction == "SHORT" and not sw:
        trap_warnings.append("BREAKDOWN TRAP: Shorting at recent low without sweep — retail shorts may be trapped")
    if not bOB and not beFVG and not sw:
        trap_warnings.append("No institutional confirmation — setup may lack smart money backing")

    # ── Contrarian Score (0-100) ──────────────────────────────────
    c_score = 0
    if sw:
        c_score += 25
    if (near_top and direction == "SHORT") or (near_bot and direction == "LONG"):
        c_score += 15
    if n_imp >= 60:
        c_score += 10
    if rv and (rv > 75 or rv < 25):
        c_score += 10
    c_score = min(c_score, 100)

    if c_score >= 70:
        c_label = "Contrarian Opportunity"
    elif c_score >= 40:
        c_label = "Neutral"
    else:
        c_label = "Follow the Trend"

    # ── Market Regime ─────────────────────────────────────────────
    if st == "BULL" and (rv or 50) < 65:
        regime = "Risk-On"
    elif st == "BEAR" and (rv or 50) > 35:
        regime = "Risk-Off"
    else:
        regime = "Neutral"

    # ── Consensus vs SM view ──────────────────────────────────────
    ema_bull = fl.get("f_ema", False) and direction == "LONG"
    ema_bear = fl.get("f_ema", False) and direction == "SHORT"
    consensus_bias = "Bullish" if ema_bull else ("Bearish" if ema_bear else "Neutral")
    sm_conf = any([fl.get("f_ob"), fl.get("f_fvg"), fl.get("f_sweep")])
    sm_bias = ("Bullish" if direction == "LONG" else "Bearish") if sm_conf else "Uncertain"
    contrast_txt = "Aligned" if consensus_bias == sm_bias else "Divergent — caution"
    consensus_view = f"Retail view: {consensus_bias} (EMA+trend followers)"
    sm_view = f"Smart Money: {sm_bias} (OB/sweep/FVG) — {contrast_txt}"

    news_risk_label = (
        "NO RISK" if n_imp < 20 else "LOW" if n_imp < 40
        else "MEDIUM" if n_imp < 60 else "HIGH" if n_imp < 80 else "CRITICAL"
    )

    duration = "Scalp" if hold_h <= 2 else "Intraday" if hold_h <= 12 else "Swing"

    # Build narrative
    narrative = _build_narrative(
        sym, direction, price, el, eh, sl, tp, rr, av, rv, mh, st, sw,
        bOB, beOB, bFVG, beFVG, news_rl + news_rs, news_score,
    )

    return {
        "sym": sym,
        "quality": quality,
        "direction": direction,
        "status": status,
        "price": price,
        "el": el,
        "eh": eh,
        "entry": (el + eh) / 2,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "score": score_100,
        "confidence": confidence,
        "hold_h": hold_h,
        "duration": duration,
        "regime": regime,
        "contrarian_score": c_score,
        "contrarian_label": c_label,
        "consensus_view": consensus_view,
        "sm_view": sm_view,
        "reasons": reasons,
        "neg_factors": neg_factors,
        "flags": fl,
        "rsi": rv,
        "atr": av,
        "news_score": news_score,
        "news_imp": n_imp,
        "news_risk": news_risk_label,
        "sm_notes": sm_notes,
        "trap_warnings": trap_warnings,
        "narrative": narrative,
        "f_ema": bool(fl["f_ema"]),
        "f_rsi": bool(fl["f_rsi"]),
        "f_macd": bool(fl["f_macd"]),
        "f_sweep": bool(fl["f_sweep"]),
        "f_ob": bool(fl["f_ob"]),
        "f_fvg": bool(fl["f_fvg"]),
        "f_struct": bool(fl["f_struct"]),
    }


# ── Position Sizing ───────────────────────────────────────────────────────────

def calc_sizing(
    sym: str,
    entry: float,
    sl: float,
    rr: float,
    balance: float = 50.0,
    portfolio_heat: float = 0.0,
) -> dict:
    """
    Calculate position sizing for a setup using Trade212 CFD rules.
    Returns dict with margin, notional, risk, leverage, expected P&L.
    """
    lev = get_leverage(sym)
    risk_pct = settings.RISK_PCT
    if portfolio_heat > 10:
        risk_pct *= 0.5
    risk_pct = min(risk_pct, settings.MAX_RISK_PCT)

    risk_amt = round(balance * risk_pct, 2)
    sl_dist_pct = abs(entry - sl) / entry if entry else 0.01
    if sl_dist_pct == 0:
        return {}

    notional = risk_amt / sl_dist_pct
    margin = round(notional / lev, 2)
    # Cap margin at 40% of balance
    margin = min(margin, round(balance * 0.4, 2))

    actual_notional = margin * lev
    actual_risk = round(actual_notional * sl_dist_pct, 2)
    exp_profit = round(actual_risk * rr, 2)

    return {
        "margin": margin,
        "notional": round(actual_notional, 2),
        "risk_amt": actual_risk,
        "leverage": lev,
        "exp_loss": actual_risk,
        "exp_profit": exp_profit,
        "risk_pct": round(actual_risk / balance * 100, 2),
        "asset_class": get_asset_class(sym),
    }
