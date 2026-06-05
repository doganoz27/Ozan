"""
TITAN PRIME — Telegram Bot Service
VIP signal alerts, outcomes, news, and performance reports.
"""
import asyncio
from datetime import datetime
from typing import Optional

import httpx

from core.config import settings


def _fp(v) -> str:
    """Format price with appropriate decimal places."""
    if v is None:
        return "—"
    a = abs(v)
    if a > 10000:
        return f"{v:,.1f}"
    if a > 100:
        return f"{v:,.3f}"
    if a > 1:
        return f"{v:.5f}"
    return f"{v:.6f}"


async def _send_message(text: str, photo_path: Optional[str] = None) -> bool:
    """Send a Telegram message, optionally with a photo."""
    if not settings.TELEGRAM_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return False
    base_url = f"https://api.telegram.org/bot{settings.TELEGRAM_TOKEN}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            if photo_path:
                try:
                    with open(photo_path, "rb") as f:
                        r = await client.post(
                            f"{base_url}/sendPhoto",
                            data={"chat_id": settings.TELEGRAM_CHAT_ID, "caption": text[:1024], "parse_mode": "HTML"},
                            files={"photo": f},
                        )
                        return r.status_code == 200
                except Exception:
                    pass  # Fall through to text-only
            r = await client.post(
                f"{base_url}/sendMessage",
                json={
                    "chat_id": settings.TELEGRAM_CHAT_ID,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
            return r.status_code == 200
    except Exception:
        return False


async def send_signal_alert(signal: dict, chart_path: Optional[str] = None) -> bool:
    """
    Send a VIP-grade signal alert via Telegram.
    Matches the format from titan_flow.py tg_setup_alert().
    """
    sym = signal.get("sym", "?")
    direction = signal.get("direction", "?")
    quality = signal.get("quality", "B+")
    score = signal.get("score", 0)
    rr = signal.get("rr", 0)
    regime = signal.get("regime", "Neutral")
    duration = signal.get("duration", "Intraday")
    hold_h = signal.get("hold_h", 0)
    c_score = signal.get("contrarian_score", 0)
    c_label = signal.get("contrarian_label", "—")
    sm = signal.get("sm_notes", [])
    traps = signal.get("trap_warnings", [])
    cv = signal.get("consensus_view", "")
    smv = signal.get("sm_view", "")
    news_risk = signal.get("news_risk", "—")
    sig_id = signal.get("id", 0)
    sizing = signal.get("sizing") or {}
    el = signal.get("el", signal.get("entry", 0))
    eh = signal.get("eh", signal.get("entry", 0))
    sl = signal.get("sl", 0)
    tp = signal.get("tp", 0)
    confidence = signal.get("confidence", score)

    # Grade header
    if quality == "A+":
        grade_hdr = "🔥 A+ SETUP — INSTITUTIONAL APPROVED 🔥"
        grade_bar = "★★★★★"
    elif quality == "A":
        grade_hdr = "✅ A SETUP — HIGH PROBABILITY"
        grade_bar = "★★★★☆"
    else:
        grade_hdr = "🔔 B+ SETUP — STRONG OPPORTUNITY"
        grade_bar = "★★★☆☆"

    dir_emoji = "📈" if direction == "LONG" else "📉"
    regime_emoji = "🟢" if regime == "Risk-On" else "🔴" if regime == "Risk-Off" else "🟡"

    # Score bar
    filled = int(score / 10)
    bar = "█" * filled + "░" * (10 - filled)

    # Sizing block
    sz_block = ""
    if sizing:
        margin = sizing.get("margin", 0)
        lev = sizing.get("leverage", 1)
        exp_loss = sizing.get("exp_loss", 0)
        exp_profit = sizing.get("exp_profit", 0)
        notional = sizing.get("notional", 0)
        risk_pct = sizing.get("risk_pct", 0)
        sz_block = (
            f"\n┌──────────────────────────────┐\n"
            f"│  💷 POSITION SIZING           │\n"
            f"└──────────────────────────────┘\n"
            f"🔑 Deposit to Trade212 : <b>£{margin:.2f}</b> (margin)\n"
            f"⚡ Leverage            : <b>{lev}:1</b>  →  £{notional:.0f} notional\n"
            f"⚖️ Account risk        : <b>{risk_pct:.2f}%</b>\n"
            f"❌ Max loss at SL      : <b>£{exp_loss:.2f}</b>\n"
            f"✅ Target profit at TP : <b>£{exp_profit:.2f}</b>\n"
        )

    # Smart money block
    sm_block = ""
    if sm:
        sm_block = "\n🧠 <b>SMART MONEY ANALYSIS</b>\n"
        for n in sm[:3]:
            sm_block += f"  ◈ {n}\n"

    # Trap warnings
    trap_block = ""
    if traps:
        trap_block = "\n⚠️ <b>TRAP WARNING</b>\n"
        for t in traps[:2]:
            trap_block += f"  {t}\n"

    # Consensus block
    cv_block = ""
    if cv:
        cv_block = (
            f"\n🔍 <b>VIEW COMPARISON</b>\n"
            f"  {cv}\n"
            f"  {smv}\n"
        )

    # Contrarian bar
    c_bar = "█" * int(c_score / 10) + "░" * (10 - int(c_score / 10))
    c_emoji = "⚡" if c_score >= 70 else "〰️" if c_score >= 40 else "➡️"

    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")

    msg = (
        f"╔══════════════════════════╗\n"
        f"║  🏆 <b>TITAN PRIME ELITE</b>  🏆  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"<b>{grade_hdr}</b>\n"
        f"{grade_bar}  <b>{score:.0f}/100</b>  {bar}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{dir_emoji} <b>{sym}</b>  —  <b>{direction}</b>\n"
        f"{regime_emoji} Regime: <b>{regime}</b>  |  Duration: <b>{duration}</b> (~{hold_h:.0f}h)\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📌 <b>ENTRY ZONE</b>  <code>{_fp(el)} — {_fp(eh)}</code>\n"
        f"🔴 <b>STOP LOSS</b>  <code>{_fp(sl)}</code>\n"
        f"🟢 <b>TAKE PROFIT</b>  <code>{_fp(tp)}</code>\n\n"
        f"⚖️ <b>Risk/Reward : 1:{rr}</b>\n"
        f"🎯 Confidence : <b>{confidence:.0f}/100</b>\n"
        f"📰 News Risk  : <b>{news_risk}</b>\n\n"
        f"{c_emoji} <b>Contrarian Score</b>: {c_score}/100\n"
        f"   {c_bar}  {c_label}\n"
        f"{cv_block}{sm_block}{trap_block}{sz_block}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {now_str}\n"
        f"🔖 Signal ID: <code>#{sig_id:05d}</code>\n"
        f"⚡ <b>TITAN PRIME ELITE — CONFIDENTIAL</b> ⚡"
    )

    return await _send_message(msg, chart_path)


async def send_outcome_alert(
    sym: str,
    direction: str,
    status: str,
    entry: float,
    out_price: float,
    act_rr: float,
    sig_id: Optional[int] = None,
) -> bool:
    """Send a trade outcome notification."""
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")

    if status == "TP":
        header = "╔══════════════════════════╗\n║  🎯  TAKE PROFIT HIT  🎯  ║\n╚══════════════════════════╝"
        result_line = f"✅ <b>SUCCESS — +{act_rr:.2f}R PROFIT</b>" if act_rr else "✅ <b>TAKE PROFIT HIT</b>"
        pnl_emoji = "💰"
    elif status == "SL":
        header = "╔══════════════════════════╗\n║  ❌  STOP LOSS HIT  ❌  ║\n╚══════════════════════════╝"
        result_line = f"🛑 <b>STOPPED — {act_rr:.2f}R LOSS</b>" if act_rr else "🛑 <b>STOP LOSS HIT</b>"
        pnl_emoji = "🔻"
    elif status == "INVALIDATED":
        header = "╔══════════════════════════╗\n║  🚫  SIGNAL CANCELLED  🚫  ║\n╚══════════════════════════╝"
        result_line = "⛔ <b>SETUP INVALIDATED</b>"
        pnl_emoji = "⚠️"
    elif status == "EXPIRED":
        header = "╔══════════════════════════╗\n║  ⏰  TIME EXPIRED  ⏰  ║\n╚══════════════════════════╝"
        result_line = "📭 <b>SIGNAL EXPIRED</b>"
        pnl_emoji = "🕐"
    else:
        return False

    move = ""
    if entry and out_price and entry > 0:
        pct = (
            round((out_price - entry) / entry * 100, 3) if direction == "LONG"
            else round((entry - out_price) / entry * 100, 3)
        )
        move_emoji = "📈" if pct >= 0 else "📉"
        move = f"\n{move_emoji} Move  : <b>{pct:+.3f}%</b>"

    rr_bar = "█" * min(int(abs(act_rr) * 2), 10) + "░" * (10 - min(int(abs(act_rr) * 2), 10))

    msg = (
        f"{header}\n\n"
        f"📊 <b>{sym}</b>  —  {direction}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📥 Entry  : <code>{_fp(entry)}</code>\n"
        f"📤 Exit   : <code>{_fp(out_price)}</code>"
        f"{move}\n"
        f"{pnl_emoji} Result : <b>{act_rr:+.2f}R</b>  {rr_bar}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{result_line}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {now_str}"
        + (f"\n🔖 Signal ID: <code>#{sig_id:05d}</code>" if sig_id else "")
        + f"\n<i>Titan Prime Elite — system learning from this result</i>"
    )
    return await _send_message(msg)


async def send_news_alert(article: dict) -> bool:
    """Send a high-impact news alert."""
    headline = article.get("headline", "")
    imp = article.get("importance", 0)
    rl = article.get("risk_level", "NOISE")
    bias = article.get("bias", "NEUTRAL")
    bull_pct = article.get("bull_pct", 0)
    bear_pct = article.get("bear_pct", 0)
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")

    if imp >= 80:
        risk_hdr = "🔴 CRITICAL NEWS"
        risk_bar = "█████████░"
    elif imp >= 60:
        risk_hdr = "🟠 HIGH IMPACT NEWS"
        risk_bar = "███████░░░"
    elif imp >= 40:
        risk_hdr = "🟡 MEDIUM IMPACT NEWS"
        risk_bar = "█████░░░░░"
    else:
        risk_hdr = "🔵 LOW IMPACT NEWS"
        risk_bar = "███░░░░░░░"

    bias_emoji = "📈" if bias == "BULLISH" else ("📉" if bias == "BEARISH" else "➡️")

    msg = (
        f"╔══════════════════════════╗\n"
        f"║  📰 <b>MACRO NEWS ALERT</b>  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"<b>{risk_hdr}</b>  {risk_bar}\n"
        f"Impact Score: <b>{imp}/100</b>  |  Risk: <b>{rl}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 <b>HEADLINE</b>\n"
        f"<b>{headline}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{bias_emoji} <b>MARKET BIAS</b>: <b>{bias}</b>\n"
        f"  📈 Bullish probability : {bull_pct}%\n"
        f"  📉 Bearish probability : {bear_pct}%\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {now_str}  |  <i>Titan Prime Elite</i>"
    )
    return await _send_message(msg)


async def send_performance_report(stats: dict, period: str = "daily") -> bool:
    """Send daily or weekly performance report."""
    total = stats.get("total", 0)
    wins = stats.get("wins", 0)
    losses = stats.get("losses", 0)
    wr = stats.get("win_rate", 0)
    avg_rr = stats.get("avg_rr", 0)
    pf = stats.get("profit_factor", 0)
    sharpe = stats.get("sharpe", 0)
    sortino = stats.get("sortino", 0)
    balance = stats.get("balance", 50.0)
    start_balance = stats.get("start_balance", 50.0)
    pnl = round(balance - start_balance, 2)
    pnl_pct = round(pnl / start_balance * 100, 1) if start_balance else 0
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")

    wr_filled = int(wr / 10)
    wr_bar = "█" * wr_filled + "░" * (10 - wr_filled)
    wr_emoji = "🟢" if wr >= 60 else ("🟡" if wr >= 50 else "🔴")
    period_hdr = "📅 DAILY PERFORMANCE REPORT" if period == "daily" else "📆 WEEKLY PERFORMANCE REPORT"

    msg = (
        f"╔══════════════════════════╗\n"
        f"║  🏆 <b>TITAN PRIME ELITE</b>  🏆  ║\n"
        f"╚══════════════════════════╝\n\n"
        f"<b>{period_hdr}</b>\n"
        f"🕐 {now_str}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>PERFORMANCE SUMMARY</b>\n"
        f"  Total Trades : <b>{total}</b>  ({wins} TP / {losses} SL)\n"
        f"  {wr_emoji} Win Rate    : <b>{wr:.1f}%</b>  {wr_bar}\n"
        f"  Avg R:R      : <b>{avg_rr:+.2f}R</b>\n"
        f"  Profit Factor: <b>{pf:.2f}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>ACCOUNT STATUS</b>\n"
        f"  Starting   : £{start_balance:.2f}\n"
        f"  Current    : <b>£{balance:.2f}</b>\n"
        f"  Net P&L    : <b>{'+'if pnl>=0 else ''}{pnl:.2f} ({pnl_pct:+.1f}%)</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 Sharpe: {sharpe:.2f}  |  Sortino: {sortino:.2f}\n"
        f"⚡ <b>TITAN PRIME ELITE</b>  —  <i>System continuously learning</i>"
    )
    return await _send_message(msg)
