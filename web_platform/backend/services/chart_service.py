"""
TITAN PRIME — Chart Service
Generates professional dark-theme signal charts using matplotlib/mplfinance.
"""
import asyncio
import os
from datetime import datetime
from typing import Optional

from core.config import settings


async def generate_signal_chart(signal: dict, candles: list) -> Optional[str]:
    """
    Generate a PNG chart for a signal.
    Returns the file path, or None on failure.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _generate_chart_sync, signal, candles)


def _generate_chart_sync(signal: dict, candles: list) -> Optional[str]:
    """Synchronous chart generation."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        import pandas as pd
        import numpy as np

        if len(candles) < 10:
            return None

        os.makedirs(settings.CHART_DIR, exist_ok=True)

        # Build DataFrame
        df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        df = df.set_index("timestamp")
        df = df.tail(80)  # Show last 80 candles

        cl = df["close"].tolist()

        # Calculate EMAs
        def ema_calc(prices, n):
            if len(prices) < n:
                return []
            k = 2 / (n + 1)
            r = [sum(prices[:n]) / n]
            for x in prices[n:]:
                r.append(x * k + r[-1] * (1 - k))
            return r

        e9 = ema_calc(cl, 9)
        e20 = ema_calc(cl, 20)
        e50 = ema_calc(cl, 50)
        e200_full = ema_calc(cl, min(200, len(cl)))

        # Dark theme
        plt.style.use("dark_background")
        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(14, 9),
            gridspec_kw={"height_ratios": [3, 1]},
            facecolor="#0a0b0e",
        )
        ax1.set_facecolor("#111318")
        ax2.set_facecolor("#111318")

        idx = range(len(df))
        opens = df["open"].values
        highs = df["high"].values
        lows = df["low"].values
        closes = df["close"].values
        volumes = df["volume"].values

        # Draw candlesticks
        for i, (o, h, l, c) in enumerate(zip(opens, highs, lows, closes)):
            color = "#00ff88" if c >= o else "#ff3b5c"
            ax1.plot([i, i], [l, h], color=color, linewidth=0.8, alpha=0.7)
            rect_h = abs(c - o) if abs(c - o) > 0 else (h - l) * 0.001
            rect_y = min(o, c)
            ax1.add_patch(mpatches.FancyBboxPatch(
                (i - 0.35, rect_y), 0.7, rect_h,
                boxstyle="square,pad=0", color=color, alpha=0.85,
            ))

        # EMA lines
        n9 = len(cl) - len(e9)
        n20 = len(cl) - len(e20)
        n50 = len(cl) - len(e50)

        x9 = range(n9, len(cl))
        x20 = range(n20, len(cl))
        x50 = range(n50, len(cl))

        ax1.plot(list(x9), e9, color="#3b82f6", linewidth=1.0, alpha=0.8, label="EMA9")
        ax1.plot(list(x20), e20, color="#f59e0b", linewidth=1.0, alpha=0.8, label="EMA20")
        ax1.plot(list(x50), e50, color="#8b5cf6", linewidth=1.2, alpha=0.7, label="EMA50")
        if e200_full:
            n200 = len(cl) - len(e200_full)
            x200 = range(n200, len(cl))
            ax1.plot(list(x200), e200_full, color="#6b7280", linewidth=1.5, alpha=0.6, label="EMA200")

        # Entry / SL / TP lines
        entry = signal.get("entry", signal.get("price", 0))
        sl_price = signal.get("sl", 0)
        tp_price = signal.get("tp", 0)
        direction = signal.get("direction", "LONG")

        x_start = max(0, len(df) - 25)
        x_end = len(df) - 1

        if entry:
            ax1.axhline(y=entry, color="#3b82f6", linewidth=1.5, linestyle="--", alpha=0.9, label=f"Entry {entry:.4f}")
            ax1.annotate(f"  ENTRY {entry:.4f}", xy=(x_end, entry),
                        xytext=(x_end + 0.5, entry), color="#3b82f6",
                        fontsize=7, fontweight="bold", va="center")
        if sl_price:
            ax1.axhline(y=sl_price, color="#ff3b5c", linewidth=1.5, linestyle="--", alpha=0.9)
            ax1.annotate(f"  SL {sl_price:.4f}", xy=(x_end, sl_price),
                        xytext=(x_end + 0.5, sl_price), color="#ff3b5c",
                        fontsize=7, fontweight="bold", va="center")
        if tp_price:
            ax1.axhline(y=tp_price, color="#00ff88", linewidth=1.5, linestyle="--", alpha=0.9)
            ax1.annotate(f"  TP {tp_price:.4f}", xy=(x_end, tp_price),
                        xytext=(x_end + 0.5, tp_price), color="#00ff88",
                        fontsize=7, fontweight="bold", va="center")

        # Direction arrow
        mid_x = len(df) // 2
        mid_y = closes[mid_x] if mid_x < len(closes) else entry
        arrow_color = "#00ff88" if direction == "LONG" else "#ff3b5c"
        arrow_dir = "^" if direction == "LONG" else "v"
        ax1.plot(mid_x, mid_y, marker=arrow_dir, color=arrow_color, markersize=12, alpha=0.7)

        # FVG zones
        fvg = signal.get("fvg_zone")
        if fvg:
            ax1.axhspan(fvg[0], fvg[1], alpha=0.1, color="#3b82f6", label="FVG")

        # OB zones
        ob_zone = signal.get("ob_zone")
        if ob_zone:
            ax1.axhspan(ob_zone[0], ob_zone[1], alpha=0.12, color="#f59e0b", label="OB")

        # Score overlay
        score = signal.get("score", 0)
        quality = signal.get("quality", "B+")
        score_color = "#00ff88" if score >= 80 else "#f59e0b" if score >= 60 else "#ff3b5c"
        ax1.text(
            0.02, 0.97,
            f"TITAN PRIME  |  {signal.get('sym','')}  {direction}  [{quality}]  Score: {score:.0f}/100",
            transform=ax1.transAxes, fontsize=9, fontweight="bold",
            color=score_color, va="top",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#111318", alpha=0.8),
        )
        ax1.text(
            0.02, 0.91,
            f"R:R 1:{signal.get('rr',0):.1f}  |  {signal.get('duration','')}  |  {signal.get('regime','')}",
            transform=ax1.transAxes, fontsize=7.5, color="#6b7280", va="top",
        )

        # Volume bars
        vol_colors = ["#00ff88" if c >= o else "#ff3b5c" for o, c in zip(opens, closes)]
        ax2.bar(idx, volumes, color=vol_colors, alpha=0.6, width=0.8)
        ax2.set_ylabel("Volume", color="#6b7280", fontsize=8)

        # Formatting
        ax1.set_ylabel("Price", color="#6b7280", fontsize=9)
        ax1.legend(loc="upper left", fontsize=7, fancybox=True, framealpha=0.4,
                   labelcolor="white", facecolor="#111318")
        ax1.tick_params(colors="#6b7280", labelsize=7)
        ax2.tick_params(colors="#6b7280", labelsize=7)
        for spine in ax1.spines.values():
            spine.set_color("#1e2028")
        for spine in ax2.spines.values():
            spine.set_color("#1e2028")
        ax1.grid(True, color="#1e2028", linewidth=0.5, alpha=0.5)
        ax2.grid(True, color="#1e2028", linewidth=0.5, alpha=0.5)

        plt.tight_layout(pad=1.0)
        sig_id = signal.get("id", int(datetime.now().timestamp()))
        chart_path = os.path.join(settings.CHART_DIR, f"{sig_id}.png")
        plt.savefig(chart_path, dpi=120, bbox_inches="tight", facecolor="#0a0b0e")
        plt.close(fig)
        return chart_path

    except Exception as e:
        return None
