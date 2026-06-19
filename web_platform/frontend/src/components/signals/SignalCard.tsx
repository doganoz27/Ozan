"use client";
import { useState } from "react";
import { Card } from "@/components/ui/Card";
import { GradeTag } from "@/components/ui/GradeTag";
import { DirectionBadge } from "@/components/ui/DirectionBadge";
import { StatusBadge } from "@/components/ui/StatusBadge";
import { Signal } from "@/lib/types";
import clsx from "clsx";

interface SignalCardProps {
  signal: Signal;
  onEnter?: (id: number) => void;
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export function SignalCard({ signal: s, onEnter }: SignalCardProps) {
  const [expanded, setExpanded] = useState(false);

  const fmt = (p?: number, digits = 5) => {
    if (p == null) return "—";
    if (p > 1000) return p.toFixed(2);
    if (p > 100) return p.toFixed(3);
    return p.toFixed(digits);
  };

  const featureFlags = [
    { key: "f_ema", label: "EMA" },
    { key: "f_rsi", label: "RSI" },
    { key: "f_macd", label: "MACD" },
    { key: "f_sweep", label: "Sweep" },
    { key: "f_ob", label: "OB" },
    { key: "f_fvg", label: "FVG" },
    { key: "f_struct", label: "Struct" },
  ] as const;

  const scoreColor =
    s.score >= 80 ? "text-yellow-300" : s.score >= 65 ? "text-accent-green" : s.score >= 50 ? "text-accent-blue" : "text-text-dim";

  return (
    <Card onClick={() => setExpanded(!expanded)} glowing={s.quality === "A+"}>
      <div className="px-4 py-3">
        {/* Header row */}
        <div className="flex items-center justify-between mb-2">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-text-primary font-mono font-bold text-base">{s.sym}</span>
            <DirectionBadge direction={s.direction} />
            <GradeTag grade={s.quality} />
            <StatusBadge status={s.status} />
          </div>
          <div className="flex items-center gap-3">
            <span className={clsx("font-mono font-bold text-lg", scoreColor)}>{s.score?.toFixed(0)}</span>
            <span className="text-text-dim text-xs font-mono">/100</span>
          </div>
        </div>

        {/* Price info row */}
        <div className="grid grid-cols-4 gap-2 text-xs font-mono">
          <div>
            <p className="text-text-dim">Entry</p>
            <p className="text-accent-blue font-semibold">{fmt(s.entry)}</p>
          </div>
          <div>
            <p className="text-text-dim">SL</p>
            <p className="text-accent-red font-semibold">{fmt(s.sl)}</p>
          </div>
          <div>
            <p className="text-text-dim">TP</p>
            <p className="text-accent-green font-semibold">{fmt(s.tp)}</p>
          </div>
          <div>
            <p className="text-text-dim">R:R</p>
            <p className="text-accent-yellow font-semibold">1:{s.rr_t?.toFixed(1)}</p>
          </div>
        </div>

        {/* Feature flags */}
        <div className="flex gap-1 mt-2 flex-wrap">
          {featureFlags.map(({ key, label }) => (
            <span
              key={key}
              className={clsx(
                "px-1.5 py-0.5 rounded text-xs font-mono border",
                (s as any)[key]
                  ? "bg-accent-blue/10 text-accent-blue border-accent-blue/20"
                  : "bg-gray-800 text-text-muted border-gray-700"
              )}
            >
              {label}
            </span>
          ))}
          <span className="px-1.5 py-0.5 rounded text-xs font-mono border bg-gray-800 text-text-dim border-gray-700">
            {s.regime}
          </span>
          <span className="px-1.5 py-0.5 rounded text-xs font-mono border bg-gray-800 text-text-dim border-gray-700">
            {s.duration}
          </span>
        </div>
      </div>

      {/* Expanded detail */}
      {expanded && (
        <div className="border-t border-border px-4 py-3 animate-fade-in">
          {/* Sizing */}
          {s.sizing?.margin && (
            <div className="mb-3 p-3 bg-gray-900/60 rounded border border-border">
              <p className="text-accent-yellow font-mono text-xs font-bold mb-1 uppercase tracking-wider">Position Sizing</p>
              <div className="grid grid-cols-3 gap-2 text-xs font-mono">
                <div><p className="text-text-dim">Margin</p><p className="text-text-primary font-bold">£{s.sizing.margin?.toFixed(2)}</p></div>
                <div><p className="text-text-dim">Leverage</p><p className="text-text-primary font-bold">{s.sizing.leverage}:1</p></div>
                <div><p className="text-text-dim">Risk %</p><p className="text-text-primary font-bold">{s.sizing.risk_pct?.toFixed(2)}%</p></div>
                <div><p className="text-text-dim">Max Loss</p><p className="text-accent-red font-bold">£{s.sizing.exp_loss?.toFixed(2)}</p></div>
                <div><p className="text-text-dim">Target Profit</p><p className="text-accent-green font-bold">£{s.sizing.exp_profit?.toFixed(2)}</p></div>
                <div><p className="text-text-dim">Notional</p><p className="text-text-primary font-bold">£{s.sizing.notional?.toFixed(0)}</p></div>
              </div>
            </div>
          )}

          {/* Narrative */}
          {s.narrative && (
            <div className="mb-3">
              <p className="text-accent-blue font-mono text-xs font-bold mb-1 uppercase tracking-wider">AI Narrative</p>
              <pre className="text-text-dim text-xs font-mono whitespace-pre-wrap leading-relaxed bg-gray-900/40 p-2 rounded border border-border">
                {s.narrative}
              </pre>
            </div>
          )}

          {/* SM Notes */}
          {s.sm_notes?.length > 0 && (
            <div className="mb-3">
              <p className="text-accent-purple font-mono text-xs font-bold mb-1 uppercase tracking-wider">Smart Money Notes</p>
              <ul className="space-y-0.5">
                {s.sm_notes.map((note, i) => (
                  <li key={i} className="text-text-dim text-xs font-mono flex gap-1">
                    <span className="text-accent-purple">◈</span> {note}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Trap Warnings */}
          {s.trap_warnings?.length > 0 && (
            <div className="mb-3">
              <p className="text-accent-yellow font-mono text-xs font-bold mb-1 uppercase tracking-wider">Trap Warnings</p>
              <ul className="space-y-0.5">
                {s.trap_warnings.map((w, i) => (
                  <li key={i} className="text-accent-yellow text-xs font-mono flex gap-1">
                    <span>⚠</span> {w}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Consensus View */}
          {s.consensus_view && (
            <div className="mb-3">
              <p className="text-accent-blue font-mono text-xs font-bold mb-1 uppercase tracking-wider">Consensus vs Smart Money</p>
              <p className="text-text-dim text-xs">{s.consensus_view}</p>
              <p className="text-text-dim text-xs mt-0.5">{s.sm_view}</p>
            </div>
          )}

          {/* Chart */}
          {s.chart_path && (
            <div className="mb-3">
              <p className="text-accent-blue font-mono text-xs font-bold mb-1 uppercase tracking-wider">Chart</p>
              <img
                src={`${API_BASE}/charts/${s.id}.png`}
                alt={`Chart for ${s.sym}`}
                className="rounded border border-border max-w-full"
              />
            </div>
          )}

          {/* Reasons */}
          {s.reasons?.length > 0 && (
            <div className="mb-2">
              <p className="text-accent-green font-mono text-xs font-bold mb-1 uppercase tracking-wider">Bullish Factors</p>
              <ul className="space-y-0.5">
                {s.reasons.map((r, i) => (
                  <li key={i} className="text-text-dim text-xs font-mono flex gap-1">
                    <span className="text-accent-green">+</span> {r}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Neg factors */}
          {s.neg_factors?.length > 0 && (
            <div className="mb-2">
              <p className="text-accent-red font-mono text-xs font-bold mb-1 uppercase tracking-wider">Risk Factors</p>
              <ul className="space-y-0.5">
                {s.neg_factors.slice(0, 4).map((r, i) => (
                  <li key={i} className="text-text-dim text-xs font-mono flex gap-1">
                    <span className="text-accent-red">-</span> {r}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Enter button */}
          {(s.status === "WATCHLIST" || s.status === "APPROVED") && onEnter && (
            <button
              onClick={(e) => { e.stopPropagation(); onEnter(s.id); }}
              className="mt-2 w-full py-2 bg-accent-blue/20 hover:bg-accent-blue/30 border border-accent-blue/40 text-accent-blue font-mono text-sm font-semibold rounded transition-colors"
            >
              Enter Trade
            </button>
          )}
        </div>
      )}
    </Card>
  );
}
