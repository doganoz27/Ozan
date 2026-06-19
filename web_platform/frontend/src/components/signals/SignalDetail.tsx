"use client";
import { Signal } from "@/lib/types";
import { GradeTag } from "@/components/ui/GradeTag";
import { DirectionBadge } from "@/components/ui/DirectionBadge";
import { StatusBadge } from "@/components/ui/StatusBadge";
import { Card } from "@/components/ui/Card";
import clsx from "clsx";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface SignalDetailProps {
  signal: Signal;
  onEnter?: (id: number) => void;
  onClose?: () => void;
}

export function SignalDetail({ signal: s, onEnter, onClose }: SignalDetailProps) {
  const fmt = (p?: number) => {
    if (p == null) return "—";
    if (p > 1000) return p.toFixed(2);
    if (p > 100) return p.toFixed(3);
    return p.toFixed(5);
  };

  return (
    <div className="space-y-4">
      {/* Title */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-text-primary font-mono font-bold text-xl">{s.sym}</span>
          <DirectionBadge direction={s.direction} size="md" />
          <GradeTag grade={s.quality} size="md" />
          <StatusBadge status={s.status} size="md" />
        </div>
        {onClose && (
          <button onClick={onClose} className="text-text-dim hover:text-text-primary text-sm">✕ Close</button>
        )}
      </div>

      {/* Score bar */}
      <div>
        <div className="flex justify-between mb-1">
          <span className="text-text-dim text-xs uppercase tracking-wider">Signal Score</span>
          <span className={clsx(
            "font-mono font-bold text-sm",
            s.score >= 80 ? "text-yellow-300" : s.score >= 65 ? "text-accent-green" : "text-accent-blue"
          )}>{s.score?.toFixed(0)}/100</span>
        </div>
        <div className="w-full bg-gray-800 rounded-full h-2">
          <div
            className={clsx("h-2 rounded-full", s.score >= 80 ? "bg-yellow-400" : s.score >= 65 ? "bg-accent-green" : "bg-accent-blue")}
            style={{ width: `${s.score}%` }}
          />
        </div>
      </div>

      {/* Price grid */}
      <div className="grid grid-cols-4 gap-3">
        {[
          { label: "Entry", value: fmt(s.entry), color: "text-accent-blue" },
          { label: "Stop Loss", value: fmt(s.sl), color: "text-accent-red" },
          { label: "Take Profit", value: fmt(s.tp), color: "text-accent-green" },
          { label: "R:R", value: `1:${s.rr_t?.toFixed(1)}`, color: "text-accent-yellow" },
        ].map(({ label, value, color }) => (
          <div key={label} className="bg-gray-900/60 rounded p-3 border border-border">
            <p className="text-text-dim text-xs uppercase tracking-wider mb-1">{label}</p>
            <p className={clsx("font-mono font-bold text-sm", color)}>{value}</p>
          </div>
        ))}
      </div>

      {/* Sizing */}
      {s.sizing?.margin && (
        <Card className="p-3">
          <p className="text-accent-yellow font-mono text-xs font-bold mb-2 uppercase tracking-wider">Position Sizing</p>
          <div className="grid grid-cols-3 gap-2 text-xs font-mono">
            <div><p className="text-text-dim">Margin (deposit)</p><p className="text-text-primary font-bold text-sm">£{s.sizing.margin?.toFixed(2)}</p></div>
            <div><p className="text-text-dim">Leverage</p><p className="text-text-primary font-bold text-sm">{s.sizing.leverage}:1</p></div>
            <div><p className="text-text-dim">Account Risk</p><p className="text-text-primary font-bold text-sm">{s.sizing.risk_pct?.toFixed(2)}%</p></div>
            <div><p className="text-text-dim">Max Loss at SL</p><p className="text-accent-red font-bold text-sm">£{s.sizing.exp_loss?.toFixed(2)}</p></div>
            <div><p className="text-text-dim">Target at TP</p><p className="text-accent-green font-bold text-sm">£{s.sizing.exp_profit?.toFixed(2)}</p></div>
            <div><p className="text-text-dim">Notional Size</p><p className="text-text-primary font-bold text-sm">£{s.sizing.notional?.toFixed(0)}</p></div>
          </div>
        </Card>
      )}

      {/* Chart */}
      {s.chart_path && (
        <div>
          <p className="text-accent-blue font-mono text-xs font-bold mb-2 uppercase tracking-wider">Signal Chart</p>
          <img
            src={`${API_BASE}/charts/${s.id}.png`}
            alt={`Chart for ${s.sym}`}
            className="w-full rounded border border-border"
          />
        </div>
      )}

      {/* Narrative */}
      {s.narrative && (
        <div>
          <p className="text-accent-blue font-mono text-xs font-bold mb-2 uppercase tracking-wider">AI Narrative</p>
          <pre className="text-text-dim text-xs font-mono whitespace-pre-wrap leading-relaxed bg-gray-900/40 p-3 rounded border border-border">
            {s.narrative}
          </pre>
        </div>
      )}

      {/* SM Notes + Traps + Consensus */}
      <div className="grid grid-cols-2 gap-3">
        {s.sm_notes?.length > 0 && (
          <div className="bg-gray-900/40 rounded p-3 border border-border">
            <p className="text-accent-purple font-mono text-xs font-bold mb-1 uppercase tracking-wider">Smart Money</p>
            <ul className="space-y-1">
              {s.sm_notes.map((n, i) => <li key={i} className="text-text-dim text-xs">◈ {n}</li>)}
            </ul>
          </div>
        )}
        {s.trap_warnings?.length > 0 && (
          <div className="bg-yellow-900/10 rounded p-3 border border-accent-yellow/20">
            <p className="text-accent-yellow font-mono text-xs font-bold mb-1 uppercase tracking-wider">Trap Warnings</p>
            <ul className="space-y-1">
              {s.trap_warnings.map((w, i) => <li key={i} className="text-accent-yellow text-xs">⚠ {w}</li>)}
            </ul>
          </div>
        )}
      </div>

      {s.consensus_view && (
        <div className="bg-gray-900/40 rounded p-3 border border-border">
          <p className="text-accent-blue font-mono text-xs font-bold mb-1 uppercase tracking-wider">Views</p>
          <p className="text-text-dim text-xs mb-1">{s.consensus_view}</p>
          <p className="text-text-dim text-xs">{s.sm_view}</p>
        </div>
      )}

      {/* Action button */}
      {(s.status === "WATCHLIST" || s.status === "APPROVED") && onEnter && (
        <button
          onClick={() => onEnter(s.id)}
          className="w-full py-3 bg-accent-blue/20 hover:bg-accent-blue/30 border border-accent-blue/40 text-accent-blue font-mono text-sm font-semibold rounded transition-colors"
        >
          Enter Trade on {s.sym} {s.direction}
        </button>
      )}
    </div>
  );
}
