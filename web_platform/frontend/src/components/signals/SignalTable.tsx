"use client";
import { GradeTag } from "@/components/ui/GradeTag";
import { DirectionBadge } from "@/components/ui/DirectionBadge";
import { StatusBadge } from "@/components/ui/StatusBadge";
import { Signal } from "@/lib/types";
import clsx from "clsx";

interface SignalTableProps {
  signals: Signal[];
  onSelect?: (signal: Signal) => void;
  selectedId?: number;
}

export function SignalTable({ signals, onSelect, selectedId }: SignalTableProps) {
  const fmt = (p?: number) => {
    if (p == null) return "—";
    if (p > 1000) return p.toFixed(2);
    if (p > 100) return p.toFixed(3);
    return p.toFixed(5);
  };

  const fmtTime = (dt?: string) => {
    if (!dt) return "—";
    const d = new Date(dt);
    return d.toLocaleString("en-GB", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs font-mono">
        <thead>
          <tr className="border-b border-border text-text-dim uppercase tracking-wider">
            <th className="text-left px-3 py-2 w-8">#</th>
            <th className="text-left px-3 py-2">Pair</th>
            <th className="text-left px-3 py-2">Grade</th>
            <th className="text-left px-3 py-2">Dir</th>
            <th className="text-right px-3 py-2">Score</th>
            <th className="text-right px-3 py-2">Entry</th>
            <th className="text-right px-3 py-2">SL</th>
            <th className="text-right px-3 py-2">TP</th>
            <th className="text-right px-3 py-2">R:R</th>
            <th className="text-center px-3 py-2">Regime</th>
            <th className="text-left px-3 py-2">Status</th>
            <th className="text-right px-3 py-2">Time</th>
          </tr>
        </thead>
        <tbody>
          {signals.map((s) => (
            <tr
              key={s.id}
              onClick={() => onSelect?.(s)}
              className={clsx(
                "border-b border-border/50 hover:bg-white/[0.02] cursor-pointer transition-colors",
                selectedId === s.id && "bg-accent-blue/5 border-accent-blue/20"
              )}
            >
              <td className="px-3 py-2 text-text-dim">{s.id}</td>
              <td className="px-3 py-2 text-text-primary font-bold">{s.sym}</td>
              <td className="px-3 py-2"><GradeTag grade={s.quality} /></td>
              <td className="px-3 py-2"><DirectionBadge direction={s.direction} /></td>
              <td className="px-3 py-2 text-right">
                <span className={clsx(
                  "font-bold",
                  s.score >= 80 ? "text-yellow-300" : s.score >= 65 ? "text-accent-green" : "text-accent-blue"
                )}>
                  {s.score?.toFixed(0)}
                </span>
              </td>
              <td className="px-3 py-2 text-right text-text-primary">{fmt(s.entry)}</td>
              <td className="px-3 py-2 text-right text-accent-red">{fmt(s.sl)}</td>
              <td className="px-3 py-2 text-right text-accent-green">{fmt(s.tp)}</td>
              <td className="px-3 py-2 text-right text-accent-yellow">1:{s.rr_t?.toFixed(1)}</td>
              <td className="px-3 py-2 text-center">
                <span className={clsx(
                  "px-1 py-0.5 rounded text-xs",
                  s.regime === "Risk-On" ? "text-accent-green" :
                  s.regime === "Risk-Off" ? "text-accent-red" : "text-text-dim"
                )}>
                  {s.regime ?? "—"}
                </span>
              </td>
              <td className="px-3 py-2"><StatusBadge status={s.status} /></td>
              <td className="px-3 py-2 text-right text-text-dim">{fmtTime(s.created_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {signals.length === 0 && (
        <div className="py-10 text-center text-text-dim text-sm">No signals in this category.</div>
      )}
    </div>
  );
}
