"use client";
import { useEffect, useState } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { DirectionBadge } from "@/components/ui/DirectionBadge";
import { StatusBadge } from "@/components/ui/StatusBadge";
import { Trade } from "@/lib/types";
import api from "@/lib/api";
import clsx from "clsx";

export default function TradesPage() {
  const [openTrades, setOpenTrades] = useState<Trade[]>([]);
  const [closedTrades, setClosedTrades] = useState<Trade[]>([]);
  const [loading, setLoading] = useState(true);
  const [closingId, setClosingId] = useState<number | null>(null);

  const loadTrades = async () => {
    try {
      const [open, all] = await Promise.all([api.trades.open(), api.trades.list({ limit: 100 })]);
      setOpenTrades(open.trades);
      setClosedTrades(all.trades.filter((t: Trade) => t.status !== "OPEN"));
    } catch (e) {
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadTrades();
    const interval = setInterval(loadTrades, 15000);
    return () => clearInterval(interval);
  }, []);

  const handleClose = async (tradeId: number) => {
    if (!confirm("Close this trade at current market price?")) return;
    setClosingId(tradeId);
    try {
      const res = await api.trades.close(tradeId);
      alert(`Trade closed. Status: ${res.status}, P&L: £${res.pnl_gbp?.toFixed(2)} (${res.pnl_r?.toFixed(2)}R)`);
      loadTrades();
    } catch (e: any) {
      alert(`Error: ${e.message}`);
    } finally {
      setClosingId(null);
    }
  };

  const fmt = (p?: number) => {
    if (p == null) return "—";
    if (p > 1000) return p.toFixed(2);
    if (p > 100) return p.toFixed(3);
    return p.toFixed(5);
  };

  const fmtDate = (dt?: string) => {
    if (!dt) return "—";
    return new Date(dt).toLocaleString("en-GB", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  };

  return (
    <div className="p-4 lg:p-6 space-y-6">
      <div>
        <h1 className="text-text-primary font-bold text-xl">Trade Manager</h1>
        <p className="text-text-dim text-xs font-mono mt-0.5">{openTrades.length} open · {closedTrades.length} closed</p>
      </div>

      {/* Open Trades */}
      <Card>
        <CardHeader title="Open Trades" subtitle="Live floating P&L" />
        {loading ? (
          <div className="p-10 text-center text-text-dim">Loading trades...</div>
        ) : openTrades.length === 0 ? (
          <div className="p-10 text-center text-text-dim text-sm">No open trades. Enter a signal to start tracking.</div>
        ) : (
          <div className="divide-y divide-border">
            {openTrades.map((t) => {
              const pnl = t.pnl_gbp ?? 0;
              const pnlColor = pnl >= 0 ? "text-accent-green" : "text-accent-red";
              const progress = t.progress_pct ?? 0;
              return (
                <div key={t.id} className="p-4">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <span className="text-text-primary font-mono font-bold">{t.sym}</span>
                      <DirectionBadge direction={t.direction} />
                      <span className="text-text-dim text-xs font-mono">#{t.id}</span>
                    </div>
                    <div className="flex items-center gap-3">
                      <div className="text-right">
                        <p className={clsx("font-mono font-bold", pnlColor)}>
                          {pnl >= 0 ? "+" : ""}£{pnl.toFixed(2)}
                        </p>
                        <p className={clsx("font-mono text-xs", pnlColor)}>
                          {(t.pnl_r ?? 0) >= 0 ? "+" : ""}{(t.pnl_r ?? 0).toFixed(2)}R
                        </p>
                      </div>
                      <button
                        onClick={() => handleClose(t.id)}
                        disabled={closingId === t.id}
                        className="px-3 py-1.5 bg-red-900/30 hover:bg-red-900/50 border border-accent-red/40 text-accent-red font-mono text-xs rounded transition-colors disabled:opacity-50"
                      >
                        {closingId === t.id ? "Closing..." : "Close"}
                      </button>
                    </div>
                  </div>
                  <div className="grid grid-cols-5 gap-2 text-xs font-mono text-text-dim mb-2">
                    <div><p>Entry</p><p className="text-text-primary">{fmt(t.entry_price)}</p></div>
                    <div><p>SL</p><p className="text-accent-red">{fmt(t.sl)}</p></div>
                    <div><p>TP</p><p className="text-accent-green">{fmt(t.tp)}</p></div>
                    <div><p>Current</p><p className="text-accent-blue">{fmt(t.current_price)}</p></div>
                    <div><p>Margin</p><p className="text-text-primary">£{t.margin_gbp?.toFixed(2)}</p></div>
                  </div>
                  {/* Progress bar */}
                  <div>
                    <div className="flex justify-between text-xs text-text-dim mb-1">
                      <span>Progress to TP</span>
                      <span>{progress.toFixed(0)}%</span>
                    </div>
                    <div className="w-full bg-gray-800 rounded-full h-1.5">
                      <div
                        className={clsx("h-1.5 rounded-full transition-all", pnl >= 0 ? "bg-accent-green" : "bg-accent-red")}
                        style={{ width: `${Math.max(0, Math.min(100, progress))}%` }}
                      />
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Card>

      {/* Closed Trades History */}
      <Card>
        <CardHeader title="Trade History" subtitle="Closed positions" />
        <div className="overflow-x-auto">
          <table className="w-full text-xs font-mono">
            <thead>
              <tr className="border-b border-border text-text-dim uppercase tracking-wider">
                <th className="text-left px-3 py-2">#</th>
                <th className="text-left px-3 py-2">Pair</th>
                <th className="text-left px-3 py-2">Dir</th>
                <th className="text-right px-3 py-2">Entry</th>
                <th className="text-right px-3 py-2">Exit</th>
                <th className="text-right px-3 py-2">P&L £</th>
                <th className="text-right px-3 py-2">P&L R</th>
                <th className="text-left px-3 py-2">Status</th>
                <th className="text-right px-3 py-2">Opened</th>
                <th className="text-right px-3 py-2">Closed</th>
              </tr>
            </thead>
            <tbody>
              {closedTrades.map((t) => {
                const pnl = t.pnl_gbp ?? 0;
                const pnlColor = pnl >= 0 ? "text-accent-green" : "text-accent-red";
                return (
                  <tr key={t.id} className="border-b border-border/50 hover:bg-white/[0.02] transition-colors">
                    <td className="px-3 py-2 text-text-dim">{t.id}</td>
                    <td className="px-3 py-2 text-text-primary font-bold">{t.sym}</td>
                    <td className="px-3 py-2"><DirectionBadge direction={t.direction} /></td>
                    <td className="px-3 py-2 text-right text-text-primary">{fmt(t.entry_price)}</td>
                    <td className="px-3 py-2 text-right text-text-dim">—</td>
                    <td className={clsx("px-3 py-2 text-right font-bold", pnlColor)}>
                      {pnl >= 0 ? "+" : ""}£{pnl.toFixed(2)}
                    </td>
                    <td className={clsx("px-3 py-2 text-right", pnlColor)}>
                      {(t.pnl_r ?? 0) >= 0 ? "+" : ""}{(t.pnl_r ?? 0).toFixed(2)}R
                    </td>
                    <td className="px-3 py-2"><StatusBadge status={t.status} /></td>
                    <td className="px-3 py-2 text-right text-text-dim">{fmtDate(t.opened_at)}</td>
                    <td className="px-3 py-2 text-right text-text-dim">{fmtDate(t.closed_at)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          {closedTrades.length === 0 && (
            <div className="py-10 text-center text-text-dim text-sm">No closed trades yet.</div>
          )}
        </div>
      </Card>
    </div>
  );
}
