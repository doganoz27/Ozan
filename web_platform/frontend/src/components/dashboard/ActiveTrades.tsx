"use client";
import { useEffect, useState } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { DirectionBadge } from "@/components/ui/DirectionBadge";
import { Trade } from "@/lib/types";
import api from "@/lib/api";
import Link from "next/link";
import clsx from "clsx";

interface ActiveTradesProps {
  liveUpdate?: any;
}

export function ActiveTrades({ liveUpdate }: ActiveTradesProps) {
  const [trades, setTrades] = useState<Trade[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const load = async () => {
      try {
        const res = await api.trades.open();
        setTrades(res.trades);
      } catch (e) {
      } finally {
        setLoading(false);
      }
    };
    load();
    const interval = setInterval(load, 15000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (liveUpdate) {
      setTrades((prev) => {
        const updated = prev.map((t) =>
          t.signal_id === liveUpdate.signal_id ? { ...t, ...liveUpdate } : t
        );
        return updated.filter((t) => t.status === "OPEN");
      });
    }
  }, [liveUpdate]);

  const formatPrice = (p?: number) => {
    if (p == null) return "—";
    if (p > 100) return p.toFixed(2);
    return p.toFixed(5);
  };

  return (
    <Card>
      <CardHeader
        title="Active Trades"
        subtitle={`${trades.length} open`}
        right={
          <Link href="/trades" className="text-accent-blue text-xs hover:underline">
            Manage
          </Link>
        }
      />
      {loading ? (
        <div className="p-6 text-text-dim text-sm text-center">Loading trades...</div>
      ) : trades.length === 0 ? (
        <div className="p-6 text-text-dim text-sm text-center">No open trades.</div>
      ) : (
        <div className="divide-y divide-border">
          {trades.map((trade) => {
            const pnl = trade.pnl_gbp ?? 0;
            const pnlColor = pnl >= 0 ? "text-accent-green" : "text-accent-red";
            const progressPct = trade.progress_pct ?? 0;
            return (
              <div key={trade.id} className="px-4 py-3">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <span className="text-text-primary font-mono font-bold text-sm">{trade.sym}</span>
                    <DirectionBadge direction={trade.direction} />
                  </div>
                  <div className="text-right">
                    <p className={clsx("font-mono font-bold text-sm", pnlColor)}>
                      {pnl >= 0 ? "+" : ""}£{pnl.toFixed(2)}
                    </p>
                    <p className={clsx("font-mono text-xs", pnlColor)}>
                      {(trade.pnl_r ?? 0) >= 0 ? "+" : ""}{(trade.pnl_r ?? 0).toFixed(2)}R
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-3 text-xs font-mono text-text-dim mb-2">
                  <span>Entry: {formatPrice(trade.entry_price)}</span>
                  <span>SL: <span className="text-accent-red">{formatPrice(trade.sl)}</span></span>
                  <span>TP: <span className="text-accent-green">{formatPrice(trade.tp)}</span></span>
                  {trade.current_price && (
                    <span>Now: <span className="text-text-primary">{formatPrice(trade.current_price)}</span></span>
                  )}
                </div>
                {/* Progress bar to TP */}
                <div className="w-full bg-gray-800 rounded-full h-1.5">
                  <div
                    className={clsx("h-1.5 rounded-full transition-all", pnl >= 0 ? "bg-accent-green" : "bg-accent-red")}
                    style={{ width: `${Math.max(0, Math.min(100, progressPct))}%` }}
                  />
                </div>
                <p className="text-right text-xs text-text-dim mt-0.5">{progressPct?.toFixed(0)}% to TP</p>
              </div>
            );
          })}
        </div>
      )}
    </Card>
  );
}
