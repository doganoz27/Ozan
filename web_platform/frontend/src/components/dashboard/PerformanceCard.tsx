"use client";
import { useEffect, useState } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { PerformanceStats } from "@/lib/types";
import api from "@/lib/api";
import clsx from "clsx";

export function PerformanceCard() {
  const [stats, setStats] = useState<PerformanceStats | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const res = await api.analytics.performance();
        setStats(res);
      } catch (e) {}
    };
    load();
    const interval = setInterval(load, 60000);
    return () => clearInterval(interval);
  }, []);

  const MetricRow = ({ label, value, color }: { label: string; value: string; color?: string }) => (
    <div className="flex items-center justify-between py-2 border-b border-border last:border-0">
      <span className="text-text-dim text-xs uppercase tracking-wider">{label}</span>
      <span className={clsx("font-mono text-sm font-semibold", color ?? "text-text-primary")}>{value}</span>
    </div>
  );

  // Win rate gauge
  const wr = stats?.win_rate ?? 0;
  const wrColor = wr >= 60 ? "text-accent-green" : wr >= 50 ? "text-accent-yellow" : "text-accent-red";

  return (
    <Card>
      <CardHeader title="Performance KPIs" />
      <div className="p-4">
        {/* Win Rate Gauge */}
        <div className="flex flex-col items-center mb-4">
          <div className="relative w-32 h-16 overflow-hidden">
            <div className="absolute inset-0 flex items-end justify-center">
              <div className="w-32 h-32 rounded-full border-8 border-gray-800" />
            </div>
            <div
              className="absolute inset-0 flex items-end justify-center"
              style={{ clipPath: "inset(50% 0 0 0)" }}
            >
              <div
                className={clsx("w-32 h-32 rounded-full border-8 transition-all", wr >= 60 ? "border-accent-green" : wr >= 50 ? "border-accent-yellow" : "border-accent-red")}
                style={{
                  transform: `rotate(${(wr / 100) * 180 - 90}deg)`,
                  transformOrigin: "50% 100%",
                }}
              />
            </div>
          </div>
          <p className={clsx("font-mono text-3xl font-bold -mt-2", wrColor)}>
            {wr.toFixed(1)}%
          </p>
          <p className="text-text-dim text-xs">Win Rate</p>
        </div>

        {stats ? (
          <div>
            <MetricRow label="Profit Factor" value={stats.profit_factor.toFixed(2)} color={stats.profit_factor >= 1.5 ? "text-accent-green" : "text-accent-yellow"} />
            <MetricRow label="Avg R:R" value={`${stats.avg_rr >= 0 ? "+" : ""}${stats.avg_rr.toFixed(2)}R`} color={stats.avg_rr >= 0 ? "text-accent-green" : "text-accent-red"} />
            <MetricRow label="Sharpe" value={stats.sharpe.toFixed(2)} color={stats.sharpe >= 1 ? "text-accent-green" : "text-accent-yellow"} />
            <MetricRow label="Sortino" value={stats.sortino.toFixed(2)} />
            <MetricRow label="Total Trades" value={String(stats.total)} />
            <MetricRow label="Max Drawdown" value={`${stats.drawdown_pct.toFixed(1)}%`} color={stats.drawdown_pct > 10 ? "text-accent-red" : "text-accent-yellow"} />
            <MetricRow label="Kelly Criterion" value={`${stats.kelly?.toFixed(1) ?? 0}%`} />
          </div>
        ) : (
          <div className="text-text-dim text-sm text-center py-4">No trade history yet.</div>
        )}
      </div>
    </Card>
  );
}
