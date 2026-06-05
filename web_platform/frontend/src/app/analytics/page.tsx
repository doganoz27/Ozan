"use client";
import { useEffect, useState } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { StatCard } from "@/components/ui/StatCard";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine, Cell,
} from "recharts";
import { PerformanceStats, FeatureWeight, EquityPoint } from "@/lib/types";
import api from "@/lib/api";
import clsx from "clsx";

export default function AnalyticsPage() {
  const [perf, setPerf] = useState<PerformanceStats | null>(null);
  const [equityCurve, setEquityCurve] = useState<EquityPoint[]>([]);
  const [bySymbol, setBySymbol] = useState<any[]>([]);
  const [byQuality, setByQuality] = useState<any[]>([]);
  const [features, setFeatures] = useState<FeatureWeight[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const [p, eq, sym, qual, feat] = await Promise.all([
          api.analytics.performance(),
          api.analytics.equityCurve(),
          api.analytics.bySymbol(),
          api.analytics.byQuality(),
          api.analytics.features(),
        ]);
        setPerf(p);
        setEquityCurve(eq.equity_curve ?? []);
        setBySymbol(sym.by_symbol ?? []);
        setByQuality(qual.by_quality ?? []);
        setFeatures(feat.features ?? []);
      } catch (e) {
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  const CHART_THEME = {
    stroke: "#1e2028",
    tickColor: "#6b7280",
    green: "#00ff88",
    red: "#ff3b5c",
    blue: "#3b82f6",
    yellow: "#f59e0b",
  };

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null;
    return (
      <div className="bg-card border border-border rounded p-2 text-xs font-mono shadow-lg">
        <p className="text-text-dim mb-1">{label}</p>
        {payload.map((p: any) => (
          <p key={p.name} style={{ color: p.color }}>
            {p.name}: {typeof p.value === "number" ? p.value.toFixed(2) : p.value}
          </p>
        ))}
      </div>
    );
  };

  if (loading) {
    return (
      <div className="p-6 text-center text-text-dim">
        <div className="animate-pulse">Loading analytics...</div>
      </div>
    );
  }

  const finalBalance = equityCurve.length > 0 ? equityCurve[equityCurve.length - 1].balance : 50;
  const totalPnl = finalBalance - 50;

  return (
    <div className="p-4 lg:p-6 space-y-6">
      <div>
        <h1 className="text-text-primary font-bold text-xl">Analytics</h1>
        <p className="text-text-dim text-xs font-mono mt-0.5">Performance statistics & AI learning insights</p>
      </div>

      {/* KPI Row */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <StatCard label="Win Rate" value={`${perf?.win_rate?.toFixed(1) ?? 0}%`} color={perf && perf.win_rate >= 55 ? "green" : "yellow"} sub={`${perf?.wins ?? 0}W / ${perf?.losses ?? 0}L`} />
        <StatCard label="Profit Factor" value={perf?.profit_factor?.toFixed(2) ?? "0.00"} color={perf && perf.profit_factor >= 1.5 ? "green" : "yellow"} />
        <StatCard label="Sharpe Ratio" value={perf?.sharpe?.toFixed(2) ?? "0.00"} color={perf && perf.sharpe >= 1 ? "green" : "default"} />
        <StatCard label="Total P&L" value={`${totalPnl >= 0 ? "+" : ""}£${totalPnl.toFixed(2)}`} color={totalPnl >= 0 ? "green" : "red"} sub={`Balance: £${finalBalance.toFixed(2)}`} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Equity Curve */}
        <Card className="lg:col-span-2">
          <CardHeader title="Equity Curve" subtitle={`Starting £50.00 → £${finalBalance.toFixed(2)}`} />
          <div className="p-4 h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={equityCurve} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.stroke} />
                <XAxis dataKey="date" tick={{ fill: CHART_THEME.tickColor, fontSize: 10 }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
                <YAxis tick={{ fill: CHART_THEME.tickColor, fontSize: 10 }} tickLine={false} axisLine={false} tickFormatter={(v) => `£${v}`} />
                <Tooltip content={<CustomTooltip />} />
                <ReferenceLine y={50} stroke="#374151" strokeDasharray="4 4" />
                <Line
                  type="monotone"
                  dataKey="balance"
                  stroke={CHART_THEME.green}
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, fill: CHART_THEME.green }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Card>

        {/* Win Rate by Grade */}
        <Card>
          <CardHeader title="Win Rate by Grade" />
          <div className="p-4 h-48">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={byQuality} layout="vertical" margin={{ left: 10, right: 20, top: 5, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.stroke} horizontal={false} />
                <XAxis type="number" tick={{ fill: CHART_THEME.tickColor, fontSize: 10 }} tickLine={false} axisLine={false} tickFormatter={(v) => `${v}%`} domain={[0, 100]} />
                <YAxis type="category" dataKey="quality" tick={{ fill: CHART_THEME.tickColor, fontSize: 11 }} tickLine={false} axisLine={false} width={60} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="win_rate" radius={[0, 3, 3, 0]}>
                  {byQuality.map((entry, i) => (
                    <Cell
                      key={i}
                      fill={entry.win_rate >= 65 ? CHART_THEME.green : entry.win_rate >= 50 ? CHART_THEME.yellow : CHART_THEME.red}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>

        {/* Best Symbols */}
        <Card>
          <CardHeader title="Best Symbols" subtitle="By win rate" />
          <div className="p-4 h-48">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={bySymbol.slice(0, 10)} layout="vertical" margin={{ left: 10, right: 20, top: 5, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.stroke} horizontal={false} />
                <XAxis type="number" tick={{ fill: CHART_THEME.tickColor, fontSize: 10 }} tickLine={false} axisLine={false} tickFormatter={(v) => `${v}%`} domain={[0, 100]} />
                <YAxis type="category" dataKey="sym" tick={{ fill: CHART_THEME.tickColor, fontSize: 10 }} tickLine={false} axisLine={false} width={55} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="win_rate" fill={CHART_THEME.blue} radius={[0, 3, 3, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>

        {/* Feature Importance */}
        <Card>
          <CardHeader title="Feature Importance" subtitle="Adaptive weight learning" />
          <div className="p-4 h-48">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={features} layout="vertical" margin={{ left: 10, right: 20, top: 5, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={CHART_THEME.stroke} horizontal={false} />
                <XAxis type="number" tick={{ fill: CHART_THEME.tickColor, fontSize: 10 }} tickLine={false} axisLine={false} />
                <YAxis type="category" dataKey="feature" tick={{ fill: CHART_THEME.tickColor, fontSize: 10 }} tickLine={false} axisLine={false} width={60} />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="weight" fill={CHART_THEME.yellow} radius={[0, 3, 3, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Card>

        {/* Session Performance Table */}
        <Card>
          <CardHeader title="Symbol Breakdown" />
          <div className="overflow-x-auto">
            <table className="w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-border text-text-dim uppercase tracking-wider">
                  <th className="text-left px-3 py-2">Symbol</th>
                  <th className="text-right px-3 py-2">Trades</th>
                  <th className="text-right px-3 py-2">Win %</th>
                  <th className="text-right px-3 py-2">Avg R:R</th>
                </tr>
              </thead>
              <tbody>
                {bySymbol.slice(0, 10).map((row) => (
                  <tr key={row.sym} className="border-b border-border/50 hover:bg-white/[0.02]">
                    <td className="px-3 py-2 text-text-primary font-bold">{row.sym}</td>
                    <td className="px-3 py-2 text-right text-text-dim">{row.total}</td>
                    <td className={clsx("px-3 py-2 text-right font-bold", row.win_rate >= 60 ? "text-accent-green" : row.win_rate >= 50 ? "text-accent-yellow" : "text-accent-red")}>
                      {row.win_rate.toFixed(1)}%
                    </td>
                    <td className={clsx("px-3 py-2 text-right", row.avg_rr >= 0 ? "text-accent-green" : "text-accent-red")}>
                      {row.avg_rr >= 0 ? "+" : ""}{row.avg_rr.toFixed(2)}R
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {bySymbol.length === 0 && (
              <div className="py-8 text-center text-text-dim text-sm">No trade data yet.</div>
            )}
          </div>
        </Card>
      </div>

      {/* Extra stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <StatCard label="Avg Win R:R" value={`+${perf?.avg_win_r?.toFixed(2) ?? 0}R`} color="green" />
        <StatCard label="Avg Loss R:R" value={`-${perf?.avg_loss_r?.toFixed(2) ?? 0}R`} color="red" />
        <StatCard label="Sortino Ratio" value={perf?.sortino?.toFixed(2) ?? "0.00"} />
        <StatCard label="Kelly Criterion" value={`${perf?.kelly?.toFixed(1) ?? 0}%`} sub="Optimal risk sizing" />
      </div>
    </div>
  );
}
