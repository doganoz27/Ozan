"use client";
import { useEffect, useState, useCallback } from "react";
import { StatCard } from "@/components/ui/StatCard";
import { MarketOverview } from "@/components/dashboard/MarketOverview";
import { SignalFeed } from "@/components/dashboard/SignalFeed";
import { ActiveTrades } from "@/components/dashboard/ActiveTrades";
import { PerformanceCard } from "@/components/dashboard/PerformanceCard";
import { Badge } from "@/components/ui/Badge";
import { getWebSocket } from "@/lib/websocket";
import api from "@/lib/api";

export default function DashboardPage() {
  const [livePrices, setLivePrices] = useState<Record<string, { price: number; change_pct: number }>>({});
  const [newSignal, setNewSignal] = useState<any>(null);
  const [tradeUpdate, setTradeUpdate] = useState<any>(null);
  const [stats, setStats] = useState<any>(null);
  const [balance, setBalance] = useState<number>(50.0);
  const [wsConnected, setWsConnected] = useState(false);
  const [openTradeCount, setOpenTradeCount] = useState(0);

  // WebSocket
  useEffect(() => {
    const ws = getWebSocket();
    ws.connect();

    const unsubscribe = ws.onMessage((msg) => {
      if (msg.type === "price_update" && msg.data) {
        setLivePrices((prev) => ({ ...prev, ...msg.data }));
        setWsConnected(true);
      } else if (msg.type === "new_signal" && msg.data) {
        setNewSignal(msg.data);
      } else if (msg.type === "trade_update" && msg.data) {
        setTradeUpdate(msg.data);
      } else if (msg.type === "pong") {
        setWsConnected(true);
      }
    });

    const pingInterval = setInterval(() => {
      setWsConnected(ws.isConnected);
    }, 5000);

    return () => {
      unsubscribe();
      clearInterval(pingInterval);
    };
  }, []);

  // Load stats
  useEffect(() => {
    const loadStats = async () => {
      try {
        const [perf, trades] = await Promise.all([
          api.analytics.performance(),
          api.trades.open(),
        ]);
        setStats(perf);
        setBalance(perf.balance_gbp ?? 50);
        setOpenTradeCount(trades.trades?.length ?? 0);
      } catch (e) {}
    };
    loadStats();
    const interval = setInterval(loadStats, 60000);
    return () => clearInterval(interval);
  }, []);

  const dailyPnl = balance - 50.0;
  const pnlColor = dailyPnl >= 0 ? "green" : "red";

  return (
    <div className="p-4 lg:p-6 space-y-6">
      {/* Top bar */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-text-primary font-bold text-xl tracking-wide">TITAN PRIME ELITE</h1>
          <p className="text-text-dim text-xs font-mono mt-0.5">Institutional Trading Intelligence</p>
        </div>
        <div className="flex items-center gap-3">
          <Badge variant={wsConnected ? "green" : "gray"} size="sm">
            {wsConnected ? "● LIVE" : "○ OFFLINE"}
          </Badge>
          <span className="text-text-dim text-xs font-mono hidden md:block">
            {new Date().toLocaleString("en-GB", { weekday: "short", hour: "2-digit", minute: "2-digit" })}
          </span>
        </div>
      </div>

      {/* KPI Row */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <StatCard
          label="Account Balance"
          value={`£${balance.toFixed(2)}`}
          sub="Trade212 CFD"
          color="blue"
        />
        <StatCard
          label="Daily P&L"
          value={`${dailyPnl >= 0 ? "+" : ""}£${dailyPnl.toFixed(2)}`}
          sub={`${((dailyPnl / 50) * 100).toFixed(1)}% today`}
          color={pnlColor}
        />
        <StatCard
          label="Win Rate"
          value={`${stats?.win_rate?.toFixed(1) ?? "0.0"}%`}
          sub={`${stats?.wins ?? 0} TP / ${stats?.losses ?? 0} SL`}
          color={stats?.win_rate >= 55 ? "green" : "yellow"}
        />
        <StatCard
          label="Open Trades"
          value={String(openTradeCount)}
          sub="Active positions"
          color="blue"
        />
      </div>

      {/* Main grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left: Market Overview */}
        <div className="lg:col-span-1 space-y-4">
          <MarketOverview livePrices={livePrices} />
        </div>

        {/* Center: Signal Feed + Active Trades */}
        <div className="lg:col-span-1 space-y-4">
          <SignalFeed newSignal={newSignal} />
          <ActiveTrades liveUpdate={tradeUpdate} />
        </div>

        {/* Right: Performance */}
        <div className="lg:col-span-1 space-y-4">
          <PerformanceCard />
        </div>
      </div>
    </div>
  );
}
