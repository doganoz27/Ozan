"use client";
import { useEffect, useState } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { PriceCell } from "@/components/ui/PriceCell";
import { Badge } from "@/components/ui/Badge";
import { MarketSnapshot, MarketRegime } from "@/lib/types";
import api from "@/lib/api";

interface MarketOverviewProps {
  livePrices?: Record<string, { price: number; change_pct: number }>;
}

export function MarketOverview({ livePrices }: MarketOverviewProps) {
  const [snapshot, setSnapshot] = useState<MarketSnapshot | null>(null);
  const [regime, setRegime] = useState<MarketRegime | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const [snap, reg] = await Promise.all([api.market.snapshot(), api.market.regime()]);
        setSnapshot(snap);
        setRegime(reg);
      } catch (e) {}
    };
    load();
    const interval = setInterval(load, 30000);
    return () => clearInterval(interval);
  }, []);

  const mergedPrices = (items: any[]) =>
    items.map((item) => {
      const live = livePrices?.[item.sym];
      return {
        ...item,
        price: live?.price ?? item.price,
        change_pct: live?.change_pct ?? item.change_pct,
      };
    });

  const regimeVariant = regime?.regime === "Risk-On" ? "green" : regime?.regime === "Risk-Off" ? "red" : "yellow";

  return (
    <div className="space-y-4">
      {/* Regime + Session */}
      <Card>
        <CardHeader title="Market Regime" />
        <div className="p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Badge variant={regimeVariant} size="md">
              {regime?.regime ?? "Loading..."}
            </Badge>
            <div className="text-text-dim text-sm">
              {regime?.spy_change != null && (
                <span className={regime.spy_change >= 0 ? "text-accent-green" : "text-accent-red"}>
                  SPY {regime.spy_change >= 0 ? "+" : ""}{regime.spy_change?.toFixed(2)}%
                </span>
              )}
            </div>
          </div>
          <div className="text-right">
            <p className="text-text-dim text-xs uppercase tracking-wider">Session</p>
            <p className="text-text-primary font-mono text-sm font-semibold">{regime?.session ?? "—"}</p>
            <div className={`text-xs mt-0.5 ${regime?.session_active ? "text-accent-green" : "text-text-dim"}`}>
              {regime?.session_active ? "● Active" : "○ Closed"}
            </div>
          </div>
        </div>
      </Card>

      {/* Equities */}
      <Card>
        <CardHeader title="Equities" />
        <div className="divide-y divide-border">
          {mergedPrices(snapshot?.equities ?? []).map((item) => (
            <div key={item.sym} className="flex items-center justify-between px-4 py-2">
              <span className="text-text-primary font-mono text-sm font-semibold w-16">{item.sym}</span>
              <PriceCell price={item.price} change={item.change_pct} digits={2} />
            </div>
          ))}
          {!snapshot && <div className="px-4 py-6 text-text-dim text-sm text-center">Loading market data...</div>}
        </div>
      </Card>

      {/* Forex */}
      <Card>
        <CardHeader title="Forex" />
        <div className="divide-y divide-border">
          {mergedPrices(snapshot?.forex ?? []).map((item) => (
            <div key={item.sym} className="flex items-center justify-between px-4 py-2">
              <span className="text-text-primary font-mono text-sm font-semibold w-24">{item.sym}</span>
              <PriceCell price={item.price} change={item.change_pct} digits={5} />
            </div>
          ))}
        </div>
      </Card>

      {/* Metals & Oil */}
      <Card>
        <CardHeader title="Metals & Oil" />
        <div className="divide-y divide-border">
          {mergedPrices([...(snapshot?.metals ?? []), ...(snapshot?.oil ?? [])]).map((item) => (
            <div key={item.sym} className="flex items-center justify-between px-4 py-2">
              <span className="text-text-primary font-mono text-sm font-semibold w-24">{item.sym}</span>
              <PriceCell price={item.price} change={item.change_pct} digits={2} />
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}
