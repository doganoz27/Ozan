"use client";
import { useEffect, useState } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { GradeTag } from "@/components/ui/GradeTag";
import { DirectionBadge } from "@/components/ui/DirectionBadge";
import { StatusBadge } from "@/components/ui/StatusBadge";
import { Signal } from "@/lib/types";
import api from "@/lib/api";
import Link from "next/link";

interface SignalFeedProps {
  newSignal?: any;
}

export function SignalFeed({ newSignal }: SignalFeedProps) {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [loading, setLoading] = useState(true);

  const loadSignals = async () => {
    try {
      const res = await api.signals.list({ limit: 5 });
      setSignals(res.signals);
    } catch (e) {
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadSignals();
  }, []);

  useEffect(() => {
    if (newSignal) {
      setSignals((prev) => [newSignal, ...prev.slice(0, 4)]);
    }
  }, [newSignal]);

  const formatPrice = (p: number) => {
    if (!p) return "—";
    if (p > 100) return p.toFixed(2);
    return p.toFixed(5);
  };

  return (
    <Card>
      <CardHeader
        title="Live Signal Feed"
        subtitle="Latest 5 signals"
        right={
          <Link href="/signals" className="text-accent-blue text-xs hover:underline">
            View All
          </Link>
        }
      />
      {loading ? (
        <div className="p-6 text-text-dim text-sm text-center">Loading signals...</div>
      ) : signals.length === 0 ? (
        <div className="p-6 text-text-dim text-sm text-center">No signals yet. Analysis runs every 5 minutes.</div>
      ) : (
        <div className="divide-y divide-border">
          {signals.map((sig) => (
            <div key={sig.id} className="px-4 py-3 hover:bg-white/[0.02] transition-colors">
              <div className="flex items-center justify-between mb-1">
                <div className="flex items-center gap-2">
                  <span className="text-text-primary font-mono font-bold text-sm">{sig.sym}</span>
                  <DirectionBadge direction={sig.direction} />
                  <GradeTag grade={sig.quality} />
                </div>
                <StatusBadge status={sig.status} />
              </div>
              <div className="flex items-center gap-4 text-xs text-text-dim font-mono mt-1">
                <span>Entry: <span className="text-text-primary">{formatPrice(sig.entry)}</span></span>
                <span>SL: <span className="text-accent-red">{formatPrice(sig.sl)}</span></span>
                <span>TP: <span className="text-accent-green">{formatPrice(sig.tp)}</span></span>
                <span>R:R 1:<span className="text-accent-yellow">{sig.rr_t?.toFixed(1)}</span></span>
                <span className="text-accent-blue">{sig.score?.toFixed(0)}/100</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}
