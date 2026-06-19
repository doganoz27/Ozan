"use client";
import { useEffect, useState } from "react";
import { SignalTable } from "@/components/signals/SignalTable";
import { SignalDetail } from "@/components/signals/SignalDetail";
import { Card, CardHeader } from "@/components/ui/Card";
import { Signal } from "@/lib/types";
import api from "@/lib/api";
import clsx from "clsx";

const TABS = [
  { label: "All", status: "" },
  { label: "Approved", status: "APPROVED" },
  { label: "Open", status: "OPEN" },
  { label: "Watchlist", status: "WATCHLIST" },
  { label: "TP", status: "TP" },
  { label: "SL", status: "SL" },
  { label: "Expired", status: "EXPIRED" },
];

export default function SignalsPage() {
  const [signals, setSignals] = useState<Signal[]>([]);
  const [total, setTotal] = useState(0);
  const [activeTab, setActiveTab] = useState("");
  const [selected, setSelected] = useState<Signal | null>(null);
  const [loading, setLoading] = useState(true);
  const [offset, setOffset] = useState(0);
  const LIMIT = 50;

  const loadSignals = async (tab = activeTab, off = offset) => {
    setLoading(true);
    try {
      const res = await api.signals.list({
        status: tab || undefined,
        limit: LIMIT,
        offset: off,
      });
      setSignals(res.signals);
      setTotal(res.total);
    } catch (e) {
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadSignals(activeTab, 0);
    setOffset(0);
  }, [activeTab]);

  const handleEnterTrade = async (signalId: number) => {
    try {
      const res = await api.signals.enter(signalId);
      alert(`Trade entered! Trade ID: ${res.trade_id}`);
      loadSignals();
    } catch (e: any) {
      alert(`Error: ${e.message}`);
    }
  };

  return (
    <div className="p-4 lg:p-6 h-screen flex flex-col">
      <div className="mb-4">
        <h1 className="text-text-primary font-bold text-xl tracking-wide">Signal Center</h1>
        <p className="text-text-dim text-xs font-mono mt-0.5">{total} signals total</p>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 mb-4 flex-wrap">
        {TABS.map((tab) => (
          <button
            key={tab.label}
            onClick={() => setActiveTab(tab.status)}
            className={clsx(
              "px-3 py-1.5 text-xs font-mono font-semibold uppercase tracking-wider rounded border transition-colors",
              activeTab === tab.status
                ? "bg-accent-blue/20 text-accent-blue border-accent-blue/40"
                : "bg-gray-800 text-text-dim border-border hover:text-text-primary"
            )}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div className="flex-1 flex gap-4 overflow-hidden min-h-0">
        {/* Signal table */}
        <div className={clsx("overflow-auto", selected ? "w-1/2" : "w-full")}>
          <Card className="h-full">
            {loading ? (
              <div className="p-10 text-center text-text-dim">Loading signals...</div>
            ) : (
              <SignalTable
                signals={signals}
                onSelect={setSelected}
                selectedId={selected?.id}
              />
            )}
          </Card>

          {/* Pagination */}
          {total > LIMIT && (
            <div className="flex items-center justify-center gap-3 mt-3">
              <button
                disabled={offset === 0}
                onClick={() => { const o = Math.max(0, offset - LIMIT); setOffset(o); loadSignals(activeTab, o); }}
                className="px-3 py-1 text-xs font-mono border border-border text-text-dim hover:text-text-primary disabled:opacity-40 rounded"
              >
                Prev
              </button>
              <span className="text-text-dim text-xs font-mono">
                {offset + 1}–{Math.min(offset + LIMIT, total)} of {total}
              </span>
              <button
                disabled={offset + LIMIT >= total}
                onClick={() => { const o = offset + LIMIT; setOffset(o); loadSignals(activeTab, o); }}
                className="px-3 py-1 text-xs font-mono border border-border text-text-dim hover:text-text-primary disabled:opacity-40 rounded"
              >
                Next
              </button>
            </div>
          )}
        </div>

        {/* Signal detail panel */}
        {selected && (
          <div className="w-1/2 overflow-auto">
            <Card className="h-full p-4">
              <SignalDetail
                signal={selected}
                onEnter={handleEnterTrade}
                onClose={() => setSelected(null)}
              />
            </Card>
          </div>
        )}
      </div>
    </div>
  );
}
