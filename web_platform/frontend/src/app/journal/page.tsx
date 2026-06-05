"use client";
import { useEffect, useState } from "react";
import { Card, CardHeader } from "@/components/ui/Card";
import { DirectionBadge } from "@/components/ui/DirectionBadge";
import { JournalEntry } from "@/lib/types";
import api from "@/lib/api";
import clsx from "clsx";

const TABS = ["Active", "Take Profit", "Stop Loss"];
const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export default function JournalPage() {
  const [entries, setEntries] = useState<JournalEntry[]>([]);
  const [activeTab, setActiveTab] = useState("Active");
  const [loading, setLoading] = useState(true);
  const [selectedEntry, setSelectedEntry] = useState<JournalEntry | null>(null);
  const [editMode, setEditMode] = useState(false);
  const [editData, setEditData] = useState({ entry_reason: "", exit_reason: "", lessons: "" });

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const res = await api.journal.list({ limit: 100 });
        setEntries(res.entries);
      } catch (e) {
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  // Filter by tab — Active = no exit_reason, TP = exit_reason contains "TP", SL = exit_reason contains "SL"
  const filteredEntries = entries.filter((e) => {
    if (activeTab === "Active") return !e.exit_reason;
    if (activeTab === "Take Profit") return e.exit_reason?.includes("TP") || e.exit_reason?.includes("profit");
    if (activeTab === "Stop Loss") return e.exit_reason?.includes("SL") || e.exit_reason?.includes("stop");
    return true;
  });

  const handleSaveEdit = async () => {
    if (!selectedEntry) return;
    try {
      await api.journal.update(selectedEntry.id, {
        ...editData,
        sym: selectedEntry.sym,
        direction: selectedEntry.direction,
      });
      const res = await api.journal.list({ limit: 100 });
      setEntries(res.entries);
      setEditMode(false);
    } catch (e: any) {
      alert(`Error saving: ${e.message}`);
    }
  };

  const fmtDate = (dt?: string) => {
    if (!dt) return "—";
    return new Date(dt).toLocaleString("en-GB", { month: "short", day: "numeric", year: "numeric", hour: "2-digit", minute: "2-digit" });
  };

  return (
    <div className="p-4 lg:p-6 space-y-4">
      <div>
        <h1 className="text-text-primary font-bold text-xl">Trade Journal</h1>
        <p className="text-text-dim text-xs font-mono mt-0.5">{entries.length} entries</p>
      </div>

      {/* Tabs */}
      <div className="flex gap-1">
        {TABS.map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={clsx(
              "px-4 py-2 text-sm font-medium rounded border transition-colors",
              activeTab === tab
                ? "bg-accent-blue/20 text-accent-blue border-accent-blue/40"
                : "bg-card text-text-dim border-border hover:text-text-primary"
            )}
          >
            {tab}
          </button>
        ))}
      </div>

      <div className="flex gap-4">
        {/* Journal list */}
        <div className="flex-1 space-y-3">
          {loading ? (
            <div className="text-text-dim text-sm text-center py-10">Loading journal...</div>
          ) : filteredEntries.length === 0 ? (
            <div className="text-text-dim text-sm text-center py-10">No journal entries in this category.</div>
          ) : (
            filteredEntries.map((entry) => (
              <Card
                key={entry.id}
                onClick={() => {
                  setSelectedEntry(entry);
                  setEditMode(false);
                  setEditData({
                    entry_reason: entry.entry_reason || "",
                    exit_reason: entry.exit_reason || "",
                    lessons: entry.lessons || "",
                  });
                }}
                className={clsx(selectedEntry?.id === entry.id && "border-accent-blue/40")}
              >
                <div className="p-4">
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <span className="text-text-primary font-mono font-bold">{entry.sym}</span>
                      <DirectionBadge direction={entry.direction as "LONG" | "SHORT"} />
                    </div>
                    <span className="text-text-dim text-xs font-mono">{fmtDate(entry.created_at)}</span>
                  </div>
                  {entry.entry_reason && (
                    <p className="text-text-dim text-xs mb-1">
                      <span className="text-accent-blue font-mono">Entry:</span> {entry.entry_reason.slice(0, 100)}
                      {entry.entry_reason.length > 100 ? "..." : ""}
                    </p>
                  )}
                  {entry.exit_reason && (
                    <p className="text-text-dim text-xs mb-1">
                      <span className="text-accent-yellow font-mono">Exit:</span> {entry.exit_reason.slice(0, 100)}
                    </p>
                  )}
                  {entry.lessons && (
                    <p className="text-text-dim text-xs">
                      <span className="text-accent-purple font-mono">Lessons:</span> {entry.lessons.slice(0, 80)}...
                    </p>
                  )}
                  {/* Chart thumbnail */}
                  {entry.chart_path && (
                    <img
                      src={`${API_BASE}/charts/${entry.trade_id}.png`}
                      alt="Chart"
                      className="mt-2 rounded border border-border max-h-24 object-cover"
                    />
                  )}
                </div>
              </Card>
            ))
          )}
        </div>

        {/* Entry detail / edit */}
        {selectedEntry && (
          <div className="w-96 flex-shrink-0">
            <Card className="p-4">
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-text-primary font-semibold">
                  {selectedEntry.sym} {selectedEntry.direction}
                </h3>
                <div className="flex gap-2">
                  <button
                    onClick={() => setEditMode(!editMode)}
                    className="text-xs text-accent-blue hover:underline"
                  >
                    {editMode ? "Cancel" : "Edit"}
                  </button>
                  <button onClick={() => setSelectedEntry(null)} className="text-xs text-text-dim hover:text-text-primary">
                    ✕
                  </button>
                </div>
              </div>

              {editMode ? (
                <div className="space-y-3">
                  <div>
                    <label className="text-text-dim text-xs font-mono uppercase tracking-wider block mb-1">Entry Reason</label>
                    <textarea
                      value={editData.entry_reason}
                      onChange={(e) => setEditData({ ...editData, entry_reason: e.target.value })}
                      className="w-full bg-gray-900 border border-border text-text-primary text-xs font-mono p-2 rounded resize-none h-20"
                    />
                  </div>
                  <div>
                    <label className="text-text-dim text-xs font-mono uppercase tracking-wider block mb-1">Exit Reason</label>
                    <textarea
                      value={editData.exit_reason}
                      onChange={(e) => setEditData({ ...editData, exit_reason: e.target.value })}
                      className="w-full bg-gray-900 border border-border text-text-primary text-xs font-mono p-2 rounded resize-none h-20"
                    />
                  </div>
                  <div>
                    <label className="text-text-dim text-xs font-mono uppercase tracking-wider block mb-1">Lessons Learned</label>
                    <textarea
                      value={editData.lessons}
                      onChange={(e) => setEditData({ ...editData, lessons: e.target.value })}
                      className="w-full bg-gray-900 border border-border text-text-primary text-xs font-mono p-2 rounded resize-none h-24"
                    />
                  </div>
                  <button
                    onClick={handleSaveEdit}
                    className="w-full py-2 bg-accent-green/20 hover:bg-accent-green/30 border border-accent-green/40 text-accent-green font-mono text-sm rounded"
                  >
                    Save Entry
                  </button>
                </div>
              ) : (
                <div className="space-y-3 text-xs font-mono">
                  <div>
                    <p className="text-text-dim uppercase tracking-wider mb-1">Entry Reason</p>
                    <p className="text-text-primary">{selectedEntry.entry_reason || "Not recorded"}</p>
                  </div>
                  <div>
                    <p className="text-text-dim uppercase tracking-wider mb-1">Exit Reason</p>
                    <p className="text-text-primary">{selectedEntry.exit_reason || "Not recorded"}</p>
                  </div>
                  <div>
                    <p className="text-text-dim uppercase tracking-wider mb-1">Lessons</p>
                    <p className="text-text-primary">{selectedEntry.lessons || "None recorded"}</p>
                  </div>
                  <p className="text-text-dim text-xs pt-2 border-t border-border">
                    Created: {fmtDate(selectedEntry.created_at)}
                  </p>
                </div>
              )}
            </Card>
          </div>
        )}
      </div>
    </div>
  );
}
