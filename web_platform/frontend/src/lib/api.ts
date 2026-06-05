// TITAN PRIME — API Client

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

async function fetchAPI<T>(path: string, options?: RequestInit): Promise<T> {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, {
    headers: { "Content-Type": "application/json", ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`API error ${res.status}: ${err}`);
  }
  return res.json();
}

// ── Signals ───────────────────────────────────────────────────────────────────
export const api = {
  signals: {
    list: (params?: { status?: string; quality?: string; sym?: string; limit?: number; offset?: number }) => {
      const q = new URLSearchParams();
      if (params?.status) q.set("status", params.status);
      if (params?.quality) q.set("quality", params.quality);
      if (params?.sym) q.set("sym", params.sym);
      if (params?.limit) q.set("limit", String(params.limit));
      if (params?.offset) q.set("offset", String(params.offset));
      return fetchAPI<{ signals: any[]; total: number }>(`/api/signals?${q}`);
    },
    active: () => fetchAPI<{ signals: any[] }>("/api/signals/active"),
    stats: () => fetchAPI<any>("/api/signals/stats"),
    get: (id: number) => fetchAPI<any>(`/api/signals/${id}`),
    enter: (id: number) => fetchAPI<any>(`/api/signals/${id}/enter`, { method: "POST" }),
  },

  trades: {
    list: (params?: { status?: string; sym?: string }) => {
      const q = new URLSearchParams();
      if (params?.status) q.set("status", params.status);
      if (params?.sym) q.set("sym", params.sym);
      return fetchAPI<{ trades: any[] }>(`/api/trades?${q}`);
    },
    open: () => fetchAPI<{ trades: any[] }>("/api/trades/open"),
    get: (id: number) => fetchAPI<any>(`/api/trades/${id}`),
    close: (id: number) => fetchAPI<any>(`/api/trades/${id}/close`, { method: "POST" }),
  },

  journal: {
    list: (params?: { sym?: string; limit?: number }) => {
      const q = new URLSearchParams();
      if (params?.sym) q.set("sym", params.sym);
      if (params?.limit) q.set("limit", String(params.limit));
      return fetchAPI<{ entries: any[] }>(`/api/journal?${q}`);
    },
    get: (id: number) => fetchAPI<any>(`/api/journal/${id}`),
    create: (data: any) => fetchAPI<any>("/api/journal", { method: "POST", body: JSON.stringify(data) }),
    update: (id: number, data: any) => fetchAPI<any>(`/api/journal/${id}`, { method: "PUT", body: JSON.stringify(data) }),
    delete: (id: number) => fetchAPI<any>(`/api/journal/${id}`, { method: "DELETE" }),
  },

  analytics: {
    performance: () => fetchAPI<any>("/api/analytics/performance"),
    bySymbol: () => fetchAPI<any>("/api/analytics/by_symbol"),
    byQuality: () => fetchAPI<any>("/api/analytics/by_quality"),
    equityCurve: () => fetchAPI<any>("/api/analytics/equity_curve"),
    features: () => fetchAPI<any>("/api/analytics/features"),
  },

  market: {
    snapshot: () => fetchAPI<any>("/api/market/snapshot"),
    regime: () => fetchAPI<any>("/api/market/regime"),
    news: (limit?: number, minImportance?: number) => {
      const q = new URLSearchParams();
      if (limit) q.set("limit", String(limit));
      if (minImportance) q.set("min_importance", String(minImportance));
      return fetchAPI<any>(`/api/market/news?${q}`);
    },
  },

  news: {
    list: (params?: { limit?: number; min_importance?: number; bias?: string }) => {
      const q = new URLSearchParams();
      if (params?.limit) q.set("limit", String(params.limit));
      if (params?.min_importance) q.set("min_importance", String(params.min_importance));
      if (params?.bias) q.set("bias", params.bias);
      return fetchAPI<any>(`/api/news?${q}`);
    },
    highImpact: () => fetchAPI<any>("/api/news/high-impact"),
  },

  health: () => fetchAPI<any>("/health"),
};

export default api;
