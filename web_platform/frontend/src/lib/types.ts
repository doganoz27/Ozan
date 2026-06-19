// TITAN PRIME — TypeScript Types

export interface Signal {
  id: number;
  sym: string;
  direction: "LONG" | "SHORT";
  quality: "A+" | "A" | "B+" | "WATCHLIST";
  score: number;
  confidence: number;
  entry: number;
  sl: number;
  tp: number;
  rr_t: number;
  act_rr?: number;
  status: "OPEN" | "TP" | "SL" | "EXPIRED" | "INVALIDATED" | "APPROVED" | "WATCHLIST";
  out_price?: number;
  regime?: string;
  contrarian_score: number;
  duration?: "Scalp" | "Intraday" | "Swing";
  f_ema: boolean;
  f_rsi: boolean;
  f_macd: boolean;
  f_sweep: boolean;
  f_ob: boolean;
  f_fvg: boolean;
  f_struct: boolean;
  narrative?: string;
  chart_path?: string;
  reasons: string[];
  neg_factors: string[];
  sm_notes: string[];
  trap_warnings: string[];
  consensus_view?: string;
  sm_view?: string;
  news_risk?: string;
  sizing: PositionSizing;
  hold_h?: number;
  created_at: string;
  closed_at?: string;
}

export interface PositionSizing {
  margin?: number;
  notional?: number;
  risk_amt?: number;
  leverage?: number;
  exp_loss?: number;
  exp_profit?: number;
  risk_pct?: number;
  asset_class?: string;
}

export interface Trade {
  id: number;
  signal_id?: number;
  sym: string;
  direction: "LONG" | "SHORT";
  entry_price: number;
  sl: number;
  tp: number;
  margin_gbp: number;
  risk_gbp: number;
  target_profit_gbp: number;
  leverage: number;
  status: "OPEN" | "TP" | "SL" | "CLOSED";
  pnl_gbp?: number;
  pnl_r?: number;
  current_price?: number;
  progress_pct?: number;
  opened_at: string;
  closed_at?: string;
}

export interface JournalEntry {
  id: number;
  trade_id?: number;
  sym: string;
  direction: string;
  entry_reason?: string;
  exit_reason?: string;
  lessons?: string;
  chart_path?: string;
  created_at: string;
}

export interface MarketPrice {
  sym: string;
  price?: number;
  change_pct?: number;
  high?: number;
  low?: number;
  updated_at?: string;
}

export interface MarketSnapshot {
  equities: MarketPrice[];
  forex: MarketPrice[];
  metals: MarketPrice[];
  oil: MarketPrice[];
}

export interface NewsItem {
  id?: number;
  headline: string;
  summary?: string;
  importance: number;
  risk_level?: string;
  bias?: string;
  bull_pct: number;
  bear_pct: number;
  affected_assets?: string[];
  published_at?: string;
  created_at?: string;
  datetime?: number;
}

export interface AccountState {
  balance_gbp: number;
  peak_gbp: number;
  drawdown_pct: number;
  total_trades: number;
  wins: number;
  losses: number;
  win_rate: number;
  avg_rr: number;
  profit_factor: number;
  sharpe: number;
  sortino: number;
}

export interface PerformanceStats {
  total: number;
  wins: number;
  losses: number;
  win_rate: number;
  avg_rr: number;
  profit_factor: number;
  sharpe: number;
  sortino: number;
  kelly: number;
  balance_gbp: number;
  peak_gbp: number;
  drawdown_pct: number;
  avg_win_r: number;
  avg_loss_r: number;
}

export interface FeatureWeight {
  feature: string;
  weight: number;
  win_rate?: number;
  sample_count: number;
  updated_at?: string;
}

export interface MarketRegime {
  regime: "Risk-On" | "Risk-Off" | "Neutral";
  regime_color: string;
  session: string;
  session_active: boolean;
  spy_change: number;
  gold_change: number;
  bull_news_count: number;
  bear_news_count: number;
  timestamp: string;
}

export interface WebSocketMessage {
  type: "price_update" | "new_signal" | "trade_update" | "news" | "pong";
  data?: any;
  ts?: number;
}

export interface EquityPoint {
  date: string;
  balance: number;
  trade?: number;
  sym?: string;
  rr?: number;
  status?: string;
}
