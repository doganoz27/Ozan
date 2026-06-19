import clsx from "clsx";

interface StatusBadgeProps {
  status: string;
  size?: "sm" | "md";
}

const STATUS_STYLES: Record<string, string> = {
  OPEN: "bg-blue-900/40 text-accent-blue border-accent-blue/30",
  APPROVED: "bg-blue-900/40 text-accent-blue border-accent-blue/30",
  TP: "bg-green-900/40 text-accent-green border-accent-green/30",
  SL: "bg-red-900/40 text-accent-red border-accent-red/30",
  EXPIRED: "bg-gray-800 text-text-dim border-gray-700",
  INVALIDATED: "bg-gray-800 text-text-dim border-gray-700",
  WATCHLIST: "bg-purple-900/40 text-accent-purple border-accent-purple/30",
  CLOSED: "bg-gray-800 text-text-dim border-gray-700",
};

const STATUS_ICONS: Record<string, string> = {
  OPEN: "●",
  APPROVED: "●",
  TP: "✓",
  SL: "✗",
  EXPIRED: "○",
  INVALIDATED: "○",
  WATCHLIST: "◎",
  CLOSED: "□",
};

export function StatusBadge({ status, size = "sm" }: StatusBadgeProps) {
  const style = STATUS_STYLES[status] || STATUS_STYLES["CLOSED"];
  const icon = STATUS_ICONS[status] || "?";
  return (
    <span
      className={clsx(
        "inline-flex items-center gap-1 font-mono font-semibold border rounded uppercase tracking-wider",
        size === "sm" ? "px-2 py-0.5 text-xs" : "px-3 py-1 text-sm",
        style
      )}
    >
      {icon} {status}
    </span>
  );
}
