import clsx from "clsx";

interface DirectionBadgeProps {
  direction: "LONG" | "SHORT";
  size?: "sm" | "md";
}

export function DirectionBadge({ direction, size = "sm" }: DirectionBadgeProps) {
  const isLong = direction === "LONG";
  return (
    <span
      className={clsx(
        "inline-flex items-center gap-1 font-mono font-bold border rounded uppercase tracking-wider",
        size === "sm" ? "px-2 py-0.5 text-xs" : "px-3 py-1 text-sm",
        isLong
          ? "bg-green-900/40 text-accent-green border-accent-green/30"
          : "bg-red-900/40 text-accent-red border-accent-red/30"
      )}
    >
      {isLong ? "▲" : "▼"} {direction}
    </span>
  );
}
