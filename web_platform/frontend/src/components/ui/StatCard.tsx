import clsx from "clsx";

interface StatCardProps {
  label: string;
  value: string | number;
  sub?: string;
  color?: "green" | "red" | "blue" | "yellow" | "default";
  className?: string;
}

export function StatCard({ label, value, sub, color = "default", className }: StatCardProps) {
  const valueColor = {
    green: "text-accent-green",
    red: "text-accent-red",
    blue: "text-accent-blue",
    yellow: "text-accent-yellow",
    default: "text-text-primary",
  }[color];

  return (
    <div className={clsx("bg-card border border-border rounded-lg p-4", className)}>
      <p className="text-text-dim text-xs font-mono uppercase tracking-widest mb-1">{label}</p>
      <p className={clsx("text-2xl font-bold font-mono", valueColor)}>{value}</p>
      {sub && <p className="text-text-dim text-xs mt-1">{sub}</p>}
    </div>
  );
}
