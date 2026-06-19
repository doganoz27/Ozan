import clsx from "clsx";

interface BadgeProps {
  children: React.ReactNode;
  variant?: "default" | "green" | "red" | "blue" | "yellow" | "purple" | "gray";
  size?: "sm" | "md";
  className?: string;
}

export function Badge({ children, variant = "default", size = "sm", className }: BadgeProps) {
  return (
    <span
      className={clsx(
        "inline-flex items-center font-mono font-semibold rounded uppercase tracking-wider",
        size === "sm" ? "px-2 py-0.5 text-xs" : "px-3 py-1 text-sm",
        {
          "bg-gray-800 text-gray-300 border border-gray-700": variant === "default",
          "bg-green-900/40 text-accent-green border border-accent-green/30": variant === "green",
          "bg-red-900/40 text-accent-red border border-accent-red/30": variant === "red",
          "bg-blue-900/40 text-accent-blue border border-accent-blue/30": variant === "blue",
          "bg-yellow-900/40 text-accent-yellow border border-accent-yellow/30": variant === "yellow",
          "bg-purple-900/40 text-accent-purple border border-accent-purple/30": variant === "purple",
          "bg-gray-900/60 text-text-dim border border-border": variant === "gray",
        },
        className
      )}
    >
      {children}
    </span>
  );
}
