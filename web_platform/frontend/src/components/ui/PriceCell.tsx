import clsx from "clsx";

interface PriceCellProps {
  price?: number;
  change?: number;
  digits?: number;
}

export function PriceCell({ price, change, digits = 4 }: PriceCellProps) {
  const isPositive = (change ?? 0) >= 0;
  return (
    <div className="text-right">
      <div className="text-text-primary font-mono text-sm font-medium">
        {price != null ? price.toFixed(digits) : "—"}
      </div>
      {change != null && (
        <div className={clsx("font-mono text-xs", isPositive ? "text-accent-green" : "text-accent-red")}>
          {isPositive ? "+" : ""}{change.toFixed(2)}%
        </div>
      )}
    </div>
  );
}
