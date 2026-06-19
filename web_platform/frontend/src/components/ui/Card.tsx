import clsx from "clsx";

interface CardProps {
  children: React.ReactNode;
  className?: string;
  glowing?: boolean;
  onClick?: () => void;
}

export function Card({ children, className, glowing, onClick }: CardProps) {
  return (
    <div
      onClick={onClick}
      className={clsx(
        "bg-card border border-border rounded-lg",
        glowing && "shadow-[0_0_15px_rgba(0,255,136,0.07)]",
        onClick && "cursor-pointer hover:border-gray-600 transition-colors",
        className
      )}
    >
      {children}
    </div>
  );
}

interface CardHeaderProps {
  title: string;
  subtitle?: string;
  right?: React.ReactNode;
  className?: string;
}

export function CardHeader({ title, subtitle, right, className }: CardHeaderProps) {
  return (
    <div className={clsx("flex items-center justify-between px-4 py-3 border-b border-border", className)}>
      <div>
        <h3 className="text-text-primary font-semibold text-sm uppercase tracking-wider">{title}</h3>
        {subtitle && <p className="text-text-dim text-xs mt-0.5">{subtitle}</p>}
      </div>
      {right && <div>{right}</div>}
    </div>
  );
}
