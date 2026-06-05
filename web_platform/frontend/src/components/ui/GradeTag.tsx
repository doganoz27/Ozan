import clsx from "clsx";

interface GradeTagProps {
  grade: string;
  size?: "sm" | "md";
}

const GRADE_STYLES: Record<string, string> = {
  "A+": "bg-yellow-500/20 text-yellow-300 border-yellow-500/40",
  A: "bg-green-500/20 text-green-300 border-green-500/40",
  "B+": "bg-blue-500/20 text-blue-300 border-blue-500/40",
  B: "bg-gray-500/20 text-gray-300 border-gray-500/40",
  WATCHLIST: "bg-purple-500/20 text-purple-300 border-purple-500/40",
};

const GRADE_ICONS: Record<string, string> = {
  "A+": "★",
  A: "✓",
  "B+": "○",
  WATCHLIST: "◎",
};

export function GradeTag({ grade, size = "sm" }: GradeTagProps) {
  const style = GRADE_STYLES[grade] || GRADE_STYLES["WATCHLIST"];
  const icon = GRADE_ICONS[grade] || "";
  return (
    <span
      className={clsx(
        "inline-flex items-center gap-1 font-mono font-bold border rounded uppercase tracking-wider",
        size === "sm" ? "px-2 py-0.5 text-xs" : "px-3 py-1 text-sm",
        style
      )}
    >
      {icon} {grade}
    </span>
  );
}
