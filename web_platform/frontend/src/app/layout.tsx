import type { Metadata } from "next";
import "./globals.css";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Titan Prime Elite",
  description: "Institutional-grade trading intelligence platform",
};

const NAV_ITEMS = [
  { href: "/", label: "Dashboard", icon: "⬡" },
  { href: "/signals", label: "Signals", icon: "◈" },
  { href: "/trades", label: "Trades", icon: "◇" },
  { href: "/journal", label: "Journal", icon: "□" },
  { href: "/analytics", label: "Analytics", icon: "△" },
];

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="bg-bg text-text-primary min-h-screen flex">
        {/* Sidebar Navigation */}
        <nav className="w-16 lg:w-56 flex-shrink-0 bg-card border-r border-border flex flex-col fixed top-0 left-0 bottom-0 z-50">
          {/* Logo */}
          <div className="h-14 flex items-center px-4 border-b border-border">
            <div className="flex items-center gap-2">
              <span className="text-accent-green font-mono font-bold text-lg">T</span>
              <span className="hidden lg:block text-text-primary font-semibold text-sm tracking-wide">
                TITAN PRIME
              </span>
            </div>
          </div>

          {/* Nav items */}
          <div className="flex-1 py-4 overflow-y-auto">
            {NAV_ITEMS.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className="flex items-center gap-3 px-4 py-3 text-text-dim hover:text-text-primary hover:bg-white/[0.04] transition-colors group"
              >
                <span className="text-base w-6 text-center group-hover:text-accent-green transition-colors">
                  {item.icon}
                </span>
                <span className="hidden lg:block text-sm font-medium">{item.label}</span>
              </Link>
            ))}
          </div>

          {/* Footer */}
          <div className="p-4 border-t border-border hidden lg:block">
            <p className="text-text-dim text-xs font-mono">v2.0.0 Elite</p>
            <p className="text-text-muted text-xs font-mono">Trade212 CFD</p>
          </div>
        </nav>

        {/* Main content */}
        <main className="flex-1 ml-16 lg:ml-56 min-h-screen overflow-x-hidden">
          {children}
        </main>
      </body>
    </html>
  );
}
