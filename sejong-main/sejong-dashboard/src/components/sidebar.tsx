"use client";

import { usePathname } from "next/navigation";
import Link from "next/link";
import { useState } from "react";
import {
  LayoutDashboard,
  RefreshCw,
  Globe,
  FileText,
  BarChart3,
  CheckCircle,
  Settings,
  ChevronLeft,
  ChevronRight,
  Menu,
  X,
} from "lucide-react";

const NAV_ITEMS = [
  { href: "/", label: "대시보드", icon: LayoutDashboard },
  { href: "/pipeline", label: "파이프라인", icon: RefreshCw },
  { href: "/sites", label: "수집 사이트 관리", icon: Globe },
  { href: "/documents", label: "수집 문서 보기", icon: FileText },
  { href: "/analytics", label: "통계", icon: BarChart3 },
  { href: "/quality", label: "품질 보고서", icon: CheckCircle },
];

const BOTTOM_ITEMS = [
  { href: "/settings", label: "설정", icon: Settings },
];

export default function Sidebar() {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);

  const isActive = (href: string) => {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  };

  const navContent = (
    <div className="flex flex-col h-full">
      {/* Logo */}
      <div className="px-4 py-5 border-b border-white/10">
        <div className="flex items-center gap-3">
          <span className="text-2xl shrink-0">🏛️</span>
          {!collapsed && (
            <div className="overflow-hidden">
              <h1 className="text-sm font-bold text-white leading-tight">세종도서관</h1>
              <p className="text-[11px] text-white/50 leading-tight">해외정책자료 수집 시스템</p>
            </div>
          )}
        </div>
      </div>

      {/* Main Nav */}
      <nav className="flex-1 py-3 space-y-0.5 px-2 overflow-y-auto">
        {NAV_ITEMS.map((item) => {
          const Icon = item.icon;
          const active = isActive(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              onClick={() => setMobileOpen(false)}
              className={`
                flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors duration-200
                ${active
                  ? "bg-white/15 text-white border-l-[3px] border-white ml-0 pl-[9px]"
                  : "text-white/70 hover:bg-white/10 hover:text-white"
                }
              `}
              title={collapsed ? item.label : undefined}
            >
              <Icon size={20} className="shrink-0" />
              {!collapsed && <span>{item.label}</span>}
            </Link>
          );
        })}
      </nav>

      {/* Bottom Nav */}
      <div className="border-t border-white/10 py-3 px-2 space-y-0.5">
        {BOTTOM_ITEMS.map((item) => {
          const Icon = item.icon;
          const active = isActive(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              onClick={() => setMobileOpen(false)}
              className={`
                flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors duration-200
                ${active
                  ? "bg-white/15 text-white border-l-[3px] border-white ml-0 pl-[9px]"
                  : "text-white/70 hover:bg-white/10 hover:text-white"
                }
              `}
              title={collapsed ? item.label : undefined}
            >
              <Icon size={20} className="shrink-0" />
              {!collapsed && <span>{item.label}</span>}
            </Link>
          );
        })}

        {/* Collapse Toggle (desktop only) */}
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="hidden lg:flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm text-white/50 hover:text-white hover:bg-white/10 transition-colors w-full"
        >
          {collapsed ? <ChevronRight size={20} /> : <ChevronLeft size={20} />}
          {!collapsed && <span>접기</span>}
        </button>
      </div>
    </div>
  );

  return (
    <>
      {/* Mobile hamburger */}
      <button
        onClick={() => setMobileOpen(true)}
        className="lg:hidden fixed top-4 left-4 z-50 p-2 bg-[#1e3a5f] text-white rounded-lg shadow-lg"
      >
        <Menu size={20} />
      </button>

      {/* Mobile overlay */}
      {mobileOpen && (
        <div
          className="lg:hidden fixed inset-0 bg-black/50 z-40"
          onClick={() => setMobileOpen(false)}
        />
      )}

      {/* Mobile sidebar */}
      <aside
        className={`
          lg:hidden fixed top-0 left-0 h-full w-[240px] bg-[#1e3a5f] z-50
          transform transition-transform duration-300
          ${mobileOpen ? "translate-x-0" : "-translate-x-full"}
        `}
      >
        <button
          onClick={() => setMobileOpen(false)}
          className="absolute top-4 right-4 text-white/70 hover:text-white"
        >
          <X size={20} />
        </button>
        {navContent}
      </aside>

      {/* Desktop sidebar */}
      <aside
        className={`
          hidden lg:block shrink-0 h-screen sticky top-0 bg-[#1e3a5f]
          transition-[width] duration-300 ease-in-out
          ${collapsed ? "w-[64px]" : "w-[240px]"}
        `}
      >
        {navContent}
      </aside>
    </>
  );
}
