"use client";

import type { SummaryData } from "@/lib/types";

function ProgressBar({ value, max, color }: { value: number; max: number; color: string }) {
  const pct = Math.min((value / max) * 100, 100);
  return (
    <div className="w-full h-2 bg-gray-200 rounded-full overflow-hidden mt-2">
      <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, backgroundColor: color }} />
    </div>
  );
}

function Card({ icon, label, value, sub, subColor, children }: {
  icon: string; label: string; value: string; sub: string; subColor?: string; children?: React.ReactNode;
}) {
  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5 flex flex-col gap-1 hover:shadow-md transition-shadow">
      <div className="flex items-center gap-2 text-sm text-gray-500">
        <span>{icon}</span>
        <span>{label}</span>
      </div>
      <div className="text-3xl font-bold text-gray-900 mt-1">{value}</div>
      {children}
      <div className={`text-xs mt-1 ${subColor || "text-gray-500"}`}>{sub}</div>
    </div>
  );
}

export default function SummaryCards({ data }: { data: SummaryData }) {
  const selPct = ((data.selectedActive / data.selectedTarget) * 100).toFixed(0);
  const selColor = data.selectedActive >= data.selectedTarget ? "#22c55e"
    : data.selectedActive >= data.selectedTarget * 0.9 ? "#3b82f6"
    : data.selectedActive >= data.selectedTarget * 0.7 ? "#eab308" : "#ef4444";

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      <Card
        icon="📊" label="총 수집" value={`${data.totalCollected.toLocaleString()}건`}
        sub={`기수집 ${data.completedDocs.toLocaleString()}건 제외 후 · ${data.jsonFileCount}개 JSON`}
      />
      <Card
        icon="✅" label="선별 완료" value={`${data.selectedActive}건`}
        sub={`${selPct}% 달성`}
        subColor={data.selectedActive >= data.selectedTarget ? "text-green-600" : "text-yellow-600"}
      >
        <ProgressBar value={data.selectedActive} max={data.selectedTarget} color={selColor} />
        <div className="text-xs text-gray-400 mt-0.5">/ {data.selectedTarget} 목표</div>
      </Card>
      <Card
        icon="🔍" label="검증 통과" value={`${data.verifiedCount}건`}
        sub={`날짜 ${data.excludedByDate}건, PDF ${data.excludedByPdf}건, 언어 ${data.excludedByLang}건 제외`}
        subColor="text-gray-500"
      >
        <div className="text-sm text-blue-600 font-medium">{data.verifiedRate}%</div>
      </Card>
      <Card
        icon="🌍" label="대상 국가" value={`${data.totalCountries}개국`}
        sub={`${data.totalOrgs}개 기관${data.gbExcluded ? " · GB/GAO 이번 라운드 제외" : ""}`}
      />
    </div>
  );
}
