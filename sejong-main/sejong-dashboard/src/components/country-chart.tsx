"use client";

import type { CountryData } from "@/lib/types";

export default function CountryChart({ data, onCountryClick }: {
  data: CountryData[];
  onCountryClick?: (code: string) => void;
}) {
  const maxCollected = Math.max(...data.map(d => d.collected));

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
      <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider mb-4">국가별 수집 현황</h3>
      <div className="space-y-2.5">
        {data.map((c) => {
          const barW = (c.collected / maxCollected) * 100;
          const isExcluded = c.excluded;
          return (
            <button
              key={c.code}
              onClick={() => !isExcluded && onCountryClick?.(c.code)}
              className={`w-full text-left group ${isExcluded ? "opacity-50" : "hover:bg-gray-50"} rounded-lg p-1.5 transition-colors`}
              disabled={isExcluded}
            >
              <div className="flex items-center gap-2 mb-1">
                <span className="text-base">{c.flag}</span>
                <span className="text-sm font-semibold text-gray-700 w-8">{c.code}</span>
                <div className="flex-1 h-5 bg-gray-100 rounded-full overflow-hidden relative">
                  <div
                    className={`h-full rounded-full transition-all ${isExcluded ? "bg-gray-300" : "bg-[#1e3a5f]"}`}
                    style={{ width: `${barW}%` }}
                  />
                </div>
                <span className="text-sm font-medium text-gray-700 w-16 text-right">{c.collected}건</span>
              </div>
              <div className="flex items-center gap-2 ml-8 text-[10px]">
                {isExcluded ? (
                  <span className="text-red-500 font-medium">⚠️ 이번 라운드 제외</span>
                ) : (
                  <>
                    <span className="text-gray-400">선별 {c.selected}/{c.quota}</span>
                    <span className="text-gray-300">·</span>
                    <span className="text-gray-400">{c.orgCount}개 기관</span>
                  </>
                )}
              </div>
            </button>
          );
        })}
      </div>
      <div className="mt-3 text-center">
        <button className="text-xs text-blue-600 hover:text-blue-800">더보기 (35개국) →</button>
      </div>
    </div>
  );
}
