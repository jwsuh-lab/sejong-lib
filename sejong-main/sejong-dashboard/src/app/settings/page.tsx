"use client";

import { useSettings, useUpdateSetting } from "@/lib/hooks";
import { useState, useEffect } from "react";

const SETTING_LABELS: Record<string, string> = {
  target_count: "목표 선별 건수",
  cutoff_date: "기준 날짜 (cutoff)",
  min_pdf_size_kb: "최소 PDF 크기 (KB)",
  min_title_length: "최소 제목 길이",
  min_keyword_count: "최소 키워드 수",
  org_cap_percent: "기관별 상한 비율 (%)",
  completed_count: "기완료 건수",
  gb_excluded: "영국(GB) 제외",
  gao_excluded: "GAO 제외",
};

const BOOL_KEYS = ["gb_excluded", "gao_excluded"];

export default function SettingsPage() {
  const { data: settings, isLoading } = useSettings();
  const updateSetting = useUpdateSetting();
  const [localValues, setLocalValues] = useState<Record<string, string>>({});

  useEffect(() => {
    if (settings) {
      const map: Record<string, string> = {};
      settings.forEach((s) => (map[s.key] = s.value));
      setLocalValues(map);
    }
  }, [settings]);

  const handleSave = (key: string, value: string) => {
    updateSetting.mutate({ key, value });
  };

  return (
    <div className="p-6 lg:pl-8 space-y-6">
      <div>
        <h1 className="text-xl font-bold text-gray-900">설정</h1>
        <p className="text-sm text-gray-500 mt-0.5">파이프라인 파라미터 및 시스템 설정</p>
      </div>

      {isLoading ? (
        <div className="text-gray-400 text-sm">로딩 중...</div>
      ) : (
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 divide-y divide-gray-100 max-w-2xl">
          {Object.entries(localValues).map(([key, value]) => (
            <div key={key} className="flex items-center justify-between px-6 py-4">
              <label className="text-sm font-medium text-gray-700 min-w-[180px]">
                {SETTING_LABELS[key] || key}
              </label>
              {BOOL_KEYS.includes(key) ? (
                <button
                  onClick={() => {
                    const next = value === "true" ? "false" : "true";
                    setLocalValues((v) => ({ ...v, [key]: next }));
                    handleSave(key, next);
                  }}
                  className={`
                    relative w-11 h-6 rounded-full transition-colors
                    ${value === "true" ? "bg-blue-500" : "bg-gray-300"}
                  `}
                >
                  <span
                    className={`
                      absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform
                      ${value === "true" ? "translate-x-5" : "translate-x-0"}
                    `}
                  />
                </button>
              ) : (
                <input
                  type="text"
                  value={value}
                  onChange={(e) =>
                    setLocalValues((v) => ({ ...v, [key]: e.target.value }))
                  }
                  onBlur={() => handleSave(key, value)}
                  className="w-40 px-3 py-1.5 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-500"
                />
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
