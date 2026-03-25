"use client";

import { useState, useEffect } from "react";
import { useSettings, useUpdateSetting } from "@/lib/hooks";

const SETTING_DEFS = [
  { key: "target_count", label: "목표 건수", type: "number" },
  { key: "cutoff_date", label: "기준 날짜 (CUTOFF)", type: "date" },
  { key: "min_pdf_size_kb", label: "최소 PDF 크기 (KB)", type: "number" },
  { key: "org_cap_percent", label: "기관당 상한 (%)", type: "number" },
  { key: "min_title_length", label: "최소 제목 길이 (자)", type: "number" },
  { key: "min_keyword_count", label: "최소 키워드 수", type: "number" },
  { key: "completed_count", label: "기수집 문서 수", type: "number" },
];

const TOGGLE_DEFS = [
  { key: "gb_excluded", label: "GB 제외" },
  { key: "gao_excluded", label: "GAO 제외" },
];

export default function SettingsDrawer({ onClose }: { onClose: () => void }) {
  const { data: settings } = useSettings();
  const updateSetting = useUpdateSetting();
  const [localValues, setLocalValues] = useState<Record<string, string>>({});

  useEffect(() => {
    if (settings) {
      const map: Record<string, string> = {};
      settings.forEach(s => { map[s.key] = s.value; });
      setLocalValues(map);
    }
  }, [settings]);

  const handleChange = (key: string, value: string) => {
    setLocalValues(prev => ({ ...prev, [key]: value }));
  };

  const handleSave = (key: string) => {
    const value = localValues[key];
    if (value !== undefined) {
      updateSetting.mutate({ key, value });
    }
  };

  const handleToggle = (key: string) => {
    const current = localValues[key] === "true";
    const newValue = (!current).toString();
    setLocalValues(prev => ({ ...prev, [key]: newValue }));
    updateSetting.mutate({ key, value: newValue });
  };

  return (
    <div className="fixed inset-0 z-50 flex justify-end" onClick={onClose}>
      <div className="absolute inset-0 bg-black/30" />
      <div className="relative bg-white w-full max-w-md shadow-2xl overflow-y-auto" onClick={e => e.stopPropagation()}>
        <div className="sticky top-0 bg-[#1e3a5f] text-white p-5 flex items-center justify-between">
          <h2 className="text-lg font-bold">⚙️ 설정</h2>
          <button onClick={onClose} className="text-white/80 hover:text-white text-xl">✕</button>
        </div>
        <div className="p-5 space-y-4">
          {SETTING_DEFS.map(def => (
            <div key={def.key} className="flex items-center justify-between gap-4">
              <label className="text-sm text-gray-700 flex-shrink-0">{def.label}</label>
              <input
                type={def.type}
                value={localValues[def.key] ?? ""}
                onChange={e => handleChange(def.key, e.target.value)}
                onBlur={() => handleSave(def.key)}
                className="px-3 py-1.5 border border-gray-200 rounded-lg text-sm w-40 text-right focus:outline-none focus:ring-2 focus:ring-blue-300"
              />
            </div>
          ))}
          {TOGGLE_DEFS.map(def => (
            <div key={def.key} className="flex items-center justify-between">
              <span className="text-sm text-gray-700">{def.label}</span>
              <div
                onClick={() => handleToggle(def.key)}
                className={`w-10 h-6 rounded-full flex items-center p-0.5 cursor-pointer transition-colors ${
                  localValues[def.key] === "true" ? "bg-green-500 justify-end" : "bg-gray-300 justify-start"
                }`}
              >
                <div className="w-5 h-5 rounded-full bg-white shadow" />
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
