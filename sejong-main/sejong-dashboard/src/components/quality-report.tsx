"use client";

import { useQualityReport } from "@/lib/hooks";

export default function QualityReport({ onClose }: { onClose: () => void }) {
  const { data: checks, isLoading } = useQualityReport();

  const passCount = checks?.filter(c => c.passed).length ?? 0;
  const totalCount = checks?.length ?? 0;
  const allPass = passCount === totalCount && totalCount > 0;

  return (
    <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="bg-white rounded-2xl shadow-2xl max-w-lg w-full" onClick={e => e.stopPropagation()}>
        <div className={`p-5 rounded-t-2xl ${allPass ? "bg-green-600" : "bg-yellow-500"} text-white`}>
          <h2 className="text-lg font-bold">📊 품질 리포트 (납품 전 체크리스트)</h2>
          <p className="text-sm mt-1 opacity-90">{passCount}/{totalCount}개 항목 통과</p>
        </div>
        <div className="p-5 max-h-[60vh] overflow-y-auto">
          {isLoading && <div className="text-center py-8 text-gray-400">로딩 중...</div>}
          <div className="space-y-2">
            {checks?.map((c, i) => (
              <div key={i} className={`flex items-start gap-3 p-3 rounded-lg ${c.passed ? "bg-green-50" : "bg-red-50"}`}>
                <span className="text-lg flex-shrink-0">{c.passed ? "✅" : "❌"}</span>
                <div>
                  <div className="text-sm font-medium text-gray-800">{c.label}</div>
                  <div className="text-xs text-gray-500 mt-0.5">{c.detail}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
        <div className="p-4 border-t flex justify-end">
          <button onClick={onClose} className="px-6 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 text-sm font-medium">
            닫기
          </button>
        </div>
      </div>
    </div>
  );
}
