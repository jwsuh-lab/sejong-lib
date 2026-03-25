"use client";

import { useQualityReport } from "@/lib/hooks";
import { CheckCircle, XCircle } from "lucide-react";

export default function QualityPage() {
  const { data, isLoading } = useQualityReport();

  return (
    <div className="p-6 lg:pl-8 space-y-6">
      <div>
        <h1 className="text-xl font-bold text-gray-900">품질 보고서</h1>
        <p className="text-sm text-gray-500 mt-0.5">수집 데이터 품질 검증 결과</p>
      </div>

      {isLoading ? (
        <div className="text-gray-400 text-sm">로딩 중...</div>
      ) : (
        <div className="bg-white rounded-xl shadow-sm border border-gray-100 divide-y divide-gray-100">
          {data?.map((check, i) => (
            <div key={i} className="flex items-center gap-4 px-6 py-4">
              {check.passed ? (
                <CheckCircle size={20} className="text-green-500 shrink-0" />
              ) : (
                <XCircle size={20} className="text-red-500 shrink-0" />
              )}
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-gray-900">{check.label}</p>
                <p className="text-xs text-gray-500 mt-0.5">{check.detail}</p>
              </div>
            </div>
          ))}
          {data?.length === 0 && (
            <div className="px-6 py-12 text-center text-gray-400 text-sm">검증 결과가 없습니다</div>
          )}
        </div>
      )}
    </div>
  );
}
