"use client";

import { useState } from "react";
import PipelineStepper from "@/components/pipeline-stepper";
import { usePipelineHistory } from "@/lib/hooks";

const STATUS_BADGES: Record<string, { text: string; className: string }> = {
  completed: { text: "완료", className: "bg-green-100 text-green-700" },
  partial: { text: "부분", className: "bg-yellow-100 text-yellow-700" },
  failed: { text: "실패", className: "bg-red-100 text-red-700" },
  stopped: { text: "중지", className: "bg-gray-100 text-gray-600" },
  running: { text: "실행중", className: "bg-blue-100 text-blue-700" },
};

function formatDuration(start: string | null, end: string | null): string {
  if (!start || !end) return "-";
  const ms = new Date(end).getTime() - new Date(start).getTime();
  if (ms < 0) return "-";
  const secs = Math.floor(ms / 1000);
  const mins = Math.floor(secs / 60);
  const s = secs % 60;
  return mins > 0 ? `${mins}분 ${s}초` : `${s}초`;
}

function formatDateTime(iso: string | null): string {
  if (!iso) return "-";
  const d = new Date(iso);
  return `${d.getMonth() + 1}/${d.getDate()} ${d.getHours().toString().padStart(2, "0")}:${d.getMinutes().toString().padStart(2, "0")}`;
}

export default function PipelinePage() {
  const { data: history, isLoading } = usePipelineHistory();
  const [showAll, setShowAll] = useState(false);

  const items = history || [];
  const displayed = showAll ? items : items.slice(0, 10);

  return (
    <div className="p-6 lg:pl-8 space-y-6">
      <div>
        <h1 className="text-xl font-bold text-gray-900">파이프라인</h1>
        <p className="text-sm text-gray-500 mt-0.5">5단계 수집 파이프라인 실행 및 모니터링</p>
      </div>
      <PipelineStepper />

      {/* Execution History */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
        <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider mb-4">실행 이력</h3>

        {isLoading ? (
          <div className="text-sm text-gray-400 text-center py-8">로딩 중...</div>
        ) : items.length === 0 ? (
          <div className="text-sm text-gray-400 text-center py-8">실행 이력이 없습니다</div>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs text-gray-500 border-b border-gray-100">
                    <th className="pb-2 pr-3 font-medium">#</th>
                    <th className="pb-2 pr-3 font-medium">시작</th>
                    <th className="pb-2 pr-3 font-medium">소요시간</th>
                    <th className="pb-2 pr-3 font-medium">상태</th>
                    <th className="pb-2 pr-3 font-medium text-right">수집</th>
                    <th className="pb-2 pr-3 font-medium text-right">선별</th>
                    <th className="pb-2 pr-3 font-medium text-right">검증</th>
                    <th className="pb-2 font-medium text-right">제외</th>
                  </tr>
                </thead>
                <tbody>
                  {displayed.map((run) => {
                    const badge = STATUS_BADGES[run.status] || STATUS_BADGES.stopped;
                    return (
                      <tr key={run.id} className="border-b border-gray-50 hover:bg-gray-50 transition-colors">
                        <td className="py-2.5 pr-3 text-gray-400">{run.id}</td>
                        <td className="py-2.5 pr-3 text-gray-700">{formatDateTime(run.startedAt)}</td>
                        <td className="py-2.5 pr-3 text-gray-500">{formatDuration(run.startedAt, run.finishedAt)}</td>
                        <td className="py-2.5 pr-3">
                          <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${badge.className}`}>
                            {badge.text}
                          </span>
                          {run.errorMessage && (
                            <span className="ml-1.5 text-[10px] text-red-500" title={run.errorMessage}>
                              (!)
                            </span>
                          )}
                        </td>
                        <td className="py-2.5 pr-3 text-right font-medium text-gray-700">{run.totalCollected.toLocaleString()}</td>
                        <td className="py-2.5 pr-3 text-right font-medium text-gray-700">{run.totalSelected.toLocaleString()}</td>
                        <td className="py-2.5 pr-3 text-right font-medium text-green-700">{run.totalVerified.toLocaleString()}</td>
                        <td className="py-2.5 text-right font-medium text-red-600">{run.totalExcluded.toLocaleString()}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {items.length > 10 && (
              <button
                onClick={() => setShowAll(!showAll)}
                className="mt-3 text-xs text-blue-600 hover:text-blue-800 font-medium"
              >
                {showAll ? "최근 10건만 보기" : `전체 ${items.length}건 보기`}
              </button>
            )}
          </>
        )}
      </div>
    </div>
  );
}
