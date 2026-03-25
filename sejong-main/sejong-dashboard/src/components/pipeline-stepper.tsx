"use client";

import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import type { StepStatus, LogEntry } from "@/lib/types";
import { useQueryClient } from "@tanstack/react-query";
import {
  usePipelineStatus,
  useRunPipeline,
  useRunPipelineStep,
  useStopPipeline,
  useQualityReport,
} from "@/lib/hooks";
import { subscribeLogs, downloadExcel } from "@/lib/api";

const STEP_LABELS: Record<number, { label: string; detail: string }> = {
  1: { label: "크롤링", detail: "US, SE, SG, 범용 31개국 (GB 제외)" },
  2: { label: "선별", detail: "3-Pass 알고리즘 + 영어 필터" },
  3: { label: "보강", detail: "날짜·PDF·저자·요약·키워드·ISBN/CCL" },
  4: { label: "검증", detail: "3중 전수 검증 (날짜·PDF·언어)" },
  5: { label: "출력", detail: "25컬럼 Excel (맑은 고딕, #2F5496)" },
};

const STATUS_ICONS: Record<StepStatus, { icon: string; color: string }> = {
  pending: { icon: "⏳", color: "bg-gray-200 text-gray-500" },
  running: { icon: "⟳", color: "bg-blue-100 text-blue-700 animate-pulse" },
  completed: { icon: "✓", color: "bg-green-100 text-green-700" },
  failed: { icon: "✕", color: "bg-red-100 text-red-700" },
  partial: { icon: "!", color: "bg-yellow-100 text-yellow-700" },
};

function StepBadge({ step, status, isCurrent, progress }: {
  step: number; status: StepStatus; isCurrent: boolean; progress: number;
}) {
  const { icon, color } = STATUS_ICONS[status];
  const { label, detail } = STEP_LABELS[step];

  return (
    <div className="flex flex-col items-center gap-1.5 min-w-[100px]">
      <div className={`w-10 h-10 rounded-full flex items-center justify-center text-sm font-bold ${color} transition-all`}>
        {icon}
      </div>
      <span className="text-sm font-semibold text-gray-800">{step}. {label}</span>
      <span className="text-[10px] text-gray-500 text-center leading-tight max-w-[120px]">{detail}</span>
      {isCurrent && status === "running" && (
        <div className="w-full mt-1">
          <div className="w-full h-1.5 bg-gray-200 rounded-full overflow-hidden">
            <div
              className="h-full bg-blue-500 rounded-full transition-all"
              style={{ width: `${progress}%` }}
            />
          </div>
          <span className="text-[10px] text-blue-600 font-medium">{progress}%</span>
        </div>
      )}
    </div>
  );
}

function Connector({ active }: { active: boolean }) {
  return (
    <div className={`hidden md:block flex-1 h-0.5 mt-5 min-w-[20px] ${active ? "bg-green-400" : "bg-gray-200"}`} />
  );
}

const STATUS_BADGES: Record<string, { text: string; className: string }> = {
  completed: { text: "완료", className: "bg-green-100 text-green-700" },
  partial: { text: "부분 완료", className: "bg-yellow-100 text-yellow-700" },
  failed: { text: "실패", className: "bg-red-100 text-red-700" },
  stopped: { text: "중지됨", className: "bg-gray-100 text-gray-600" },
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

const LOG_COLORS: Record<string, string> = {
  info: "text-gray-500",
  success: "text-green-600",
  warning: "text-yellow-600",
  error: "text-red-600",
};

export default function PipelineStepper() {
  const [showDropdown, setShowDropdown] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const logPanelRef = useRef<HTMLDivElement>(null);

  const { data: state } = usePipelineStatus();
  const runAll = useRunPipeline();
  const runStep = useRunPipelineStep();
  const stop = useStopPipeline();
  const qc = useQueryClient();
  const prevRunning = useRef(false);

  // SSE 로그 구독
  useEffect(() => {
    const unsub = subscribeLogs((log) => {
      setLogs((prev) => [log, ...prev].slice(0, 200));
    });
    return unsub;
  }, []);

  const { data: qualityChecks } = useQualityReport();

  // 파이프라인 완료 시 대시보드 자동 갱신
  useEffect(() => {
    const isRunning = state?.isRunning ?? false;
    if (prevRunning.current && !isRunning) {
      qc.invalidateQueries({ queryKey: ["summary"] });
      qc.invalidateQueries({ queryKey: ["countries"] });
      qc.invalidateQueries({ queryKey: ["categories"] });
      qc.invalidateQueries({ queryKey: ["documents"] });
      qc.invalidateQueries({ queryKey: ["quality-report"] });
      qc.invalidateQueries({ queryKey: ["pipeline-history"] });
    }
    prevRunning.current = isRunning;
  }, [state?.isRunning, qc]);

  const steps = [1, 2, 3, 4, 5] as const;
  const isRunning = state?.isRunning ?? false;
  const currentStep = state?.currentStep ?? null;
  const progress = state?.progress ?? 0;

  const stepStatuses: Record<number, StepStatus> = state?.steps
    ? Object.fromEntries(Object.entries(state.steps).map(([k, v]) => [Number(k), v as StepStatus]))
    : { 1: "pending", 2: "pending", 3: "pending", 4: "pending", 5: "pending" };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-6">
      <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider mb-4">파이프라인 진행 상태</h3>

      {/* Stepper */}
      <div className="flex items-start justify-between gap-1 mb-5 overflow-x-auto pb-2">
        {steps.map((s, i) => (
          <div key={s} className="contents">
            <StepBadge step={s} status={stepStatuses[s]} isCurrent={currentStep === s} progress={progress} />
            {i < 4 && <Connector active={stepStatuses[s] === "completed"} />}
          </div>
        ))}
      </div>

      {/* Log Panel */}
      <div ref={logPanelRef} className="bg-gray-50 rounded-lg border border-gray-200 p-3 h-[200px] overflow-y-auto log-font text-xs">
        {logs.length === 0 && (
          <div className="text-gray-400 text-center py-8">파이프라인 실행 시 로그가 여기에 표시됩니다</div>
        )}
        {logs.map((log, i) => (
          <div key={i} className={`py-0.5 ${LOG_COLORS[log.level] || "text-gray-500"}`}>
            <span className="text-gray-400 mr-2">{log.timestamp}</span>
            <span className="text-gray-500 mr-2">[{log.step}]</span>
            <span>{log.message}</span>
          </div>
        ))}
      </div>

      {/* Execution Summary */}
      {!isRunning && state?.finalStatus && state.finalStatus !== "running" && (
        <div className="mt-4 bg-gray-50 rounded-lg border border-gray-200 p-4">
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-3">
              <h4 className="text-sm font-semibold text-gray-700">실행 결과</h4>
              <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${STATUS_BADGES[state.finalStatus]?.className || "bg-gray-100 text-gray-600"}`}>
                {STATUS_BADGES[state.finalStatus]?.text || state.finalStatus}
              </span>
              <span className="text-xs text-gray-400">
                {formatDuration(state.startedAt, state.finishedAt)}
              </span>
            </div>
            {state.hasExcelFile && (
              <button
                onClick={() => downloadExcel()}
                className="text-xs px-3 py-1.5 bg-[#1e3a5f] text-white rounded-lg hover:bg-[#2f5496] transition-colors"
              >
                Excel 다운로드
              </button>
            )}
          </div>

          <div className="grid grid-cols-4 gap-3 mb-3">
            {[
              { label: "수집", value: state.totalCollected, color: "text-blue-700 bg-blue-50" },
              { label: "선별", value: state.totalSelected, color: "text-indigo-700 bg-indigo-50" },
              { label: "검증", value: state.totalVerified, color: "text-green-700 bg-green-50" },
              { label: "제외", value: state.totalExcluded, color: "text-red-700 bg-red-50" },
            ].map((s) => (
              <div key={s.label} className={`rounded-lg p-3 text-center ${s.color}`}>
                <div className="text-lg font-bold">{s.value.toLocaleString()}</div>
                <div className="text-[10px] font-medium mt-0.5">{s.label}</div>
              </div>
            ))}
          </div>

          {state.errorMessage && (
            <div className="text-xs text-red-600 bg-red-50 rounded-lg p-2 mb-3">
              {state.errorMessage}
            </div>
          )}

          {qualityChecks && qualityChecks.length > 0 && (
            <div className="flex items-center gap-2">
              {(() => {
                const passed = qualityChecks.filter((c) => c.passed).length;
                const total = qualityChecks.length;
                const allPassed = passed === total;
                return (
                  <Link href="/quality" className={`text-xs font-medium px-2 py-1 rounded-full hover:opacity-80 transition-opacity ${allPassed ? "bg-green-100 text-green-700" : "bg-yellow-100 text-yellow-700"}`}>
                    품질 체크 {passed}/{total} 통과 →
                  </Link>
                );
              })()}
            </div>
          )}
        </div>
      )}

      {/* Controls */}
      <div className="flex items-center gap-3 mt-4">
        <button
          className="px-4 py-2 bg-[#1e3a5f] text-white rounded-lg text-sm font-medium hover:bg-[#2f5496] transition-colors flex items-center gap-2 disabled:opacity-50"
          disabled={isRunning}
          onClick={() => runAll.mutate(undefined)}
        >
          ▶ 전체 실행
        </button>
        <div className="relative">
          <button
            onClick={() => setShowDropdown(!showDropdown)}
            className="px-4 py-2 bg-white border border-gray-300 rounded-lg text-sm font-medium hover:bg-gray-50 transition-colors disabled:opacity-50"
            disabled={isRunning}
          >
            단계별 실행 ▾
          </button>
          {showDropdown && (
            <div className="absolute top-full mt-1 left-0 bg-white border border-gray-200 rounded-lg shadow-lg z-50 py-1 min-w-[280px]">
              {[
                "1. 크롤링 — US, SE, SG, 범용 31개국",
                "2. 선별 — 3-Pass 알고리즘",
                "3. 메타데이터 보강",
                "4. 검증 — 3중 전수 검증",
                "5. Excel 출력 — 25컬럼",
              ].map((item, i) => (
                <button key={i} className="block w-full text-left px-4 py-2 text-sm hover:bg-gray-50 text-gray-700"
                  onClick={() => {
                    setShowDropdown(false);
                    runStep.mutate(i + 1);
                  }}>
                  {item}
                </button>
              ))}
            </div>
          )}
        </div>
        <button
          className="px-4 py-2 bg-white border border-red-300 text-red-600 rounded-lg text-sm font-medium hover:bg-red-50 transition-colors disabled:opacity-50"
          disabled={!isRunning}
          onClick={() => stop.mutate()}
        >
          ⏹ 중지
        </button>
        {isRunning && currentStep && (
          <span className="text-sm text-blue-600 ml-auto">
            {STEP_LABELS[currentStep]?.label} 진행중 — {progress}%
          </span>
        )}
      </div>
    </div>
  );
}
