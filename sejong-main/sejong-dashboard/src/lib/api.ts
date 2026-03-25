import type {
  SummaryData,
  CountryData,
  CategoryData,
  PipelineState,
  PipelineHistoryItem,
  LogEntry,
  Document,
  QualityCheck,
  SiteData,
} from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, init);
  if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);
  return res.json();
}

// ─── Dashboard ───
export function fetchSummary(): Promise<SummaryData> {
  return apiFetch("/api/dashboard/summary");
}

export function fetchCountries(): Promise<CountryData[]> {
  return apiFetch("/api/dashboard/countries");
}

export function fetchCategories(): Promise<CategoryData[]> {
  return apiFetch("/api/dashboard/categories");
}

// ─── Documents ───
export interface DocumentQueryParams {
  page?: number;
  size?: number;
  country?: string;
  status?: string;
  docType?: string;
  search?: string;
  sort?: string;
  category?: string;
}

export interface DocumentListResponse {
  items: Document[];
  total: number;
  page: number;
  totalPages: number;
}

export function fetchDocuments(
  params: DocumentQueryParams
): Promise<DocumentListResponse> {
  const qs = new URLSearchParams(
    Object.fromEntries(
      Object.entries(params).filter(
        ([, v]) => v != null && v !== "" && v !== "all"
      )
    ) as Record<string, string>
  );
  return apiFetch(`/api/documents?${qs}`);
}

// ─── Pipeline ───
export function fetchPipelineStatus(): Promise<PipelineState> {
  return apiFetch("/api/pipeline/status");
}

export async function runPipeline(
  steps?: number[]
): Promise<{ runId: number }> {
  return apiFetch("/api/pipeline/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ steps }),
  });
}

export async function runPipelineStep(step: number): Promise<{ ok: boolean }> {
  return apiFetch(`/api/pipeline/run-step?step=${step}`, { method: "POST" });
}

export async function stopPipeline(): Promise<{ stopped: boolean }> {
  return apiFetch("/api/pipeline/stop", { method: "POST" });
}

export function subscribeLogs(onLog: (log: LogEntry) => void): () => void {
  const es = new EventSource(`${API_BASE}/api/pipeline/logs`);
  es.onmessage = (e) => {
    try {
      onLog(JSON.parse(e.data));
    } catch {
      // ignore parse errors
    }
  };
  es.onerror = () => {
    console.warn("SSE 연결 끊김, 재연결 시도...");
  };
  return () => es.close();
}

export function fetchPipelineHistory(): Promise<PipelineHistoryItem[]> {
  return apiFetch("/api/pipeline/history");
}

// ─── Export ───
export function fetchQualityReport(): Promise<QualityCheck[]> {
  return apiFetch("/api/export/report");
}

export async function downloadExcel(): Promise<void> {
  const res = await fetch(`${API_BASE}/api/export/xlsx`);
  if (!res.ok) throw new Error("Excel 파일 다운로드 실패");
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `세종도서관_해외자료수집_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}_v2.xlsx`;
  a.click();
  URL.revokeObjectURL(url);
}

// ─── Settings ───
export interface SettingItem {
  key: string;
  value: string;
}

export function fetchSettings(): Promise<SettingItem[]> {
  return apiFetch("/api/settings");
}

export async function updateSetting(
  key: string,
  value: string
): Promise<void> {
  await apiFetch("/api/settings", {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key, value }),
  });
}

// ─── Sites ───
export interface SiteQueryParams {
  page?: number;
  size?: number;
  country?: string;
  search?: string;
  excluded?: boolean;
}

export interface SiteListResponse {
  items: SiteData[];
  total: number;
  page: number;
  totalPages: number;
}

export function fetchSites(params: SiteQueryParams): Promise<SiteListResponse> {
  const qs = new URLSearchParams(
    Object.fromEntries(
      Object.entries(params).filter(
        ([, v]) => v != null && v !== "" && v === false ? true : !!v
      )
    ) as Record<string, string>
  );
  return apiFetch(`/api/sites?${qs}`);
}

export function fetchSite(id: number): Promise<SiteData> {
  return apiFetch(`/api/sites/${id}`);
}

export function checkSiteCode(code: string): Promise<{ exists: boolean }> {
  return apiFetch(`/api/sites/check-code?code=${encodeURIComponent(code)}`);
}

export function createSite(data: Partial<SiteData>): Promise<SiteData> {
  return apiFetch("/api/sites", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
}

export function updateSite(id: number, data: Partial<SiteData>): Promise<SiteData> {
  return apiFetch(`/api/sites/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
}

export function deleteSite(id: number): Promise<{ ok: boolean; deletedCode: string; orphanedDocs: number }> {
  return apiFetch(`/api/sites/${id}`, { method: "DELETE" });
}
