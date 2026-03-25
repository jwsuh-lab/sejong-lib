export interface SummaryData {
  totalCollected: number;
  jsonFileCount: number;
  selectedActive: number;
  selectedTarget: number;
  verifiedCount: number;
  verifiedRate: number;
  excludedByDate: number;
  excludedByPdf: number;
  excludedByLang: number;
  totalCountries: number;
  totalOrgs: number;
  completedDocs: number;
  gbExcluded: boolean;
  gaoExcluded: boolean;
}

export interface CountryData {
  code: string;
  name: string;
  flag: string;
  collected: number;
  selected: number;
  quota: number;
  orgCount: number;
  excluded?: boolean;
}

export interface CategoryData {
  name: string;
  nameKr: string;
  count: number;
  color: string;
  icon: string;
}

export type PipelineStep = 1 | 2 | 3 | 4 | 5;
export type StepStatus = "pending" | "running" | "completed" | "failed" | "partial";

export interface PipelineState {
  currentStep: number | null;
  steps: Record<string, string>;
  progress: number;
  isRunning: boolean;
  // 최근 실행 결과
  runId: number | null;
  startedAt: string | null;
  finishedAt: string | null;
  finalStatus: string | null;
  totalCollected: number;
  totalSelected: number;
  totalVerified: number;
  totalExcluded: number;
  errorMessage: string | null;
  hasExcelFile: boolean;
}

export interface PipelineHistoryItem {
  id: number;
  startedAt: string | null;
  finishedAt: string | null;
  status: string;
  totalCollected: number;
  totalSelected: number;
  totalVerified: number;
  totalExcluded: number;
  errorMessage?: string | null;
}

export interface LogEntry {
  timestamp: string;
  level: "info" | "success" | "warning" | "error";
  step: string;
  message: string;
}

export type DocType = "정책자료" | "통계자료" | "발간자료" | "보고서" | "회의자료";

export interface Document {
  id: number;
  country: string;
  countryFlag: string;
  orgName: string;
  orgCode: string;
  title: string;
  docType: string;
  publishedDate: string;
  relevanceScore: number;
  status: "verified" | "unverified" | "excluded";
  pdfUrl: string | null;
  pdfSizeKb: number | null;
  authors: string;
  description: string;
  keywords: string[];
  category: string;
  brmCode1: string;
  brmCode2: string;
  journal: string;
  volumeInfo: string;
  isbn: string;
  issn: string;
  license: string;
  dateVerified: boolean;
  dateVerifiedBy: string;
  pdfVerified: boolean;
  langVerified: boolean;
  excludeReasons: string[];
  link?: string;
}

export interface QualityCheck {
  label: string;
  passed: boolean;
  detail: string;
}

export interface SiteData {
  id: number;
  code: string;
  name: string | null;
  nameKr: string | null;
  countryCode: string | null;
  country: string | null;
  orgType: string | null;
  acronym: string | null;
  url: string | null;
  brmCategory: string | null;
  currentUse: string | null;
  expectedCount: number | null;
  excluded: boolean;
  scheduleType: string;
  scheduleDays: string | null;
  scheduleTime: string;
  lastCrawledAt: string | null;
  nextCrawlAt: string | null;
  crawlStatus: string;
  lastError: string | null;
  docCount: number;
}
