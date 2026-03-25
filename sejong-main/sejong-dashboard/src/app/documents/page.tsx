"use client";

import { useState, useEffect } from "react";
import { useSearchParams } from "next/navigation";
import { Suspense } from "react";
import type { Document as DocType } from "@/lib/types";
import { useDocuments } from "@/lib/hooks";
import { downloadExcel } from "@/lib/api";
import {
  Search,
  ChevronLeft,
  ChevronRight,
  Download,
  List,
  LayoutGrid,
  ExternalLink,
  FileText,
  X,
} from "lucide-react";

const STATUS_TABS = [
  { key: "all", label: "전체" },
  { key: "unverified", label: "미확인" },
  { key: "verified", label: "검증됨" },
  { key: "excluded", label: "제외됨" },
];

const STATUS_BADGE: Record<string, { label: string; cls: string }> = {
  verified: { label: "검증됨", cls: "bg-green-50 text-green-700" },
  unverified: { label: "미확인", cls: "bg-yellow-50 text-yellow-700" },
  excluded: { label: "제외", cls: "bg-red-50 text-red-700" },
};

function ScoreBar({ score }: { score: number }) {
  const pct = Math.round(score * 100);
  const color = pct >= 70 ? "bg-green-500" : pct >= 40 ? "bg-yellow-500" : "bg-red-400";
  return (
    <div className="flex items-center gap-2">
      <div className="w-20 h-2 bg-gray-100 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-gray-600 font-medium">{pct}점</span>
    </div>
  );
}

function DocumentDetailPanel({ doc, onClose }: { doc: DocType; onClose: () => void }) {
  return (
    <div className="fixed inset-0 bg-black/40 z-50 flex justify-end" onClick={onClose}>
      <div
        className="bg-white w-full max-w-md h-full overflow-y-auto shadow-2xl animate-slide-in"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="sticky top-0 bg-[#1e3a5f] text-white p-5 flex items-start justify-between">
          <div className="flex-1 min-w-0 mr-3">
            <p className="text-xs text-white/60 mb-1">
              {doc.countryFlag} {doc.country} · {doc.orgName || doc.orgCode}
            </p>
            <h2 className="text-base font-bold leading-tight">{doc.title}</h2>
          </div>
          <button onClick={onClose} className="text-white/70 hover:text-white shrink-0 mt-0.5">
            <X size={20} />
          </button>
        </div>

        <div className="p-5 space-y-5">
          {/* Basic Info */}
          <Section title="기본 정보">
            <Row label="발행일" value={doc.publishedDate || "-"} />
            <Row label="유형" value={doc.docType} />
            <Row label="저자" value={doc.authors || "-"} />
            <Row label="관련도" value={<ScoreBar score={doc.relevanceScore} />} />
            <Row label="카테고리" value={doc.category || "-"} />
          </Section>

          {doc.description && (
            <div>
              <h4 className="text-sm font-semibold text-gray-500 mb-1">설명</h4>
              <p className="text-sm text-gray-700 bg-gray-50 rounded-lg p-3 leading-relaxed">
                {doc.description}
              </p>
            </div>
          )}

          {/* Verification */}
          <Section title="검증 상태">
            <Row
              label="날짜 검증"
              value={doc.dateVerified ? `✅ ${doc.dateVerifiedBy}으로 확인` : "⚠️ 미확인"}
            />
            <Row
              label="PDF 검증"
              value={
                doc.pdfVerified
                  ? `✅ 유효 ${doc.pdfSizeKb ? `(${(doc.pdfSizeKb / 1024).toFixed(1)}MB)` : ""}`
                  : "❌ 미달"
              }
            />
            <Row label="언어 검증" value={doc.langVerified ? "✅ 영어" : "❌ 비영어"} />
            {doc.excludeReasons.length > 0 && (
              <Row label="제외 사유" value={doc.excludeReasons.join(", ")} />
            )}
          </Section>

          {/* Classification */}
          <Section title="분류">
            <Row label="BRM 대분류" value={doc.brmCode1 || "-"} />
            <Row label="BRM 소분류" value={doc.brmCode2 || "-"} />
            {doc.keywords.length > 0 && (
              <div className="px-3 py-2">
                <span className="text-xs text-gray-500 block mb-1">키워드</span>
                <div className="flex flex-wrap gap-1">
                  {doc.keywords.map((kw, i) => (
                    <span key={i} className="text-xs bg-blue-50 text-blue-700 px-2 py-0.5 rounded-full">
                      {kw}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </Section>

          {/* Metadata */}
          {(doc.journal || doc.isbn || doc.issn) && (
            <Section title="메타데이터">
              {doc.journal && <Row label="수록잡지" value={doc.journal} />}
              {doc.volumeInfo && <Row label="권호정보" value={doc.volumeInfo} />}
              {doc.isbn && <Row label="ISBN" value={doc.isbn} />}
              {doc.issn && <Row label="ISSN" value={doc.issn} />}
              {doc.license && <Row label="라이선스" value={doc.license} />}
            </Section>
          )}

          {/* Actions */}
          <div className="flex gap-3 pt-2">
            {doc.link && (
              <a
                href={doc.link}
                target="_blank"
                rel="noopener noreferrer"
                className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 bg-[#1e3a5f] text-white text-sm rounded-lg hover:bg-[#2d5a8e] transition-colors"
              >
                <ExternalLink size={14} />
                원문 보기
              </a>
            )}
            {doc.pdfUrl && (
              <a
                href={doc.pdfUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 border border-[#1e3a5f] text-[#1e3a5f] text-sm rounded-lg hover:bg-gray-50 transition-colors"
              >
                <FileText size={14} />
                PDF 다운로드
              </a>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h4 className="text-sm font-semibold text-gray-500 mb-2">{title}</h4>
      <div className="bg-gray-50 rounded-lg divide-y divide-gray-200">{children}</div>
    </div>
  );
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex items-center px-3 py-2">
      <span className="text-xs text-gray-500 w-20 shrink-0">{label}</span>
      <span className="text-sm text-gray-800 flex-1">{value}</span>
    </div>
  );
}

function DocumentsContent() {
  const searchParams = useSearchParams();
  const siteParam = searchParams.get("site") || "";

  const [statusTab, setStatusTab] = useState("all");
  const [search, setSearch] = useState(siteParam);
  const [countryFilter, setCountryFilter] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");
  const [docTypeFilter, setDocTypeFilter] = useState("all");
  const [page, setPage] = useState(1);
  const [viewMode, setViewMode] = useState<"card" | "table">("card");
  const [selectedDoc, setSelectedDoc] = useState<DocType | null>(null);
  const [downloading, setDownloading] = useState(false);
  const pageSize = 20;

  useEffect(() => {
    if (siteParam) setSearch(siteParam);
  }, [siteParam]);

  const { data, isLoading } = useDocuments({
    page,
    size: pageSize,
    status: statusTab !== "all" ? statusTab : undefined,
    country: countryFilter || undefined,
    category: categoryFilter || undefined,
    docType: docTypeFilter !== "all" ? docTypeFilter : undefined,
    search: search || undefined,
    sort: "score",
  });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const totalPages = data?.totalPages ?? 1;

  const resetPage = () => setPage(1);

  const handleExcel = async () => {
    setDownloading(true);
    try { await downloadExcel(); } catch { /* handled in api */ }
    setDownloading(false);
  };

  return (
    <div className="p-6 lg:pl-8 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">수집 문서 보기</h1>
          <p className="text-sm text-gray-500 mt-0.5">크롤링으로 수집된 문서 열람 및 분류</p>
        </div>
        <button
          onClick={handleExcel}
          disabled={downloading}
          className="flex items-center gap-2 px-4 py-2 bg-[#1e3a5f] text-white text-sm rounded-lg hover:bg-[#2d5a8e] disabled:opacity-50 transition-colors"
        >
          <Download size={16} />
          {downloading ? "다운로드 중..." : "Excel 다운로드"}
        </button>
      </div>

      {/* Status Tabs */}
      <div className="flex items-center gap-1 border-b border-gray-200">
        {STATUS_TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => { setStatusTab(tab.key); resetPage(); }}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors -mb-px ${
              statusTab === tab.key
                ? "border-[#1e3a5f] text-[#1e3a5f]"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {tab.label}
          </button>
        ))}
        <div className="flex-1" />
        <div className="flex items-center gap-1 pb-1">
          <button
            onClick={() => setViewMode("card")}
            className={`p-1.5 rounded ${viewMode === "card" ? "bg-gray-200" : "hover:bg-gray-100"}`}
            title="카드 뷰"
          >
            <LayoutGrid size={16} />
          </button>
          <button
            onClick={() => setViewMode("table")}
            className={`p-1.5 rounded ${viewMode === "table" ? "bg-gray-200" : "hover:bg-gray-100"}`}
            title="테이블 뷰"
          >
            <List size={16} />
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative flex-1 min-w-[200px] max-w-sm">
          <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="문서 제목, 기관명, 저자 검색..."
            value={search}
            onChange={(e) => { setSearch(e.target.value); resetPage(); }}
            className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-500"
          />
        </div>
        <select
          value={docTypeFilter}
          onChange={(e) => { setDocTypeFilter(e.target.value); resetPage(); }}
          className="px-3 py-2 text-sm border border-gray-200 rounded-lg"
        >
          <option value="all">유형: 전체</option>
          <option value="정책자료">정책자료</option>
          <option value="통계자료">통계자료</option>
          <option value="발간자료">발간자료</option>
          <option value="보고서">보고서</option>
          <option value="회의자료">회의자료</option>
        </select>
        <span className="text-sm text-gray-500">총 {total}건</span>
      </div>

      {/* Content */}
      {isLoading ? (
        <div className="text-center py-12 text-gray-400 text-sm">로딩 중...</div>
      ) : items.length === 0 ? (
        <div className="text-center py-12 text-gray-400 text-sm">검색 결과가 없습니다</div>
      ) : viewMode === "card" ? (
        /* Card View */
        <div className="space-y-3">
          {items.map((doc) => {
            const badge = STATUS_BADGE[doc.status] || STATUS_BADGE.unverified;
            return (
              <div
                key={doc.id}
                onClick={() => setSelectedDoc(doc)}
                className="bg-white rounded-xl border border-gray-100 shadow-sm p-4 hover:border-blue-200 hover:shadow-md cursor-pointer transition-all"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 text-xs text-gray-500 mb-1.5">
                      <span>{doc.countryFlag} {doc.orgName || doc.orgCode}</span>
                      <span>·</span>
                      <span>{doc.docType}</span>
                      {doc.publishedDate && (
                        <>
                          <span>·</span>
                          <span>{doc.publishedDate}</span>
                        </>
                      )}
                    </div>
                    <h3 className="text-sm font-medium text-gray-900 leading-snug mb-2">
                      {doc.title}
                    </h3>
                    <div className="flex items-center gap-3">
                      <ScoreBar score={doc.relevanceScore} />
                      {doc.pdfUrl && (
                        <span className="text-xs text-blue-600 flex items-center gap-1">
                          <FileText size={12} />
                          PDF {doc.pdfSizeKb ? `${(doc.pdfSizeKb / 1024).toFixed(1)}MB` : ""}
                        </span>
                      )}
                    </div>
                    {doc.keywords.length > 0 && (
                      <div className="flex flex-wrap gap-1 mt-2">
                        {doc.keywords.slice(0, 5).map((kw, i) => (
                          <span key={i} className="text-[11px] bg-gray-100 text-gray-600 px-1.5 py-0.5 rounded">
                            {kw}
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                  <span className={`text-xs px-2 py-0.5 rounded-full shrink-0 ${badge.cls}`}>
                    {badge.label}
                  </span>
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        /* Table View */
        <div className="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-100">
                  <th className="text-left px-4 py-3 text-xs font-medium text-gray-500">국가</th>
                  <th className="text-left px-4 py-3 text-xs font-medium text-gray-500">기관</th>
                  <th className="text-left px-4 py-3 text-xs font-medium text-gray-500">자료명</th>
                  <th className="text-left px-4 py-3 text-xs font-medium text-gray-500">유형</th>
                  <th className="text-left px-4 py-3 text-xs font-medium text-gray-500">관련도</th>
                  <th className="text-left px-4 py-3 text-xs font-medium text-gray-500">상태</th>
                  <th className="text-center px-4 py-3 text-xs font-medium text-gray-500">PDF</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {items.map((doc) => {
                  const badge = STATUS_BADGE[doc.status] || STATUS_BADGE.unverified;
                  return (
                    <tr
                      key={doc.id}
                      onClick={() => setSelectedDoc(doc)}
                      className="hover:bg-blue-50/50 cursor-pointer"
                    >
                      <td className="px-4 py-3 whitespace-nowrap">
                        {doc.countryFlag} {doc.country}
                      </td>
                      <td className="px-4 py-3 text-xs text-gray-700 truncate max-w-[120px]">{doc.orgName || doc.orgCode}</td>
                      <td className="px-4 py-3 text-gray-800 truncate max-w-[300px]" title={doc.title}>{doc.title}</td>
                      <td className="px-4 py-3 text-xs text-gray-600">{doc.docType}</td>
                      <td className="px-4 py-3"><ScoreBar score={doc.relevanceScore} /></td>
                      <td className="px-4 py-3">
                        <span className={`text-xs px-2 py-0.5 rounded-full ${badge.cls}`}>{badge.label}</span>
                      </td>
                      <td className="px-4 py-3 text-center">
                        {doc.pdfUrl ? <FileText size={14} className="inline text-blue-500" /> : <span className="text-gray-300">-</span>}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <span className="text-xs text-gray-500">
            {total}건 중 {(page - 1) * pageSize + 1}~{Math.min(page * pageSize, total)}
          </span>
          <div className="flex items-center gap-1">
            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={page === 1}
              className="p-1.5 rounded-md hover:bg-gray-100 disabled:opacity-30"
            >
              <ChevronLeft size={16} />
            </button>
            <span className="text-sm px-2">
              {page} / {totalPages}
            </span>
            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={page === totalPages}
              className="p-1.5 rounded-md hover:bg-gray-100 disabled:opacity-30"
            >
              <ChevronRight size={16} />
            </button>
          </div>
        </div>
      )}

      {/* Detail Panel */}
      {selectedDoc && (
        <DocumentDetailPanel doc={selectedDoc} onClose={() => setSelectedDoc(null)} />
      )}
    </div>
  );
}

export default function DocumentsPage() {
  return (
    <Suspense fallback={<div className="p-6 text-gray-400 text-sm">로딩 중...</div>}>
      <DocumentsContent />
    </Suspense>
  );
}
