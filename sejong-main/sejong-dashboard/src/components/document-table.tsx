"use client";

import { useState, useEffect, useRef } from "react";
import type { Document } from "@/lib/types";
import { useDocuments } from "@/lib/hooks";
import { downloadExcel } from "@/lib/api";

const STATUS_BADGE: Record<string, { label: string; cls: string }> = {
  verified: { label: "✅ 통과", cls: "bg-green-50 text-green-700" },
  unverified: { label: "⚠ 미확인", cls: "bg-yellow-50 text-yellow-700" },
  excluded: { label: "✕ 제외", cls: "bg-red-50 text-red-700" },
};

function ScoreBadge({ score }: { score: number }) {
  const display = score < 0 ? 0 : score;
  const cls = display >= 0.7 ? "bg-green-50 text-green-700" : display >= 0.4 ? "bg-yellow-50 text-yellow-700" : "bg-red-50 text-red-700";
  return <span className={`text-xs font-semibold px-2 py-0.5 rounded-full ${cls}`}>{display.toFixed(2)}</span>;
}

function useDebounce<T>(value: T, delay: number): T {
  const [debouncedValue, setDebouncedValue] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedValue(value), delay);
    return () => clearTimeout(timer);
  }, [value, delay]);
  return debouncedValue;
}

function DocumentDetailModal({ doc, onClose }: { doc: Document; onClose: () => void }) {
  return (
    <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div className="bg-white rounded-2xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto" onClick={e => e.stopPropagation()}>
        <div className="sticky top-0 bg-[#1e3a5f] text-white p-5 rounded-t-2xl">
          <h2 className="text-lg font-bold leading-tight">{doc.title}</h2>
        </div>
        <div className="p-5 space-y-5">
          <Section title="기본 정보">
            <Row label="국가" value={`${doc.countryFlag} ${doc.country}`} />
            <Row label="기관(영문)" value={doc.orgName} />
            <Row label="기관코드" value={doc.orgCode} />
            <Row label="발행일" value={doc.publishedDate} />
            <Row label="저자" value={doc.authors || "—"} />
            <Row label="문서유형" value={doc.docType} />
            <Row label="관련성 점수" value={<ScoreBadge score={doc.relevanceScore} />} />
          </Section>
          <Section title="분류 정보">
            <Row label="BRM 대분류" value={doc.brmCode1} />
            <Row label="BRM 소분류" value={doc.brmCode2} />
            <Row label="키워드" value={doc.keywords.join(", ")} />
            <Row label="카테고리" value={doc.category} />
          </Section>
          <Section title="메타데이터">
            <Row label="수록잡지" value={doc.journal || "—"} />
            <Row label="권호정보" value={doc.volumeInfo || "—"} />
            <Row label="ISBN" value={doc.isbn || "—"} />
            <Row label="ISSN" value={doc.issn || "—"} />
            <Row label="라이선스" value={doc.license || "—"} />
          </Section>
          {doc.description && (
            <div>
              <h4 className="text-sm font-semibold text-gray-500 mb-1">발췌</h4>
              <p className="text-sm text-gray-700 bg-gray-50 rounded-lg p-3">{doc.description}</p>
            </div>
          )}
          <div className="flex gap-3">
            {(doc as Document & { link?: string }).link && (
              <a href={(doc as Document & { link?: string }).link!} target="_blank" rel="noopener noreferrer"
                className="px-4 py-2 bg-[#1e3a5f] text-white text-sm rounded-lg hover:bg-[#2f5496] transition-colors">
                🔗 원문 페이지
              </a>
            )}
            {doc.pdfUrl && (
              <a href={doc.pdfUrl} target="_blank" rel="noopener noreferrer"
                className="px-4 py-2 bg-white border border-[#1e3a5f] text-[#1e3a5f] text-sm rounded-lg hover:bg-gray-50 transition-colors">
                📄 PDF 다운로드
              </a>
            )}
          </div>
          <Section title="검증 상태">
            <Row label="날짜 검증" value={doc.dateVerified ? `✅ ${doc.dateVerifiedBy}으로 확인` : "⚠️ 미확인"} />
            <Row label="PDF 검증" value={doc.pdfVerified ? `✅ 유효 (${doc.pdfSizeKb ? (doc.pdfSizeKb / 1024).toFixed(1) + "MB" : ""})` : "❌ 미달"} />
            <Row label="언어 검증" value={doc.langVerified ? "✅ 영어" : "❌ 비영어"} />
            <Row label="제외 여부" value={doc.excludeReasons.length > 0 ? `❌ ${doc.excludeReasons.join(", ")}` : "미제외"} />
          </Section>
        </div>
        <div className="sticky bottom-0 bg-white border-t p-4 flex justify-end rounded-b-2xl">
          <button onClick={onClose} className="px-6 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 text-sm font-medium">
            닫기
          </button>
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
      <span className="text-xs text-gray-500 w-24 flex-shrink-0">{label}</span>
      <span className="text-sm text-gray-800 flex-1">{value}</span>
    </div>
  );
}

export default function DocumentTable({ countryFilter, categoryFilter }: {
  countryFilter?: string;
  categoryFilter?: string;
}) {
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [docTypeFilter, setDocTypeFilter] = useState("all");
  const [selectedDoc, setSelectedDoc] = useState<Document | null>(null);
  const [page, setPage] = useState(1);
  const pageSize = 20;
  const [downloading, setDownloading] = useState(false);
  const debouncedSearch = useDebounce(search, 300);

  const { data, isLoading } = useDocuments({
    page,
    size: pageSize,
    country: countryFilter,
    status: statusFilter !== "all" ? statusFilter : undefined,
    docType: docTypeFilter !== "all" ? docTypeFilter : undefined,
    search: debouncedSearch || undefined,
    category: categoryFilter,
    sort: "score",
  });

  const items = data?.items ?? [];
  const total = data?.total ?? 0;
  const totalPages = data?.totalPages ?? 1;

  const resetPage = () => setPage(1);

  const handleExcelDownload = async () => {
    setDownloading(true);
    try {
      await downloadExcel();
    } catch (e) {
      alert("Excel 다운로드 실패: " + (e instanceof Error ? e.message : "알 수 없는 오류"));
    } finally {
      setDownloading(false);
    }
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
      <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider mb-4">선별 문서 목록</h3>

      {/* Filter Bar */}
      <div className="flex flex-wrap gap-3 mb-4">
        <input
          type="text"
          placeholder="자료명, 기관명 검색..."
          value={search}
          onChange={(e) => { setSearch(e.target.value); resetPage(); }}
          className="px-3 py-1.5 border border-gray-200 rounded-lg text-sm flex-1 min-w-[200px] focus:outline-none focus:ring-2 focus:ring-blue-300"
        />
        <select value={statusFilter} onChange={e => { setStatusFilter(e.target.value); resetPage(); }}
          className="px-3 py-1.5 border border-gray-200 rounded-lg text-sm bg-white">
          <option value="all">상태: 전체</option>
          <option value="verified">✅ 검증통과</option>
          <option value="unverified">⚠️ 미확인</option>
          <option value="excluded">❌ 제외</option>
        </select>
        <select value={docTypeFilter} onChange={e => { setDocTypeFilter(e.target.value); resetPage(); }}
          className="px-3 py-1.5 border border-gray-200 rounded-lg text-sm bg-white">
          <option value="all">유형: 전체</option>
          <option value="정책자료">정책자료</option>
          <option value="통계자료">통계자료</option>
          <option value="발간자료">발간자료</option>
          <option value="보고서">보고서</option>
          <option value="회의자료">회의자료</option>
        </select>
        {(countryFilter || categoryFilter) && (
          <span className="px-3 py-1.5 bg-blue-50 text-blue-700 rounded-lg text-xs font-medium flex items-center gap-1">
            {countryFilter && `국가: ${countryFilter}`}
            {categoryFilter && `카테고리: ${categoryFilter}`}
          </span>
        )}
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200">
              <th className="text-left py-2 px-2 text-xs font-semibold text-gray-500 w-10">#</th>
              <th className="text-left py-2 px-2 text-xs font-semibold text-gray-500 w-16">국가</th>
              <th className="text-left py-2 px-2 text-xs font-semibold text-gray-500 w-28">기관</th>
              <th className="text-left py-2 px-2 text-xs font-semibold text-gray-500">자료명</th>
              <th className="text-left py-2 px-2 text-xs font-semibold text-gray-500 w-18">유형</th>
              <th className="text-left py-2 px-2 text-xs font-semibold text-gray-500 w-14">점수</th>
              <th className="text-left py-2 px-2 text-xs font-semibold text-gray-500 w-20">상태</th>
              <th className="text-center py-2 px-2 text-xs font-semibold text-gray-500 w-8">PDF</th>
            </tr>
          </thead>
          <tbody>
            {isLoading && (
              <tr><td colSpan={8} className="py-8 text-center text-gray-400 text-sm">로딩 중...</td></tr>
            )}
            {!isLoading && items.map((doc, i) => {
              const badge = STATUS_BADGE[doc.status] || STATUS_BADGE.unverified;
              return (
                <tr
                  key={doc.id}
                  onClick={() => setSelectedDoc(doc)}
                  className="border-b border-gray-50 hover:bg-blue-50/50 cursor-pointer transition-colors"
                >
                  <td className="py-2.5 px-2 text-gray-400">{(page - 1) * pageSize + i + 1}</td>
                  <td className="py-2.5 px-2">
                    <span className="text-sm">{doc.countryFlag}</span>
                    <span className="text-xs text-gray-600 ml-1">{doc.country}</span>
                  </td>
                  <td className="py-2.5 px-2 text-xs text-gray-700 truncate max-w-[120px]" title={doc.orgName}>
                    {doc.orgCode.startsWith("NEW_") ? doc.orgCode.replace("NEW_", "") : (doc.orgName || doc.orgCode).split(" ").slice(0, 2).join(" ")}
                  </td>
                  <td className="py-2.5 px-2 text-gray-800 truncate max-w-[300px]" title={doc.title}>{doc.title}</td>
                  <td className="py-2.5 px-2 text-xs text-gray-600">{doc.docType}</td>
                  <td className="py-2.5 px-2"><ScoreBadge score={doc.relevanceScore} /></td>
                  <td className="py-2.5 px-2">
                    <span className={`text-xs px-1.5 py-0.5 rounded-full ${badge.cls}`}>{badge.label}</span>
                  </td>
                  <td className="py-2.5 px-2 text-center">
                    {doc.pdfUrl ? <span className="text-blue-600" title="PDF">📄</span> : <span className="text-gray-300">—</span>}
                  </td>
                </tr>
              );
            })}
            {!isLoading && items.length === 0 && (
              <tr><td colSpan={8} className="py-8 text-center text-gray-400 text-sm">검색 결과가 없습니다</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Footer */}
      <div className="flex items-center justify-between mt-4">
        <div className="flex items-center gap-2">
          <button
            onClick={() => setPage(p => Math.max(1, p - 1))}
            disabled={page === 1}
            className="px-3 py-1 border border-gray-200 rounded text-sm disabled:opacity-40"
          >
            ◀
          </button>
          <span className="text-sm text-gray-600">{page} / {totalPages}</span>
          <button
            onClick={() => setPage(p => Math.min(totalPages, p + 1))}
            disabled={page === totalPages}
            className="px-3 py-1 border border-gray-200 rounded text-sm disabled:opacity-40"
          >
            ▶
          </button>
          <span className="text-xs text-gray-400 ml-2">(총 {total}건)</span>
        </div>
        <button
          onClick={handleExcelDownload}
          disabled={downloading}
          className="px-4 py-2 bg-[#1e3a5f] text-white text-sm rounded-lg hover:bg-[#2f5496] transition-colors flex items-center gap-2 disabled:opacity-50"
        >
          {downloading ? "다운로드 중..." : "📥 Excel 다운로드"}
        </button>
      </div>

      {selectedDoc && <DocumentDetailModal doc={selectedDoc} onClose={() => setSelectedDoc(null)} />}
    </div>
  );
}
