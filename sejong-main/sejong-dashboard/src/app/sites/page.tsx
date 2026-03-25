"use client";

import { useState } from "react";
import {
  useSites,
  useCreateSite,
  useUpdateSite,
  useDeleteSite,
} from "@/lib/hooks";
import { checkSiteCode } from "@/lib/api";
import type { SiteData } from "@/lib/types";
import {
  Plus,
  Search,
  Pencil,
  Trash2,
  ExternalLink,
  ChevronLeft,
  ChevronRight,
  X,
  AlertTriangle,
} from "lucide-react";
import Link from "next/link";

const COUNTRY_FLAGS: Record<string, string> = {
  US: "\u{1F1FA}\u{1F1F8}", CA: "\u{1F1E8}\u{1F1E6}", SE: "\u{1F1F8}\u{1F1EA}", SG: "\u{1F1F8}\u{1F1EC}",
  EU: "\u{1F1EA}\u{1F1FA}", AT: "\u{1F1E6}\u{1F1F9}", NO: "\u{1F1F3}\u{1F1F4}", IN: "\u{1F1EE}\u{1F1F3}",
  IT: "\u{1F1EE}\u{1F1F9}", BE: "\u{1F1E7}\u{1F1EA}", HU: "\u{1F1ED}\u{1F1FA}", GB: "\u{1F1EC}\u{1F1E7}",
  DK: "\u{1F1E9}\u{1F1F0}", FI: "\u{1F1EB}\u{1F1EE}", NL: "\u{1F1F3}\u{1F1F1}", DE: "\u{1F1E9}\u{1F1EA}",
  FR: "\u{1F1EB}\u{1F1F7}", CH: "\u{1F1E8}\u{1F1ED}", JP: "\u{1F1EF}\u{1F1F5}", AU: "\u{1F1E6}\u{1F1FA}",
  NZ: "\u{1F1F3}\u{1F1FF}", KR: "\u{1F1F0}\u{1F1F7}", IE: "\u{1F1EE}\u{1F1EA}", ES: "\u{1F1EA}\u{1F1F8}",
  PT: "\u{1F1F5}\u{1F1F9}", PL: "\u{1F1F5}\u{1F1F1}", CZ: "\u{1F1E8}\u{1F1FF}", IL: "\u{1F1EE}\u{1F1F1}",
  TW: "\u{1F1F9}\u{1F1FC}", MX: "\u{1F1F2}\u{1F1FD}", BR: "\u{1F1E7}\u{1F1F7}", ZA: "\u{1F1FF}\u{1F1E6}",
  KE: "\u{1F1F0}\u{1F1EA}", NG: "\u{1F1F3}\u{1F1EC}",
};

const SCHEDULE_LABELS: Record<string, string> = {
  manual: "수동",
  daily: "매일",
  weekly: "매주",
  biweekly: "격주",
  monthly: "매월",
};

const STATUS_BADGE: Record<string, { label: string; cls: string }> = {
  idle: { label: "대기", cls: "bg-gray-100 text-gray-600" },
  running: { label: "수집 중", cls: "bg-blue-100 text-blue-700" },
  error: { label: "오류", cls: "bg-red-100 text-red-700" },
};

type FormData = {
  code: string;
  name: string;
  nameKr: string;
  countryCode: string;
  country: string;
  orgType: string;
  acronym: string;
  url: string;
  brmCategory: string;
  currentUse: string;
  expectedCount: string;
  excluded: boolean;
  scheduleType: string;
  scheduleDays: string;
  scheduleTime: string;
};

const EMPTY_FORM: FormData = {
  code: "",
  name: "",
  nameKr: "",
  countryCode: "",
  country: "",
  orgType: "",
  acronym: "",
  url: "",
  brmCategory: "",
  currentUse: "Y",
  expectedCount: "",
  excluded: false,
  scheduleType: "manual",
  scheduleDays: "",
  scheduleTime: "03:00",
};

export default function SitesPage() {
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [countryFilter, setCountryFilter] = useState("");
  const [showModal, setShowModal] = useState(false);
  const [editingSite, setEditingSite] = useState<SiteData | null>(null);
  const [form, setForm] = useState<FormData>(EMPTY_FORM);
  const [codeError, setCodeError] = useState("");
  const [deleteConfirm, setDeleteConfirm] = useState<SiteData | null>(null);

  const { data, isLoading } = useSites({
    page,
    size: 20,
    search: search || undefined,
    country: countryFilter || undefined,
  });

  const createMutation = useCreateSite();
  const updateMutation = useUpdateSite();
  const deleteMutation = useDeleteSite();

  const openCreate = () => {
    setEditingSite(null);
    setForm(EMPTY_FORM);
    setCodeError("");
    setShowModal(true);
  };

  const openEdit = (site: SiteData) => {
    setEditingSite(site);
    setForm({
      code: site.code,
      name: site.name || "",
      nameKr: site.nameKr || "",
      countryCode: site.countryCode || "",
      country: site.country || "",
      orgType: site.orgType || "",
      acronym: site.acronym || "",
      url: site.url || "",
      brmCategory: site.brmCategory || "",
      currentUse: site.currentUse || "Y",
      expectedCount: site.expectedCount?.toString() || "",
      excluded: site.excluded,
      scheduleType: site.scheduleType || "manual",
      scheduleDays: site.scheduleDays || "",
      scheduleTime: site.scheduleTime || "03:00",
    });
    setCodeError("");
    setShowModal(true);
  };

  const handleCodeBlur = async () => {
    if (!form.code || editingSite) return;
    const { exists } = await checkSiteCode(form.code);
    setCodeError(exists ? "이미 존재하는 코드입니다" : "");
  };

  const handleSubmit = async () => {
    if (codeError) return;

    const payload = {
      code: form.code,
      name: form.name || null,
      nameKr: form.nameKr || null,
      countryCode: form.countryCode || null,
      country: form.country || null,
      orgType: form.orgType || null,
      acronym: form.acronym || null,
      url: form.url || null,
      brmCategory: form.brmCategory || null,
      currentUse: form.currentUse || null,
      expectedCount: form.expectedCount ? parseInt(form.expectedCount) : null,
      excluded: form.excluded,
      scheduleType: form.scheduleType,
      scheduleDays: form.scheduleDays || null,
      scheduleTime: form.scheduleTime,
    };

    if (editingSite) {
      await updateMutation.mutateAsync({ id: editingSite.id, data: payload });
    } else {
      await createMutation.mutateAsync(payload);
    }
    setShowModal(false);
  };

  const handleDelete = async () => {
    if (!deleteConfirm) return;
    await deleteMutation.mutateAsync(deleteConfirm.id);
    setDeleteConfirm(null);
  };

  const totalActive = data ? data.total : 0;

  return (
    <div className="p-6 lg:pl-8 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">수집 사이트 관리</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            크롤링 대상 사이트 추가/삭제 및 수집 주기 설정
          </p>
        </div>
        <button
          onClick={openCreate}
          className="flex items-center gap-2 px-4 py-2 bg-[#1e3a5f] text-white text-sm rounded-lg hover:bg-[#2d5a8e] transition-colors"
        >
          <Plus size={16} />
          사이트 추가
        </button>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="relative flex-1 min-w-[200px] max-w-sm">
          <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="사이트 검색 (코드, 기관명, 약칭)"
            value={search}
            onChange={(e) => { setSearch(e.target.value); setPage(1); }}
            className="w-full pl-9 pr-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30 focus:border-blue-500"
          />
        </div>
        <select
          value={countryFilter}
          onChange={(e) => { setCountryFilter(e.target.value); setPage(1); }}
          className="px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30"
        >
          <option value="">전체 국가</option>
          {Object.entries(COUNTRY_FLAGS).map(([code, flag]) => (
            <option key={code} value={code}>{flag} {code}</option>
          ))}
        </select>
        <span className="text-sm text-gray-500">총 {totalActive}개 사이트</span>
      </div>

      {/* Table */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-100 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-100">
                <th className="text-left px-4 py-3 font-medium text-gray-500">코드</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">기관명</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">국가</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">유형</th>
                <th className="text-center px-4 py-3 font-medium text-gray-500">수집 주기</th>
                <th className="text-center px-4 py-3 font-medium text-gray-500">상태</th>
                <th className="text-center px-4 py-3 font-medium text-gray-500">문서</th>
                <th className="text-left px-4 py-3 font-medium text-gray-500">URL</th>
                <th className="text-center px-4 py-3 font-medium text-gray-500">관리</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50">
              {isLoading ? (
                <tr><td colSpan={9} className="px-4 py-12 text-center text-gray-400">로딩 중...</td></tr>
              ) : data?.items.length === 0 ? (
                <tr><td colSpan={9} className="px-4 py-12 text-center text-gray-400">등록된 사이트가 없습니다</td></tr>
              ) : (
                data?.items.map((site) => {
                  const flag = COUNTRY_FLAGS[site.countryCode?.toUpperCase() || ""] || "";
                  const status = STATUS_BADGE[site.crawlStatus] || STATUS_BADGE.idle;
                  return (
                    <tr key={site.id} className={`hover:bg-gray-50/50 ${site.excluded ? "opacity-50" : ""}`}>
                      <td className="px-4 py-3 font-mono text-xs text-gray-700">{site.code}</td>
                      <td className="px-4 py-3">
                        <div className="font-medium text-gray-900">{site.name || site.code}</div>
                        {site.nameKr && <div className="text-xs text-gray-500">{site.nameKr}</div>}
                      </td>
                      <td className="px-4 py-3 whitespace-nowrap">
                        {flag} {site.country || site.countryCode || "-"}
                      </td>
                      <td className="px-4 py-3 text-gray-600">{site.orgType || "-"}</td>
                      <td className="px-4 py-3 text-center">
                        <span className="text-xs px-2 py-0.5 rounded-full bg-gray-100 text-gray-600">
                          {SCHEDULE_LABELS[site.scheduleType] || site.scheduleType}
                          {site.scheduleType !== "manual" && ` ${site.scheduleTime}`}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-center">
                        <span className={`text-xs px-2 py-0.5 rounded-full ${status.cls}`}>
                          {status.label}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-center">
                        <Link
                          href={`/documents?site=${site.code}`}
                          className="text-blue-600 hover:underline"
                        >
                          {site.docCount}
                        </Link>
                      </td>
                      <td className="px-4 py-3">
                        {site.url ? (
                          <a href={site.url} target="_blank" rel="noopener noreferrer"
                            className="text-blue-500 hover:text-blue-700">
                            <ExternalLink size={14} />
                          </a>
                        ) : "-"}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center justify-center gap-1">
                          <button
                            onClick={() => openEdit(site)}
                            className="p-1.5 rounded-md hover:bg-gray-100 text-gray-500 hover:text-blue-600"
                            title="편집"
                          >
                            <Pencil size={14} />
                          </button>
                          <button
                            onClick={() => setDeleteConfirm(site)}
                            className="p-1.5 rounded-md hover:bg-gray-100 text-gray-500 hover:text-red-600"
                            title="삭제"
                          >
                            <Trash2 size={14} />
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {data && data.totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100">
            <span className="text-xs text-gray-500">
              {data.total}개 중 {(page - 1) * 20 + 1}~{Math.min(page * 20, data.total)}
            </span>
            <div className="flex items-center gap-1">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page === 1}
                className="p-1.5 rounded-md hover:bg-gray-100 disabled:opacity-30"
              >
                <ChevronLeft size={16} />
              </button>
              <span className="text-sm px-2">{page} / {data.totalPages}</span>
              <button
                onClick={() => setPage(p => Math.min(data.totalPages, p + 1))}
                disabled={page === data.totalPages}
                className="p-1.5 rounded-md hover:bg-gray-100 disabled:opacity-30"
              >
                <ChevronRight size={16} />
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Create/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4" onClick={() => setShowModal(false)}>
          <div className="bg-white rounded-xl shadow-xl w-full max-w-lg max-h-[90vh] overflow-y-auto" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
              <h2 className="text-lg font-bold text-gray-900">
                {editingSite ? "사이트 편집" : "새 사이트 추가"}
              </h2>
              <button onClick={() => setShowModal(false)} className="text-gray-400 hover:text-gray-600">
                <X size={20} />
              </button>
            </div>

            <div className="px-6 py-4 space-y-4">
              {/* Code */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">사이트 코드 *</label>
                <input
                  type="text"
                  value={form.code}
                  onChange={(e) => setForm(f => ({ ...f, code: e.target.value.toUpperCase() }))}
                  onBlur={handleCodeBlur}
                  disabled={!!editingSite}
                  placeholder="예: US_CBO"
                  className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30 disabled:bg-gray-50"
                />
                {codeError && <p className="text-xs text-red-500 mt-1">{codeError}</p>}
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">기관명 (영문)</label>
                  <input type="text" value={form.name}
                    onChange={(e) => setForm(f => ({ ...f, name: e.target.value }))}
                    className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">기관명 (한글)</label>
                  <input type="text" value={form.nameKr}
                    onChange={(e) => setForm(f => ({ ...f, nameKr: e.target.value }))}
                    className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">국가코드 *</label>
                  <select value={form.countryCode}
                    onChange={(e) => setForm(f => ({ ...f, countryCode: e.target.value }))}
                    className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                  >
                    <option value="">선택</option>
                    {Object.entries(COUNTRY_FLAGS).map(([code, flag]) => (
                      <option key={code} value={code}>{flag} {code}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">기관 유형</label>
                  <select value={form.orgType}
                    onChange={(e) => setForm(f => ({ ...f, orgType: e.target.value }))}
                    className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                  >
                    <option value="">선택</option>
                    <option value="정부">정부</option>
                    <option value="중앙은행">중앙은행</option>
                    <option value="연구기관">연구기관</option>
                    <option value="국제기구">국제기구</option>
                    <option value="의회">의회</option>
                    <option value="감사원">감사원</option>
                  </select>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">수집 URL *</label>
                <input type="url" value={form.url}
                  onChange={(e) => setForm(f => ({ ...f, url: e.target.value }))}
                  placeholder="https://..."
                  className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">약칭</label>
                  <input type="text" value={form.acronym}
                    onChange={(e) => setForm(f => ({ ...f, acronym: e.target.value }))}
                    className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">예상 수집 건수</label>
                  <input type="number" value={form.expectedCount}
                    onChange={(e) => setForm(f => ({ ...f, expectedCount: e.target.value }))}
                    className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                  />
                </div>
              </div>

              {/* Schedule */}
              <div className="border-t border-gray-100 pt-4">
                <h3 className="text-sm font-medium text-gray-700 mb-3">수집 주기 설정</h3>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">주기</label>
                    <select value={form.scheduleType}
                      onChange={(e) => setForm(f => ({ ...f, scheduleType: e.target.value }))}
                      className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30"
                    >
                      <option value="manual">수동</option>
                      <option value="daily">매일</option>
                      <option value="weekly">매주</option>
                      <option value="biweekly">격주</option>
                      <option value="monthly">매월</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs text-gray-500 mb-1">수집 시각</label>
                    <input type="time" value={form.scheduleTime}
                      onChange={(e) => setForm(f => ({ ...f, scheduleTime: e.target.value }))}
                      disabled={form.scheduleType === "manual"}
                      className="w-full px-3 py-2 text-sm border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500/30 disabled:bg-gray-50"
                    />
                  </div>
                </div>
              </div>

              {/* Toggles */}
              <div className="flex items-center gap-6 border-t border-gray-100 pt-4">
                <label className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={form.currentUse === "Y"}
                    onChange={(e) => setForm(f => ({ ...f, currentUse: e.target.checked ? "Y" : "N" }))}
                    className="rounded"
                  />
                  사용 중
                </label>
                <label className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={form.excluded}
                    onChange={(e) => setForm(f => ({ ...f, excluded: e.target.checked }))}
                    className="rounded"
                  />
                  제외
                </label>
              </div>
            </div>

            <div className="flex items-center justify-end gap-2 px-6 py-4 border-t border-gray-100">
              <button
                onClick={() => setShowModal(false)}
                className="px-4 py-2 text-sm text-gray-600 hover:bg-gray-100 rounded-lg"
              >
                취소
              </button>
              <button
                onClick={handleSubmit}
                disabled={!form.code || !!codeError || createMutation.isPending || updateMutation.isPending}
                className="px-4 py-2 text-sm bg-[#1e3a5f] text-white rounded-lg hover:bg-[#2d5a8e] disabled:opacity-50 transition-colors"
              >
                {editingSite ? "저장" : "추가"}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete Confirmation */}
      {deleteConfirm && (
        <div className="fixed inset-0 bg-black/40 z-50 flex items-center justify-center p-4" onClick={() => setDeleteConfirm(null)}>
          <div className="bg-white rounded-xl shadow-xl w-full max-w-sm" onClick={e => e.stopPropagation()}>
            <div className="px-6 py-5 text-center">
              <AlertTriangle size={40} className="mx-auto text-amber-500 mb-3" />
              <h3 className="text-lg font-bold text-gray-900 mb-1">사이트 삭제</h3>
              <p className="text-sm text-gray-600 mb-1">
                <strong>{deleteConfirm.name || deleteConfirm.code}</strong>을(를) 삭제하시겠습니까?
              </p>
              {deleteConfirm.docCount > 0 && (
                <p className="text-sm text-amber-600">
                  이 사이트에 수집된 문서 {deleteConfirm.docCount}건이 있습니다.
                </p>
              )}
            </div>
            <div className="flex items-center gap-2 px-6 py-4 border-t border-gray-100">
              <button
                onClick={() => setDeleteConfirm(null)}
                className="flex-1 px-4 py-2 text-sm text-gray-600 hover:bg-gray-100 rounded-lg"
              >
                취소
              </button>
              <button
                onClick={handleDelete}
                disabled={deleteMutation.isPending}
                className="flex-1 px-4 py-2 text-sm bg-red-500 text-white rounded-lg hover:bg-red-600 disabled:opacity-50"
              >
                삭제
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
