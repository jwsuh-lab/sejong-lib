"use client";

import { useState } from "react";
import SummaryCards from "@/components/summary-cards";
import PipelineStepper from "@/components/pipeline-stepper";
import CountryChart from "@/components/country-chart";
import CategoryChart from "@/components/category-chart";
import DocumentTable from "@/components/document-table";
import {
  useSummary,
  useCountries,
  useCategories,
} from "@/lib/hooks";

export default function DashboardPage() {
  const [countryFilter, setCountryFilter] = useState<string | undefined>();
  const [categoryFilter, setCategoryFilter] = useState<string | undefined>();

  const { data: summaryData } = useSummary();
  const { data: countryData } = useCountries();
  const { data: categoryData } = useCategories();

  const handleCountryClick = (code: string) => {
    setCountryFilter(prev => prev === code ? undefined : code);
    setCategoryFilter(undefined);
  };

  const handleCategoryClick = (name: string) => {
    const cat = categoryData?.find(c => c.name === name);
    setCategoryFilter(prev => prev === cat?.nameKr ? undefined : cat?.nameKr);
    setCountryFilter(undefined);
  };

  return (
    <div className="p-6 lg:pl-8 space-y-6">
      {/* Page Header */}
      <div>
        <h1 className="text-xl font-bold text-gray-900">대시보드</h1>
        <p className="text-sm text-gray-500 mt-0.5">35개국 415기관 · 수집 현황 종합</p>
      </div>

      {/* A. Summary Cards */}
      {summaryData && <SummaryCards data={summaryData} />}

      {/* B. Pipeline */}
      <PipelineStepper />

      {/* C + D. Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {countryData && <CountryChart data={countryData} onCountryClick={handleCountryClick} />}
        {categoryData && <CategoryChart data={categoryData} onCategoryClick={handleCategoryClick} />}
      </div>

      {/* Active Filters Indicator */}
      {(countryFilter || categoryFilter) && (
        <div className="flex items-center gap-2 text-sm">
          <span className="text-gray-500">활성 필터:</span>
          {countryFilter && (
            <button
              onClick={() => setCountryFilter(undefined)}
              className="px-2 py-1 bg-blue-50 text-blue-700 rounded-full text-xs flex items-center gap-1 hover:bg-blue-100"
            >
              국가: {countryFilter} ✕
            </button>
          )}
          {categoryFilter && (
            <button
              onClick={() => setCategoryFilter(undefined)}
              className="px-2 py-1 bg-purple-50 text-purple-700 rounded-full text-xs flex items-center gap-1 hover:bg-purple-100"
            >
              카테고리: {categoryFilter} ✕
            </button>
          )}
        </div>
      )}

      {/* E. Document Table */}
      <DocumentTable
        countryFilter={countryFilter}
        categoryFilter={categoryFilter}
      />
    </div>
  );
}
