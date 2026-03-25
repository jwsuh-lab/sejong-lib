"use client";

import CountryChart from "@/components/country-chart";
import CategoryChart from "@/components/category-chart";
import { useCountries, useCategories } from "@/lib/hooks";

export default function AnalyticsPage() {
  const { data: countryData } = useCountries();
  const { data: categoryData } = useCategories();

  return (
    <div className="p-6 lg:pl-8 space-y-6">
      <div>
        <h1 className="text-xl font-bold text-gray-900">통계</h1>
        <p className="text-sm text-gray-500 mt-0.5">국가별 · 카테고리별 수집 현황 분석</p>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        {countryData && <CountryChart data={countryData} onCountryClick={() => {}} />}
        {categoryData && <CategoryChart data={categoryData} onCategoryClick={() => {}} />}
      </div>
    </div>
  );
}
