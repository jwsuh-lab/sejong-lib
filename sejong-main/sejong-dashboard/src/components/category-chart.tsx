"use client";

import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";
import type { CategoryData } from "@/lib/types";

export default function CategoryChart({ data, onCategoryClick }: {
  data: CategoryData[];
  onCategoryClick?: (name: string) => void;
}) {
  const total = data.reduce((s, d) => s + d.count, 0);

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
      <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider mb-4">카테고리 분포</h3>

      <div className="flex items-center gap-4">
        {/* Donut Chart */}
        <div className="w-[160px] h-[160px] flex-shrink-0">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={data}
                cx="50%"
                cy="50%"
                innerRadius={45}
                outerRadius={70}
                dataKey="count"
                stroke="none"
                onClick={(_, i) => onCategoryClick?.(data[i].name)}
                className="cursor-pointer"
              >
                {data.map((entry, i) => (
                  <Cell key={i} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip
                content={({ active, payload }) => {
                  if (!active || !payload?.[0]) return null;
                  const d = payload[0].payload as CategoryData;
                  return (
                    <div className="bg-white border border-gray-200 rounded-lg shadow-lg px-3 py-2 text-xs">
                      <div className="font-semibold text-gray-800">{d.icon} {d.nameKr}</div>
                      <div className="text-gray-500">{d.count}건 ({((d.count / total) * 100).toFixed(0)}%)</div>
                    </div>
                  );
                }}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Legend */}
        <div className="flex-1 space-y-1.5">
          {data.map((c) => (
            <button
              key={c.name}
              onClick={() => onCategoryClick?.(c.name)}
              className="flex items-center gap-2 w-full hover:bg-gray-50 rounded p-1 transition-colors text-left"
            >
              <span className="text-sm">{c.icon}</span>
              <div
                className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                style={{ backgroundColor: c.color }}
              />
              <span className="text-xs text-gray-700 flex-1">{c.nameKr}</span>
              <span className="text-xs font-semibold text-gray-600">{c.count}</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
