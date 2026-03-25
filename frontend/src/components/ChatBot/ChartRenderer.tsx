import { useState } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line,
  PieChart, Pie, Cell, Legend,
} from 'recharts';
import type { ChartSpec } from '../../api/client';

const COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16'];

const CHART_TYPE_LABELS: Record<string, string> = {
  bar: 'Bar Chart',
  line: 'Trend Line',
  pie: 'Distribution',
};

const CHART_TYPE_ICONS: Record<string, string> = {
  bar: '|||',
  line: '~',
  pie: 'O',
};

function formatValue(value: number): string {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(0)}K`;
  return value.toLocaleString();
}

function formatTooltipValue(value: number): string {
  return value.toLocaleString();
}

export default function ChartRenderer({ charts }: { charts: ChartSpec[] }) {
  const [activeTab, setActiveTab] = useState(0);

  if (charts.length === 0) return null;

  const activeChart = charts[activeTab];

  return (
    <div className="bg-white rounded-lg border border-slate-200 overflow-hidden">
      {/* Tabs */}
      {charts.length > 1 && (
        <div className="flex border-b border-slate-200 bg-slate-50">
          {charts.map((chart, i) => (
            <button
              key={i}
              onClick={() => setActiveTab(i)}
              className={`flex items-center gap-1.5 px-4 py-2.5 text-xs font-medium transition-colors border-b-2 ${
                i === activeTab
                  ? 'text-blue-600 border-blue-600 bg-white'
                  : 'text-slate-500 border-transparent hover:text-slate-700 hover:bg-slate-100'
              }`}
            >
              <span className="font-mono text-[10px]">{CHART_TYPE_ICONS[chart.chart_type] || '?'}</span>
              {CHART_TYPE_LABELS[chart.chart_type] || chart.chart_type}
            </button>
          ))}
        </div>
      )}

      {/* Chart content */}
      <div className="p-5">
        <SingleChart chart={activeChart} />
      </div>
    </div>
  );
}

function SingleChart({ chart }: { chart: ChartSpec }) {
  const { chart_type, title, x_label, y_label, data } = chart;

  const maxLabelLength = Math.max(...data.map(d => (d.label || '').length), 0);
  const xAxisHeight = Math.max(60, Math.min(maxLabelLength * 5 + 20, 120));

  const maxValue = Math.max(...data.map(d => d.value || 0), 0);
  const yAxisWidth = Math.max(50, formatValue(maxValue).length * 10 + 15);

  const dynamicHeight = chart_type === 'bar'
    ? Math.max(350, 200 + data.length * 25 + xAxisHeight)
    : chart_type === 'pie'
      ? Math.max(400, 300 + data.length * 12)
      : 350;
  const chartHeight = Math.min(dynamicHeight, 600);

  return (
    <>
      <h3 className="text-sm font-semibold text-slate-700 mb-4">{title}</h3>

      {x_label && chart_type !== 'pie' && (
        <p className="text-center text-xs text-slate-500 mb-2">{x_label}</p>
      )}

      <div style={{ width: '100%', height: chartHeight }}>
        <ResponsiveContainer width="100%" height="100%">
          {chart_type === 'bar' ? (
            <BarChart
              data={data}
              margin={{ top: 10, right: 20, bottom: xAxisHeight, left: yAxisWidth - 30 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis
                dataKey="label"
                tick={{ fontSize: 11, fill: '#475569' }}
                angle={-45}
                textAnchor="end"
                height={xAxisHeight}
                interval={0}
              />
              <YAxis
                tick={{ fontSize: 11, fill: '#475569' }}
                tickFormatter={formatValue}
                width={yAxisWidth}
              />
              <Tooltip
                formatter={(value: unknown) => [formatTooltipValue(Number(value ?? 0)), y_label || 'Value']}
                contentStyle={{
                  fontSize: 12,
                  borderRadius: 8,
                  border: '1px solid #E2E8F0',
                  boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                }}
              />
              <Bar dataKey="value" fill="#3B82F6" radius={[4, 4, 0, 0]} />
            </BarChart>
          ) : chart_type === 'line' ? (
            <LineChart
              data={data}
              margin={{ top: 10, right: 20, bottom: xAxisHeight, left: yAxisWidth - 30 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#E2E8F0" />
              <XAxis
                dataKey="label"
                tick={{ fontSize: 11, fill: '#475569' }}
                angle={-45}
                textAnchor="end"
                height={xAxisHeight}
                interval={0}
              />
              <YAxis
                tick={{ fontSize: 11, fill: '#475569' }}
                tickFormatter={formatValue}
                width={yAxisWidth}
              />
              <Tooltip
                formatter={(value: unknown) => [formatTooltipValue(Number(value ?? 0)), y_label || 'Value']}
                contentStyle={{
                  fontSize: 12,
                  borderRadius: 8,
                  border: '1px solid #E2E8F0',
                  boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                }}
              />
              <Line
                type="monotone"
                dataKey="value"
                stroke="#3B82F6"
                strokeWidth={2}
                dot={{ r: 4, fill: '#3B82F6', strokeWidth: 2, stroke: '#fff' }}
                activeDot={{ r: 6, fill: '#2563EB' }}
              />
            </LineChart>
          ) : (
            <PieChart>
              <Pie
                data={data}
                dataKey="value"
                nameKey="label"
                cx="50%"
                cy="45%"
                outerRadius="70%"
                label={({ name, percent }: { name?: string; percent?: number }) =>
                  `${name ?? ''} (${((percent ?? 0) * 100).toFixed(0)}%)`
                }
                labelLine={{ stroke: '#94A3B8', strokeWidth: 1 }}
                fontSize={11}
              >
                {data.map((_, index) => (
                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip
                formatter={(value: unknown) => [formatTooltipValue(Number(value ?? 0)), '']}
                contentStyle={{
                  fontSize: 12,
                  borderRadius: 8,
                  border: '1px solid #E2E8F0',
                  boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                }}
              />
              <Legend
                wrapperStyle={{ fontSize: 11, paddingTop: 16 }}
                iconType="circle"
                iconSize={8}
              />
            </PieChart>
          )}
        </ResponsiveContainer>
      </div>

      {y_label && chart_type !== 'pie' && (
        <p className="text-center text-xs text-slate-500 mt-2">{y_label}</p>
      )}
    </>
  );
}
