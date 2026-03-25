import { useQuery } from '@tanstack/react-query';
import { fetchDashboardStats, fetchIndexStatus, fetchGraphStats } from '../api/client';
import { StarIcon, ChartIcon, WorkflowIcon } from '../components/Icons';

export default function DashboardPage() {
  const { data: stats, isLoading, error } = useQuery({
    queryKey: ['dashboardStats'],
    queryFn: fetchDashboardStats,
    refetchInterval: 30000,
    staleTime: 10000,
  });

  const { data: weaviateStatus } = useQuery({
    queryKey: ['weaviateStatus'],
    queryFn: fetchIndexStatus,
    refetchInterval: 30000,
    retry: false,
  });

  const { data: graphStats } = useQuery({
    queryKey: ['graphStats'],
    queryFn: fetchGraphStats,
    refetchInterval: 30000,
    retry: false,
  });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-slate-500">Loading dashboard...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 text-red-700 p-4 rounded-lg border border-red-200">
        Failed to load dashboard. Make sure the backend is running.
      </div>
    );
  }

  if (!stats) return null;

  const { summary, step_coverage, quality_stats, time_stats, file_type_breakdown, parser_metrics, enrichment_metrics, workflow_dis } = stats;

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-slate-900">Executive Dashboard</h1>
        <p className="text-sm text-slate-500">Overview of pipeline performance, quality metrics, and coverage statistics</p>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4 mb-6">
        <SummaryCard
          title="Data Ingestion Score"
          value={summary.overall_dis.toFixed(1)}
          trend={summary.dis_trend}
          sublabel="Quality + Efficiency + Exec Success"
          color="green"
        />
        <SummaryCard title="Total Assets" value={summary.total_assets} color="blue" />
        <SummaryCard
          title="Workflows Executed"
          value={summary.workflows_executed}
          sublabel={`${summary.workflows_executed} onboarded`}
          color="purple"
        />
        <SummaryCard
          title="Successful"
          value={summary.workflows_successful}
          sublabel={`${summary.success_rate.toFixed(0)}% success rate`}
          color="green"
        />
        <SummaryCard
          title="Eligible Coverage"
          value={`${summary.eligible_coverage.toFixed(0)}%`}
          sublabel={`${summary.workflows_executed} of ${summary.workflows_executed} onboarded`}
          color="purple"
        />
        <SummaryCard title="Avg Quality" value={summary.avg_quality.toFixed(1)} color="green" />
        <SummaryCard title="Avg Efficiency" value={summary.avg_efficiency.toFixed(1)} color="yellow" />
      </div>

      {/* Processing Time & Quality Scores */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        {/* Processing Time Distribution */}
        <MetricsCard
          title="Processing Time Distribution"
          icon={<ChartIcon className="w-5 h-5" />}
        >
          <p className="text-sm text-slate-500 mb-4">Processing time statistics across workflows</p>
          <div className="space-y-3">
            <StatRow label="Min" value={formatTime(time_stats.min.value_ms)} workflow={time_stats.min.workflow} color="text-green-600" />
            <StatRow label="Avg" value={formatTime(time_stats.avg.value_ms)} workflow={time_stats.avg.workflow || 'All workflows'} color="text-blue-600" />
            <StatRow label="Max" value={formatTime(time_stats.max.value_ms)} workflow={time_stats.max.workflow} color="text-red-600" />
          </div>
          {/* File Type Breakdown - Processing Time */}
          <div className="border-t border-slate-100 pt-4 mt-4">
            <p className="text-xs font-semibold text-slate-500 uppercase mb-3">By File Type</p>
            <div className="grid grid-cols-4 gap-2 text-center">
              {(['CSV', 'JSON', 'PDF', 'XLSX'] as const).map((type) => (
                <div key={type} className="p-2 bg-slate-50 rounded-lg">
                  <p className="text-xs text-slate-500">{type}</p>
                  <p className="text-sm font-bold text-blue-600">{formatTime(file_type_breakdown[type].avg_time_ms)}</p>
                  <p className="text-[10px] text-slate-400">{file_type_breakdown[type].count} files</p>
                </div>
              ))}
            </div>
          </div>
        </MetricsCard>

        {/* Quality Score Distribution */}
        <MetricsCard
          title="Quality Score Distribution"
          icon={<StarIcon className="w-5 h-5" />}
        >
          <p className="text-sm text-slate-500 mb-4">Quality score statistics across workflows</p>
          <div className="space-y-3">
            <StatRow label="Min" value={quality_stats.min.value.toFixed(1)} workflow={quality_stats.min.workflow} color="text-red-600" />
            <StatRow label="Avg" value={quality_stats.avg.value.toFixed(1)} workflow={quality_stats.avg.workflow || 'All workflows'} color="text-blue-600" />
            <StatRow label="Max" value={quality_stats.max.value.toFixed(1)} workflow={quality_stats.max.workflow} color="text-green-600" />
          </div>
          {/* File Type Breakdown - Quality */}
          <div className="border-t border-slate-100 pt-4 mt-4">
            <p className="text-xs font-semibold text-slate-500 uppercase mb-3">By File Type</p>
            <div className="grid grid-cols-4 gap-2 text-center">
              {(['CSV', 'JSON', 'PDF', 'XLSX'] as const).map((type) => (
                <div key={type} className="p-2 bg-slate-50 rounded-lg">
                  <p className="text-xs text-slate-500">{type}</p>
                  <p className="text-sm font-bold text-green-600">{file_type_breakdown[type].avg_quality.toFixed(1)}</p>
                  <p className="text-[10px] text-slate-400">{file_type_breakdown[type].count} files</p>
                </div>
              ))}
            </div>
          </div>
        </MetricsCard>
      </div>

      {/* DIS Formula */}
      <MetricsCard
        title="Data Ingestion Score (DIS)"
        icon={<WorkflowIcon className="w-5 h-5" />}
        className="mb-6"
      >
        <p className="text-sm text-slate-500 mb-4">
          Composite metric combining Quality (40%), Efficiency (30%), and Execution Success (30%)
        </p>
        <div className="bg-slate-50 rounded-lg p-3 mb-4">
          <p className="text-sm text-slate-600">
            <span className="font-semibold text-slate-500">Formula:</span>
            <code className="ml-2 px-2 py-1 bg-white rounded text-xs font-mono">
              DIS = (Quality × 0.40) + (Efficiency × 0.30) + (Exec Success × 0.30)
            </code>
          </p>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-[10px] text-slate-500 uppercase">
              <th className="text-left py-2 font-medium">Component</th>
              <th className="text-right py-2 font-medium">Weight</th>
              <th className="text-right py-2 font-medium">Score</th>
              <th className="text-right py-2 font-medium">Contribution</th>
            </tr>
          </thead>
          <tbody className="text-slate-700">
            <tr className="border-t border-slate-100">
              <td className="py-2">Quality Score</td>
              <td className="text-right">40%</td>
              <td className="text-right font-medium">{summary.avg_quality.toFixed(1)}</td>
              <td className="text-right text-green-600 font-medium">{(summary.avg_quality * 0.4).toFixed(1)}</td>
            </tr>
            <tr className="border-t border-slate-100">
              <td className="py-2">Efficiency Score</td>
              <td className="text-right">30%</td>
              <td className="text-right font-medium">{summary.avg_efficiency.toFixed(1)}</td>
              <td className="text-right text-yellow-600 font-medium">{(summary.avg_efficiency * 0.3).toFixed(1)}</td>
            </tr>
            <tr className="border-t border-slate-100">
              <td className="py-2">Execution Success</td>
              <td className="text-right">30%</td>
              <td className="text-right font-medium">{summary.avg_execution_success.toFixed(1)}</td>
              <td className="text-right text-blue-600 font-medium">{(summary.avg_execution_success * 0.3).toFixed(1)}</td>
            </tr>
            <tr className="border-t-2 border-slate-200 font-semibold">
              <td className="py-2">Total DIS</td>
              <td className="text-right">100%</td>
              <td className="text-right">-</td>
              <td className="text-right text-green-600">{summary.overall_dis.toFixed(1)}</td>
            </tr>
          </tbody>
        </table>
      </MetricsCard>

      {/* Step Coverage */}
      <MetricsCard
        title="Step Coverage"
        icon={<ChartIcon className="w-5 h-5" />}
        className="mb-6"
      >
        <p className="text-sm text-slate-500 mb-4">Percentage of registered assets that have completed each pipeline step</p>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <CoverageItem label="Acquisition Step" value={`${step_coverage.acquisition.percentage.toFixed(0)}%`} detail={`${step_coverage.acquisition.count} of ${step_coverage.acquisition.total} assets`} />
          <CoverageItem label="Parse Step" value={`${step_coverage.parse.percentage.toFixed(0)}%`} detail={`${step_coverage.parse.count} of ${step_coverage.parse.total} assets`} />
          <CoverageItem label="Enrichment Step" value={`${step_coverage.enrichment.percentage.toFixed(0)}%`} detail={`${step_coverage.enrichment.count} of ${step_coverage.enrichment.total} assets`} />
        </div>
      </MetricsCard>

      {/* Quality Metrics Parser */}
      <MetricsCard
        title="Quality Metrics Parser"
        icon={<StarIcon className="w-5 h-5" />}
        className="mb-6"
      >
        <p className="text-sm text-slate-500 mb-4">Document parsing metrics including structure extraction and content analysis</p>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          <CoverageItem label="Total Tables Extracted" value={parser_metrics.total_tables.toLocaleString()} detail="Across all parsed documents" />
          <CoverageItem label="Total Sections" value={parser_metrics.total_sections.toLocaleString()} detail="Document structure elements" />
          <CoverageItem label="Total Tokens" value={parser_metrics.total_tokens.toLocaleString()} detail="Estimated LLM tokens" />
          <CoverageItem label="Avg Parse Quality" value={parser_metrics.avg_parse_quality.toFixed(1)} detail="Overall parse quality score" />
          <CoverageItem label="Estimated Cost" value={`$${parser_metrics.estimated_cost.toFixed(2)}`} detail="Claude Vision API costs" />
        </div>
        {/* Document Type Distribution */}
        <div className="border-t border-slate-100 pt-4 mt-4">
          <p className="text-xs font-semibold text-slate-500 uppercase mb-3">Document Type Distribution</p>
          <div className="flex flex-wrap gap-3">
            {parser_metrics.document_type_distribution.map((docType) => (
              <div key={docType.type} className="flex items-center gap-2">
                <span className={`px-3 py-1 rounded-full text-xs font-medium text-white ${getDocTypeColor(docType.type)}`}>
                  {docType.type}
                </span>
                <span className="text-sm text-slate-600">{docType.count} ({docType.percentage}%)</span>
              </div>
            ))}
          </div>
        </div>
      </MetricsCard>

      {/* Quality Metrics Enrichment */}
      <MetricsCard
        title="Quality Metrics Enrichment"
        icon={<StarIcon className="w-5 h-5" />}
        className="mb-6"
      >
        <p className="text-sm text-slate-500 mb-4">RAG-readiness metrics for enriched documents (entity extraction, topic coverage, semantic context)</p>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
          <CoverageItem label="Total Entities Extracted" value={enrichment_metrics.total_entities.toLocaleString()} detail="Across all enriched documents" />
          <CoverageItem label="Total Topics Identified" value={enrichment_metrics.total_topics.toLocaleString()} detail="Key themes and subjects" />
          <CoverageItem label="Estimated Cost" value={`$${enrichment_metrics.estimated_cost.toFixed(4)}`} detail="LLM API costs (USD)" />
        </div>
      </MetricsCard>

      {/* Search & Graph Backends */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
        {/* Weaviate Status */}
        <MetricsCard
          title="Weaviate Hybrid Search"
          icon={<SearchIcon className="w-5 h-5" />}
        >
          <div className="flex items-center gap-2 mb-4">
            <span className={`inline-block w-2.5 h-2.5 rounded-full ${weaviateStatus?.status === 'ready' ? 'bg-green-500' : 'bg-red-500'}`} />
            <span className="text-sm text-slate-600 capitalize">{weaviateStatus?.status || 'Unknown'}</span>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <CoverageItem
              label="Documents Indexed"
              value={weaviateStatus?.documents_indexed?.toLocaleString() ?? '—'}
              detail="GovDocument collection"
            />
            <CoverageItem
              label="Chunks Indexed"
              value={weaviateStatus?.chunks_indexed?.toLocaleString() ?? '—'}
              detail="GovChunk collection (BM25 + vector)"
            />
          </div>
          <div className="border-t border-slate-100 pt-3 mt-4">
            <p className="text-xs text-slate-400">{weaviateStatus?.message || 'Waiting for status...'}</p>
          </div>
        </MetricsCard>

        {/* Neo4j Status */}
        <MetricsCard
          title="Neo4j Knowledge Graph"
          icon={<GraphIcon className="w-5 h-5" />}
        >
          <div className="flex items-center gap-2 mb-4">
            <span className={`inline-block w-2.5 h-2.5 rounded-full ${graphStats ? 'bg-green-500' : 'bg-red-500'}`} />
            <span className="text-sm text-slate-600">{graphStats ? 'Connected' : 'Unavailable'}</span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <CoverageItem
              label="Documents"
              value={graphStats?.documents?.toLocaleString() ?? '—'}
              detail="Document nodes"
            />
            <CoverageItem
              label="Entities"
              value={graphStats?.entities?.toLocaleString() ?? '—'}
              detail="Forms, programs, orgs..."
            />
            <CoverageItem
              label="Agencies"
              value={graphStats?.agencies?.toLocaleString() ?? '—'}
              detail="Agency nodes"
            />
            <CoverageItem
              label="Relationships"
              value={graphStats?.relationships?.toLocaleString() ?? '—'}
              detail="MENTIONS, COVERS, etc."
            />
          </div>
          {graphStats && (
            <div className="border-t border-slate-100 pt-3 mt-4">
              <p className="text-xs text-slate-400">
                {graphStats.time_periods} time periods tracked
              </p>
            </div>
          )}
        </MetricsCard>
      </div>

      {/* Per-Workflow DIS Table */}
      <MetricsCard
        title="Per-Workflow DIS Breakdown"
        icon={<WorkflowIcon className="w-5 h-5" />}
      >
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-[10px] text-slate-500 uppercase bg-slate-50">
                <th className="text-left py-3 px-4 font-medium">Workflow</th>
                <th className="text-right py-3 px-4 font-medium">DIS Score</th>
                <th className="text-right py-3 px-4 font-medium">Trend</th>
                <th className="text-right py-3 px-4 font-medium" title="Composite: Parse×0.5 + Enrich×0.25 + Rel×0.25">Quality (40%)</th>
                <th className="text-right py-3 px-4 font-medium">Efficiency (30%)</th>
                <th className="text-right py-3 px-4 font-medium">Exec Success (30%)</th>
              </tr>
            </thead>
            <tbody className="text-slate-700">
              {workflow_dis.map((wf) => (
                <tr key={wf.name} className="border-t border-slate-100 hover:bg-slate-50">
                  <td className="py-3 px-4">{wf.name}</td>
                  <td className="text-right py-3 px-4 font-semibold text-green-600">{wf.dis_score.toFixed(1)}</td>
                  <td className="text-right py-3 px-4">
                    <TrendIndicator value={wf.dis_trend} />
                  </td>
                  <td className="text-right py-3 px-4">
                    {wf.quality_score.toFixed(1)} <TrendIndicator value={wf.quality_trend} small />
                  </td>
                  <td className="text-right py-3 px-4">
                    {wf.efficiency_score.toFixed(1)} <TrendIndicator value={wf.efficiency_trend} small />
                  </td>
                  <td className="text-right py-3 px-4">
                    {wf.execution_success_score.toFixed(1)} <TrendIndicator value={wf.execution_trend} small />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </MetricsCard>
    </div>
  );
}

// Helper functions

function getDocTypeColor(type: string): string {
  const colors: Record<string, string> = {
    'Narrative': 'bg-purple-600',
    'Mixed': 'bg-cyan-600',
    'Tabular': 'bg-blue-600',
  };
  return colors[type] || 'bg-slate-500';
}

function formatTime(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`;
  }
  const seconds = ms / 1000;
  if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  }
  const minutes = seconds / 60;
  return `${minutes.toFixed(1)}m`;
}

// Components

interface SummaryCardProps {
  title: string;
  value: string | number;
  sublabel?: string;
  trend?: number;
  color?: 'green' | 'blue' | 'yellow' | 'purple';
}

function SummaryCard({ title, value, sublabel, trend, color = 'blue' }: SummaryCardProps) {
  const colorClasses = {
    green: 'text-green-600',
    blue: 'text-blue-600',
    yellow: 'text-yellow-600',
    purple: 'text-purple-600',
  };

  const renderTrend = () => {
    if (trend === undefined) return null;

    if (Math.abs(trend) < 0.1) {
      return (
        <span className="text-sm font-semibold text-slate-400">
          → 0.0
        </span>
      );
    }

    const isUp = trend > 0;
    return (
      <span className={`text-sm font-semibold ${isUp ? 'text-green-600' : 'text-red-600'}`}>
        {isUp ? '↑' : '↓'} {isUp ? '+' : ''}{trend.toFixed(1)}
      </span>
    );
  };

  return (
    <div className="bg-white rounded-xl p-4 border border-slate-200 shadow-sm">
      <h4 className="text-[10px] font-medium text-slate-500 uppercase tracking-wide mb-1">{title}</h4>
      <div className="flex items-baseline gap-2">
        <span className={`text-2xl font-bold ${colorClasses[color]}`}>{value}</span>
        {renderTrend()}
      </div>
      {sublabel && <p className="text-[10px] text-slate-400 mt-1">{sublabel}</p>}
    </div>
  );
}

interface MetricsCardProps {
  title: string;
  icon?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
}

function MetricsCard({ title, icon, children, className = '' }: MetricsCardProps) {
  return (
    <div className={`bg-white rounded-xl p-6 border border-slate-200 shadow-sm ${className}`}>
      <h3 className="text-base font-semibold text-slate-900 mb-4 flex items-center gap-2">
        {icon && <span className="text-slate-600">{icon}</span>}
        {title}
      </h3>
      {children}
    </div>
  );
}

interface CoverageItemProps {
  label: string;
  value: string | number;
  detail?: string;
}

function CoverageItem({ label, value, detail }: CoverageItemProps) {
  return (
    <div className="text-center p-4 bg-slate-50 rounded-lg border border-slate-200">
      <p className="text-xs text-slate-500 mb-1">{label}</p>
      <p className="text-xl font-bold text-blue-600">{value}</p>
      {detail && <p className="text-[10px] text-slate-400 mt-1">{detail}</p>}
    </div>
  );
}

interface StatRowProps {
  label: string;
  value: string;
  workflow: string;
  color: string;
}

function StatRow({ label, value, workflow, color }: StatRowProps) {
  return (
    <div className="flex items-center justify-between py-2 px-3 bg-slate-50 rounded-lg">
      <span className="text-xs font-semibold text-slate-500 w-10">{label}</span>
      <span className="text-sm text-slate-600 flex-1 mx-4 truncate">{workflow || 'N/A'}</span>
      <span className={`text-sm font-bold ${color}`}>{value}</span>
    </div>
  );
}

function SearchIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="11" r="8" />
      <line x1="21" y1="21" x2="16.65" y2="16.65" />
    </svg>
  );
}

function GraphIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="6" cy="6" r="3" />
      <circle cx="18" cy="6" r="3" />
      <circle cx="6" cy="18" r="3" />
      <circle cx="18" cy="18" r="3" />
      <line x1="8.5" y1="7.5" x2="15.5" y2="16.5" />
      <line x1="15.5" y1="7.5" x2="8.5" y2="16.5" />
    </svg>
  );
}

interface TrendIndicatorProps {
  value: number;
  small?: boolean;
}

function TrendIndicator({ value, small }: TrendIndicatorProps) {
  if (Math.abs(value) < 0.1) {
    return <span className={`text-slate-400 ${small ? 'text-xs' : 'text-sm'}`}>→</span>;
  }

  const isUp = value > 0;
  return (
    <span className={`${isUp ? 'text-green-600' : 'text-red-600'} ${small ? 'text-xs' : 'text-sm font-semibold'}`}>
      {isUp ? '↑' : '↓'} {!small && (isUp ? '+' : '')}{value.toFixed(1)}
    </span>
  );
}
