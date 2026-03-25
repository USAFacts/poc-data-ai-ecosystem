import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { fetchAssetReports, type WorkflowReport, type StepReport } from '../api/client';

// Helper functions matching HTML report
function formatSize(sizeBytes: number | null): string {
  if (sizeBytes === null) return 'N/A';
  if (sizeBytes < 1024) return `${sizeBytes} B`;
  if (sizeBytes < 1024 * 1024) return `${(sizeBytes / 1024).toFixed(1)} KB`;
  if (sizeBytes < 1024 * 1024 * 1024) return `${(sizeBytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(sizeBytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function formatDuration(durationMs: number | null): string {
  if (durationMs === null) return 'N/A';
  if (durationMs < 1000) return `${durationMs}ms`;
  if (durationMs < 60000) return `${(durationMs / 1000).toFixed(2)}s`;
  const minutes = Math.floor(durationMs / 60000);
  const seconds = (durationMs % 60000) / 1000;
  return `${minutes}m ${seconds.toFixed(1)}s`;
}

function formatRelativeTime(isoDate: string | null): string {
  if (!isoDate) return 'Never';
  const date = new Date(isoDate);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 1) return 'Just now';
  if (diffMins === 1) return '1 minute ago';
  if (diffMins < 60) return `${diffMins} minutes ago`;
  if (diffHours === 1) return '1 hour ago';
  if (diffHours < 24) return `${diffHours} hours ago`;
  if (diffDays === 1) return 'Yesterday';
  if (diffDays < 7) return `${diffDays} days ago`;
  return date.toLocaleDateString();
}

function getStatusColor(status: string): string {
  const colors: Record<string, string> = {
    success: '#16a34a',
    partial: '#ca8a04',
    failed: '#dc2626',
    not_run: '#6b7280',
    pending: '#2563eb',
  };
  return colors[status] || '#6b7280';
}

function getQualityBadge(score: number | null): { color: string; label: string } {
  if (score === null) return { color: '#6b7280', label: 'N/A' };
  if (score >= 90) return { color: '#16a34a', label: 'Excellent' };
  if (score >= 70) return { color: '#65a30d', label: 'Good' };
  if (score >= 50) return { color: '#ca8a04', label: 'Fair' };
  return { color: '#dc2626', label: 'Poor' };
}

function getLatestStep(workflow: WorkflowReport): string {
  for (let i = workflow.steps.length - 1; i >= 0; i--) {
    if (workflow.steps[i].status === 'success' && workflow.steps[i].type !== 'onboarding') {
      return workflow.steps[i].type;
    }
  }
  return 'onboarding';
}

function getStepBadgeColor(stepType: string): string {
  const colors: Record<string, string> = {
    onboarding: '#6b7280',
    acquisition: '#2563eb',
    parse: '#7c3aed',
    chunk: '#6d28d9',
    enrichment: '#0891b2',
    sync: '#059669',
  };
  return colors[stepType] || '#6b7280';
}

function getDocTypeBadgeColor(docType: string): string {
  const colors: Record<string, string> = {
    tabular: '#2563eb',
    narrative: '#7c3aed',
    mixed: '#0891b2',
  };
  return colors[docType.toLowerCase()] || '#6b7280';
}

// Step Card Component
function StepCard({ step, minioConsoleUrl }: { step: StepReport; minioConsoleUrl: string }) {
  const [isExpanded, setIsExpanded] = useState(false);

  const getStorageLink = () => {
    if (!step.object_path) return null;
    const pathParts = step.object_path.split('/');
    const dirPath = pathParts.slice(0, -1).join('/');
    return `${minioConsoleUrl}/browser/gov-data-lake/${dirPath}`;
  };

  return (
    <div className="border border-slate-200 rounded-lg mb-2 bg-white overflow-hidden">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between px-4 py-3 hover:bg-slate-50 transition-colors"
      >
        <div className="flex items-center gap-3">
          <span className="text-slate-400 text-sm">{isExpanded ? '▼' : '▶'}</span>
          <span className="font-medium text-slate-800 capitalize">{step.name}</span>
          <span className="text-slate-500 text-sm">({step.type})</span>
        </div>
        <span
          className="px-2 py-1 rounded text-xs font-medium text-white"
          style={{ backgroundColor: getStatusColor(step.status) }}
        >
          {step.status.replace('_', ' ').replace(/\b\w/g, c => c.toUpperCase())}
        </span>
      </button>

      {isExpanded && (
        <div className="px-4 py-3 border-t border-slate-100 bg-slate-50 text-sm space-y-2">
          {/* Onboarding Details */}
          {step.type === 'onboarding' && (
            <>
              {step.description && (
                <div className="flex gap-2">
                  <span className="text-slate-500 w-28 shrink-0">Description:</span>
                  <span className="text-slate-700">{step.description}</span>
                </div>
              )}
              {step.schedule && (
                <div className="flex gap-2">
                  <span className="text-slate-500 w-28 shrink-0">Schedule:</span>
                  <span className="px-2 py-0.5 bg-blue-100 text-blue-800 rounded text-xs font-medium">
                    {step.schedule}
                  </span>
                </div>
              )}
              {step.labels && Object.keys(step.labels).length > 0 && (
                <div className="flex gap-2">
                  <span className="text-slate-500 w-28 shrink-0">Labels:</span>
                  <div className="flex flex-wrap gap-1">
                    {Object.entries(step.labels).map(([key, value]) => (
                      <span
                        key={key}
                        className="px-2 py-0.5 bg-slate-200 text-slate-700 rounded text-xs"
                      >
                        {key}: {value}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}

          {/* Acquisition Details */}
          {step.type === 'acquisition' && (
            <>
              {step.acquisition_method && (
                <div className="flex gap-2">
                  <span className="text-slate-500 w-28 shrink-0">Method:</span>
                  <span className="px-2 py-0.5 bg-green-100 text-green-800 rounded text-xs font-medium uppercase">
                    {step.acquisition_method}
                  </span>
                </div>
              )}
              {step.file_format && (
                <div className="flex gap-2">
                  <span className="text-slate-500 w-28 shrink-0">Format:</span>
                  <span className="px-2 py-0.5 bg-purple-100 text-purple-800 rounded text-xs font-medium">
                    {step.file_format}
                  </span>
                </div>
              )}
            </>
          )}

          {/* Parse Details */}
          {step.type === 'parse' && (
            <>
              {step.parser_type && (
                <div className="flex gap-2">
                  <span className="text-slate-500 w-28 shrink-0">Parser:</span>
                  <span className={`px-2 py-0.5 rounded text-xs font-medium uppercase ${
                    step.parser_type.includes('vision') || step.parser_type === 'auto'
                      ? 'bg-violet-100 text-violet-800'
                      : 'bg-indigo-100 text-indigo-800'
                  }`}>
                    {step.parser_type.includes('vision') ? 'HYBRID' :
                     step.parser_type === 'auto' ? 'HYBRID' :
                     step.parser_type}
                  </span>
                </div>
              )}
              {step.page_count !== null && step.page_count > 0 && (
                <div className="flex gap-2">
                  <span className="text-slate-500 w-28 shrink-0">
                    {step.mime_type?.includes('pdf') ? 'Pages:' :
                     step.mime_type?.includes('spreadsheet') || step.mime_type?.includes('excel') ? 'Sheets:' :
                     'Pages/Sheets:'}
                  </span>
                  <span className="text-slate-700">{step.page_count}</span>
                </div>
              )}
              {step.quality_scores && (
                <div className="mt-3 p-3 bg-white rounded border border-slate-200">
                  <div className="flex items-center gap-2 mb-2">
                    <span className="font-medium text-slate-700">Quality Metrics</span>
                    {step.document_type && (
                      <span
                        className="px-2 py-0.5 rounded text-xs font-medium text-white"
                        style={{ backgroundColor: getDocTypeBadgeColor(step.document_type) }}
                      >
                        {step.document_type}
                      </span>
                    )}
                  </div>
                  <div className="grid grid-cols-4 gap-3">
                    {step.quality_scores.overall !== undefined && (
                      <div className="text-center">
                        <div className="text-xs text-slate-500">Overall</div>
                        <div className="font-semibold text-slate-800">{step.quality_scores.overall.toFixed(1)}</div>
                      </div>
                    )}
                    {step.quality_scores.extraction !== undefined && (
                      <div className="text-center">
                        <div className="text-xs text-slate-500">Extraction</div>
                        <div className="font-semibold text-slate-800">{step.quality_scores.extraction.toFixed(1)}</div>
                      </div>
                    )}
                    {step.quality_scores.structural !== undefined && (
                      <div className="text-center">
                        <div className="text-xs text-slate-500">Structural</div>
                        <div className="font-semibold text-slate-800">{step.quality_scores.structural.toFixed(1)}</div>
                      </div>
                    )}
                    {step.quality_scores.aiReadiness !== undefined && (
                      <div className="text-center">
                        <div className="text-xs text-slate-500">AI Ready</div>
                        <div className="font-semibold text-slate-800">{step.quality_scores.aiReadiness.toFixed(1)}</div>
                      </div>
                    )}
                  </div>
                </div>
              )}
              {(step.table_count || step.section_count || step.token_count) && (
                <div className="grid grid-cols-3 gap-2 mt-2">
                  {step.table_count !== null && (
                    <div className="text-center p-2 bg-white rounded border border-slate-200">
                      <div className="text-xs text-slate-500">Tables</div>
                      <div className="font-semibold text-slate-800">{step.table_count}</div>
                    </div>
                  )}
                  {step.section_count !== null && (
                    <div className="text-center p-2 bg-white rounded border border-slate-200">
                      <div className="text-xs text-slate-500">Sections</div>
                      <div className="font-semibold text-slate-800">{step.section_count}</div>
                    </div>
                  )}
                  {step.token_count !== null && (
                    <div className="text-center p-2 bg-white rounded border border-slate-200">
                      <div className="text-xs text-slate-500">Tokens</div>
                      <div className="font-semibold text-slate-800">{step.token_count.toLocaleString()}</div>
                    </div>
                  )}
                </div>
              )}
              {/* Table Quality Summary */}
              {step.tables_quality && step.tables_quality.length > 0 && (
                <div className="mt-3 p-3 bg-white rounded border border-slate-200">
                  <div className="font-medium text-slate-700 mb-2">Table Quality ({step.tables_quality.length} tables)</div>
                  <div className="grid grid-cols-3 gap-3 text-center">
                    <div>
                      <div className="text-xs text-slate-500">Avg Cell Fill</div>
                      <div className="font-semibold text-slate-800">
                        {(step.tables_quality.reduce((sum, t) => sum + t.cellFillRate, 0) / step.tables_quality.length).toFixed(1)}%
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-slate-500">Avg Header Quality</div>
                      <div className="font-semibold text-slate-800">
                        {(step.tables_quality.reduce((sum, t) => sum + t.headerQuality, 0) / step.tables_quality.length).toFixed(1)}%
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-slate-500">Avg Type Consistency</div>
                      <div className="font-semibold text-slate-800">
                        {(step.tables_quality.reduce((sum, t) => sum + t.dataTypeConsistency, 0) / step.tables_quality.length).toFixed(1)}%
                      </div>
                    </div>
                  </div>
                </div>
              )}
              {/* Text Structure */}
              {step.text_structure && (
                <div className="mt-3 p-3 bg-white rounded border border-slate-200">
                  <div className="font-medium text-slate-700 mb-2">Text Structure</div>
                  <div className="grid grid-cols-3 sm:grid-cols-6 gap-3 text-center mb-3">
                    <div>
                      <div className="text-xs text-slate-500">Headings</div>
                      <div className="font-semibold text-slate-800">{step.text_structure.headingCount || 0}</div>
                    </div>
                    <div>
                      <div className="text-xs text-slate-500">Hierarchy</div>
                      <div className={`font-semibold ${step.text_structure.headingHierarchyValid ? 'text-green-600' : 'text-red-600'}`}>
                        {step.text_structure.headingHierarchyValid ? 'Valid' : 'Invalid'}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-slate-500">Paragraphs</div>
                      <div className="font-semibold text-slate-800">{step.text_structure.paragraphCount || 0}</div>
                    </div>
                    <div>
                      <div className="text-xs text-slate-500">Avg Length</div>
                      <div className="font-semibold text-slate-800">{(step.text_structure.avgParagraphLength || 0).toFixed(0)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-slate-500">Lists</div>
                      <div className="font-semibold text-slate-800">
                        {step.text_structure.listCount || 0}
                        {(step.text_structure.listItemsTotal || 0) > 0 && (
                          <span className="text-xs text-slate-500 ml-1">({step.text_structure.listItemsTotal} items)</span>
                        )}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-slate-500">Sentences</div>
                      <div className="font-semibold text-slate-800">{step.text_structure.sentenceCount || 0}</div>
                    </div>
                  </div>
                  <div className="flex justify-center gap-6 text-sm">
                    <span className="text-slate-600">
                      Structure: <span className="font-semibold text-slate-800">{(step.text_structure.structureScore || 0).toFixed(1)}</span>
                    </span>
                    <span className="text-slate-600">
                      Completeness: <span className="font-semibold text-slate-800">{(step.text_structure.completenessScore || 0).toFixed(1)}</span>
                    </span>
                    <span className="text-slate-600">
                      Formatting: <span className="font-semibold text-slate-800">
                        {step.text_structure.formatting ?
                          [
                            step.text_structure.formatting.hasBold && 'Bold',
                            step.text_structure.formatting.hasItalic && 'Italic',
                            step.text_structure.formatting.hasLinks && 'Links',
                            step.text_structure.formatting.hasCodeBlocks && 'Code'
                          ].filter(Boolean).join(', ') || 'None'
                          : 'N/A'}
                      </span>
                    </span>
                  </div>
                </div>
              )}
            </>
          )}

          {/* Enrichment Details */}
          {step.type === 'enrichment' && (
            <>
              {step.enricher_type && (
                <div className="flex gap-2">
                  <span className="text-slate-500 w-28 shrink-0">Enricher:</span>
                  <span className="px-2 py-0.5 bg-cyan-100 text-cyan-800 rounded text-xs font-medium uppercase">
                    {step.enricher_type}
                  </span>
                </div>
              )}
              {step.enrichment_model && (
                <div className="flex gap-2">
                  <span className="text-slate-500 w-28 shrink-0">Model:</span>
                  <span className="text-slate-700 font-mono text-xs">{step.enrichment_model}</span>
                </div>
              )}
              <div className="grid grid-cols-3 gap-2 mt-2">
                {step.entity_count !== null && (
                  <div className="text-center p-2 bg-white rounded border border-slate-200">
                    <div className="text-xs text-slate-500">Entities</div>
                    <div className="font-semibold text-slate-800">{step.entity_count}</div>
                  </div>
                )}
                {step.topic_count !== null && (
                  <div className="text-center p-2 bg-white rounded border border-slate-200">
                    <div className="text-xs text-slate-500">Topics</div>
                    <div className="font-semibold text-slate-800">{step.topic_count}</div>
                  </div>
                )}
                {step.enrichment_quality_score !== null && (
                  <div className="text-center p-2 bg-white rounded border border-slate-200">
                    <div className="text-xs text-slate-500">Quality</div>
                    <div className="font-semibold text-slate-800">{step.enrichment_quality_score.toFixed(1)}</div>
                  </div>
                )}
              </div>
            </>
          )}

          {/* Chunk Details */}
          {step.type === 'chunk' && step.status === 'success' && (
            <>
              {step.chunk_count !== null && (
                <div className="grid grid-cols-4 gap-2 mt-2">
                  <div className="text-center p-2 bg-white rounded border border-slate-200">
                    <div className="text-xs text-slate-500">Total Chunks</div>
                    <div className="font-semibold text-slate-800">{step.chunk_count}</div>
                  </div>
                  <div className="text-center p-2 bg-white rounded border border-slate-200">
                    <div className="text-xs text-slate-500">Document</div>
                    <div className="font-semibold text-slate-800">{step.document_chunks ?? 0}</div>
                  </div>
                  <div className="text-center p-2 bg-white rounded border border-slate-200">
                    <div className="text-xs text-slate-500">Sections</div>
                    <div className="font-semibold text-slate-800">{step.section_chunks ?? 0}</div>
                  </div>
                  <div className="text-center p-2 bg-white rounded border border-slate-200">
                    <div className="text-xs text-slate-500">Tables</div>
                    <div className="font-semibold text-slate-800">{step.table_chunks ?? 0}</div>
                  </div>
                </div>
              )}
            </>
          )}

          {/* Sync Details */}
          {step.type === 'sync' && (
            <div className="grid grid-cols-3 gap-2 mt-2">
              <div className="text-center p-2 bg-white rounded border border-slate-200">
                <div className="text-xs text-slate-500">Weaviate</div>
                <div className={`font-semibold ${step.weaviate_synced ? 'text-green-600' : 'text-red-500'}`}>
                  {step.weaviate_synced ? 'Synced' : 'Not synced'}
                </div>
              </div>
              <div className="text-center p-2 bg-white rounded border border-slate-200">
                <div className="text-xs text-slate-500">Neo4j</div>
                <div className={`font-semibold ${step.neo4j_synced ? 'text-green-600' : 'text-red-500'}`}>
                  {step.neo4j_synced ? 'Synced' : 'Not synced'}
                </div>
              </div>
              <div className="text-center p-2 bg-white rounded border border-slate-200">
                <div className="text-xs text-slate-500">Embedding</div>
                <div className={`font-semibold ${step.has_embedding ? 'text-green-600' : 'text-yellow-500'}`}>
                  {step.has_embedding ? '384-dim' : 'Missing'}
                </div>
              </div>
            </div>
          )}

          {/* Common details for all steps with data */}
          {step.run_id && (
            <div className="flex gap-2">
              <span className="text-slate-500 w-28 shrink-0">Run ID:</span>
              <span className="px-2 py-0.5 bg-purple-100 text-purple-800 rounded font-mono text-xs">
                {step.run_id}
              </span>
            </div>
          )}
          {step.duration_ms !== null && (
            <div className="flex gap-2">
              <span className="text-slate-500 w-28 shrink-0">Duration:</span>
              <span className="text-slate-700">{formatDuration(step.duration_ms)}</span>
            </div>
          )}
          {step.object_path && (
            <div className="flex gap-2">
              <span className="text-slate-500 w-28 shrink-0">Storage:</span>
              <a
                href={getStorageLink() || '#'}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 hover:text-blue-800 hover:underline font-mono text-xs break-all"
              >
                {step.object_path}
              </a>
            </div>
          )}
          {step.object_size !== null && (
            <div className="flex gap-2">
              <span className="text-slate-500 w-28 shrink-0">Size:</span>
              <span className="text-slate-700">{formatSize(step.object_size)}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// Workflow Item Component
function WorkflowItem({ workflow, minioConsoleUrl }: { workflow: WorkflowReport; minioConsoleUrl: string }) {
  const [isExpanded, setIsExpanded] = useState(false);
  const qualityBadge = getQualityBadge(workflow.quality_score);
  const latestStep = getLatestStep(workflow);

  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm mb-3 overflow-hidden">
      {/* Header */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between px-4 py-3 hover:bg-slate-50 transition-colors"
      >
        <div className="flex items-center gap-3">
          <span className="text-slate-400 font-medium">{isExpanded ? '−' : '+'}</span>
          <div className="text-left">
            <span className="font-semibold text-slate-800">{workflow.asset_name}</span>
            <span className="text-slate-500 text-sm ml-2">{workflow.agency_full_name}</span>
          </div>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <span
            className="px-2 py-1 rounded text-xs font-medium text-white capitalize"
            style={{ backgroundColor: getStepBadgeColor(latestStep) }}
          >
            {latestStep}
          </span>
          {workflow.total_duration_ms !== null && (
            <span className="px-2 py-1 bg-slate-100 text-slate-600 rounded text-xs">
              {formatDuration(workflow.total_duration_ms)}
            </span>
          )}
          <span className="text-slate-400 text-xs hidden sm:inline">
            {formatRelativeTime(workflow.last_run)}
          </span>
          <span
            className="px-2 py-1 rounded text-xs font-medium text-white capitalize"
            style={{ backgroundColor: getStatusColor(workflow.overall_status) }}
          >
            {workflow.overall_status.replace('_', ' ')}
          </span>
          {workflow.quality_score !== null && (
            <span
              className="px-2 py-1 rounded text-xs font-medium text-white"
              style={{ backgroundColor: qualityBadge.color }}
            >
              {workflow.quality_score.toFixed(1)} ({qualityBadge.label})
            </span>
          )}
        </div>
      </button>

      {/* Expanded Details */}
      {isExpanded && (
        <div className="px-4 py-3 border-t border-slate-100 bg-slate-50">
          {workflow.run_id && (
            <div className="flex items-center gap-2 mb-3 text-sm">
              <span className="text-slate-500">Run ID:</span>
              <span className="px-2 py-0.5 bg-purple-100 text-purple-800 rounded font-mono text-xs">
                {workflow.run_id}
              </span>
            </div>
          )}
          <div className="space-y-1">
            {workflow.steps.map((step, idx) => (
              <StepCard key={idx} step={step} minioConsoleUrl={minioConsoleUrl} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default function AssetsPage() {
  const [agencyFilter, setAgencyFilter] = useState<string>('');
  const [formatFilter, setFormatFilter] = useState<string>('');
  const [stepFilter, setStepFilter] = useState<string>('');
  const [searchQuery, setSearchQuery] = useState<string>('');

  const minioConsoleUrl = 'http://localhost:9001';

  const { data, isLoading, error } = useQuery({
    queryKey: ['assetReports', agencyFilter, formatFilter, stepFilter],
    queryFn: () => fetchAssetReports(
      agencyFilter || undefined,
      formatFilter || undefined,
      stepFilter || undefined
    ),
  });

  // Filter by search query
  const filteredWorkflows = data?.workflows.filter(workflow => {
    if (!searchQuery) return true;
    const query = searchQuery.toLowerCase();
    return (
      workflow.asset_name.toLowerCase().includes(query) ||
      workflow.agency_name.toLowerCase().includes(query) ||
      workflow.agency_full_name.toLowerCase().includes(query)
    );
  }) || [];

  if (isLoading) {
    return (
      <div className="p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-slate-200 rounded w-1/4"></div>
          <div className="h-12 bg-slate-200 rounded"></div>
          <div className="h-24 bg-slate-200 rounded"></div>
          <div className="h-24 bg-slate-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6">
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
          Error loading asset reports: {(error as Error).message}
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-slate-800">Assets Pipeline Report</h1>
        <p className="text-slate-500 text-sm mt-1">
          Detailed view of all data assets and their processing status
        </p>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-lg border border-slate-200 p-4 mb-6">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {/* Search */}
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1">Search</label>
            <input
              type="text"
              placeholder="Search assets..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          {/* Agency Filter */}
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1">Agency</label>
            <select
              value={agencyFilter}
              onChange={(e) => setAgencyFilter(e.target.value)}
              className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">All Agencies</option>
              {data?.filters.agencies.map((agency) => (
                <option key={agency} value={agency}>
                  {agency}
                </option>
              ))}
            </select>
          </div>

          {/* Format Filter */}
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1">Format</label>
            <select
              value={formatFilter}
              onChange={(e) => setFormatFilter(e.target.value)}
              className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">All Formats</option>
              {data?.filters.formats.map((format) => (
                <option key={format} value={format}>
                  {format}
                </option>
              ))}
            </select>
          </div>

          {/* Step Filter */}
          <div>
            <label className="block text-xs font-medium text-slate-500 mb-1">Latest Step</label>
            <select
              value={stepFilter}
              onChange={(e) => setStepFilter(e.target.value)}
              className="w-full px-3 py-2 border border-slate-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="">All Steps</option>
              {data?.filters.steps.map((step) => (
                <option key={step} value={step}>
                  {step.charAt(0).toUpperCase() + step.slice(1)}
                </option>
              ))}
            </select>
          </div>
        </div>
      </div>

      {/* Summary Stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-6">
        <div className="bg-white rounded-lg border border-slate-200 p-4 text-center">
          <div className="text-2xl font-bold text-slate-800">{filteredWorkflows.length}</div>
          <div className="text-xs text-slate-500 uppercase tracking-wide">Total Assets</div>
        </div>
        <div className="bg-white rounded-lg border border-slate-200 p-4 text-center">
          <div className="text-2xl font-bold text-green-600">
            {filteredWorkflows.filter(w => w.overall_status === 'success').length}
          </div>
          <div className="text-xs text-slate-500 uppercase tracking-wide">Successful</div>
        </div>
        <div className="bg-white rounded-lg border border-slate-200 p-4 text-center">
          <div className="text-2xl font-bold text-blue-600">
            {data?.filters.agencies.length || 0}
          </div>
          <div className="text-xs text-slate-500 uppercase tracking-wide">Agencies</div>
        </div>
        <div className="bg-white rounded-lg border border-slate-200 p-4 text-center">
          <div className="text-2xl font-bold text-purple-600">
            {data?.filters.formats.length || 0}
          </div>
          <div className="text-xs text-slate-500 uppercase tracking-wide">Formats</div>
        </div>
      </div>

      {/* Workflows List */}
      <div className="space-y-0">
        {filteredWorkflows.length === 0 ? (
          <div className="bg-slate-50 rounded-lg border border-slate-200 p-8 text-center text-slate-500">
            No assets match the current filters
          </div>
        ) : (
          filteredWorkflows.map((workflow) => (
            <WorkflowItem
              key={workflow.name}
              workflow={workflow}
              minioConsoleUrl={minioConsoleUrl}
            />
          ))
        )}
      </div>

      {/* Footer */}
      {data && (
        <div className="mt-6 text-center text-xs text-slate-400">
          Generated at {new Date(data.generated_at).toLocaleString()}
        </div>
      )}
    </div>
  );
}
