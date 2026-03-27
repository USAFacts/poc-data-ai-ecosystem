import axios from 'axios';
import type {
  Agency,
  AgencyDetail,
  Asset,
  AssetDetail,
  DashboardStats,
  MetricsHistory,
  MetricsSummary,
} from '../types';

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Agency endpoints
export const fetchAgencies = async (): Promise<Agency[]> => {
  const response = await api.get<Agency[]>('/agencies');
  return response.data;
};

export const fetchAgency = async (name: string): Promise<AgencyDetail> => {
  const response = await api.get<AgencyDetail>(`/agencies/${name}`);
  return response.data;
};

// Asset endpoints
export const fetchAssets = async (agency?: string): Promise<Asset[]> => {
  const params = agency ? { agency } : {};
  const response = await api.get<Asset[]>('/assets', { params });
  return response.data;
};

export const fetchAsset = async (name: string): Promise<AssetDetail> => {
  const response = await api.get<AssetDetail>(`/assets/${name}`);
  return response.data;
};

// Metrics endpoints
export const fetchMetrics = async (): Promise<MetricsSummary> => {
  const response = await api.get<MetricsSummary>('/metrics');
  return response.data;
};

export const fetchMetricsHistory = async (
  workflow?: string,
  limit = 100
): Promise<MetricsHistory> => {
  const params: Record<string, string | number> = { limit };
  if (workflow) params.workflow = workflow;
  const response = await api.get<MetricsHistory>('/metrics/history', { params });
  return response.data;
};

// Dashboard stats endpoint
export const fetchDashboardStats = async (): Promise<DashboardStats> => {
  const response = await api.get<DashboardStats>('/stats/dashboard');
  return response.data;
};

// Search/Query endpoints for Data Assistant
export interface EntityMatch {
  text: string;
  type: string;
  normalized: string;
}

export interface QueryDecomposition {
  original_query: string;
  entities: EntityMatch[];
  keywords: string[];
  intent: string;
}

export interface DocumentReference {
  document_id: string;
  document_title: string;
  asset_name: string;
  agency_name: string;
  agency_id: string;
  section: string | null;
  section_summary: string | null;
  relevance_score: number;
  snippet: string;
  // New fields for better attribution
  original_url: string | null;  // Path to landing-zone file
  source_url: string | null;  // Original URL where document was acquired
  page_count: number | null;  // Total pages in document
  page_number: number | null;  // Page number of relevant section
  sheet_name: string | null;  // Sheet name for Excel files
  file_format: string | null;  // File format (PDF, XLSX, CSV, etc.)
}

export interface UsageMetrics {
  input_tokens: number;
  output_tokens: number;
  total_tokens: number;
  context_window_used_percent: number;
  documents_searched: number;
  documents_returned: number;
  data_volume_bytes: number;
  data_volume_display: string;
}

export interface ChartSpec {
  chart_type: 'bar' | 'line' | 'pie';
  title: string;
  x_label: string;
  y_label: string;
  data: Array<{ label: string; value: number; [key: string]: any }>;
}

export interface AnswerMetrics {
  sts: number;   // Source Traceability Score (0-1)
  nvs: number;   // Numerical Verification Score (0-1)
  hds: number;   // Hallucination Detection Score (0 = best, count of flags)
  cscs: number;  // Cross-Store Consistency Score (0-1)
}

export interface TraceStep {
  stage: string;
  detail: string;
  duration_ms: number;
}

export interface SearchResult {
  query_decomposition: QueryDecomposition;
  documents: DocumentReference[];
  answer: string;
  brief_answer: string;
  confidence: number;
  usage: UsageMetrics | null;
  claude_used: boolean;
  charts: ChartSpec[];
  metrics: AnswerMetrics | null;
  trace: TraceStep[];
}

export interface AvailableEntities {
  forms: string[];
  visa_types: string[];
  programs: string[];
  organizations: string[];
  topics: string[];
}

export const searchQuery = async (query: string, mode?: string): Promise<SearchResult> => {
  const response = await api.post<SearchResult>('/search/query', { query, mode });
  return response.data;
};

export const fetchAvailableEntities = async (): Promise<AvailableEntities> => {
  const response = await api.get<AvailableEntities>('/search/entities');
  return response.data;
};

// Asset Report types (matching HTML report structure)
export interface TableQuality {
  rowCount: number;
  columnCount: number;
  cellFillRate: number;
  hasHeaders: boolean;
  headerQuality: number;
  dataTypeConsistency: number;
}

export interface TextStructure {
  headingCount: number;
  headingHierarchyDepth: number;
  headingHierarchyValid: boolean;
  paragraphCount: number;
  avgParagraphLength: number;
  listCount: number;
  listItemsTotal: number;
  sentenceCount: number;
  avgSentenceLength: number;
  structureScore: number;
  completenessScore: number;
  formatting: {
    hasBold: boolean;
    hasItalic: boolean;
    hasLinks: boolean;
    hasCodeBlocks: boolean;
  };
}

export interface StepReport {
  name: string;
  type: string;
  status: string;
  run_id: string | null;
  zone: string | null;
  object_path: string | null;
  object_size: number | null;
  duration_ms: number | null;
  // Onboarding fields
  description: string | null;
  labels: Record<string, string> | null;
  source_url: string | null;
  schedule: string | null;
  // Acquisition fields
  file_format: string | null;
  acquisition_method: string | null;
  // Parse fields
  parser_type: string | null;
  page_count: number | null;  // Pages for PDF, sheets for Excel
  quality_scores: Record<string, number> | null;
  document_type: string | null;
  table_count: number | null;
  section_count: number | null;
  token_count: number | null;
  tables_quality: TableQuality[] | null;  // Table quality metrics
  text_structure: TextStructure | null;  // Text structure metrics
  mime_type: string | null;  // To distinguish PDF vs Excel
  // Chunk fields
  chunk_count: number | null;
  document_chunks: number | null;
  section_chunks: number | null;
  table_chunks: number | null;
  // Enrichment fields
  enricher_type: string | null;
  enrichment_model: string | null;
  entity_count: number | null;
  topic_count: number | null;
  enrichment_quality_score: number | null;
  has_embedding: boolean | null;
  // Sync fields
  weaviate_synced: boolean | null;
  neo4j_synced: boolean | null;
}

export interface WorkflowReport {
  name: string;
  asset_name: string;
  agency_name: string;
  agency_full_name: string;
  run_id: string | null;
  steps: StepReport[];
  overall_status: string;
  quality_score: number | null;
  parse_quality_score: number | null;
  enrichment_quality_score: number | null;
  total_duration_ms: number | null;
  last_run: string | null;
  file_format: string | null;
}

export interface AssetReportResponse {
  workflows: WorkflowReport[];
  filters: {
    agencies: string[];
    formats: string[];
    steps: string[];
  };
  generated_at: string;
}

export const fetchAssetReports = async (
  agency?: string,
  format?: string,
  step?: string
): Promise<AssetReportResponse> => {
  const params: Record<string, string> = {};
  if (agency) params.agency = agency;
  if (format) params.format = format;
  if (step) params.step = step;
  const response = await api.get<AssetReportResponse>('/asset-reports/report', { params });
  return response.data;
};

// Search Index Management endpoints
export interface IndexStatus {
  status: string;
  documents_indexed: number;
  chunks_indexed?: number;
  message: string;
}

export interface IndexRefreshResult {
  status: string;
  message: string;
  documents_indexed: number;
  chunks_indexed?: number;
}

export const fetchIndexStatus = async (): Promise<IndexStatus> => {
  const response = await api.get<IndexStatus>('/search/index-status');
  return response.data;
};

export const refreshSearchIndex = async (): Promise<IndexRefreshResult> => {
  const response = await api.post<IndexRefreshResult>('/search/refresh-index');
  return response.data;
};

// Graph API endpoints
export interface GraphStats {
  documents: number;
  entities: number;
  agencies: number;
  time_periods: number;
  relationships: number;
}

export const fetchGraphStats = async (): Promise<GraphStats> => {
  const response = await api.get<GraphStats>('/graph/stats');
  return response.data;
};

// Weaviate Status
// Weaviate Status (matches API response shape)
export interface WeaviateProperty {
  name: string;
  data_type: string;
  tokenization?: string | null;
}

export interface WeaviateCollection {
  name: string;
  object_count: number;
  properties: WeaviateProperty[];
}

export interface WeaviateStatus {
  status: string;
  connection: {
    host: string;
    port: number;
    grpc_port: number;
  };
  collections: WeaviateCollection[];
  total_objects: number;
}

export const fetchWeaviateStatus = async (): Promise<WeaviateStatus> => {
  const response = await api.get<WeaviateStatus>('/weaviate/status');
  return response.data;
};

// Neo4j Status (matches API response shape)
export interface Neo4jTopEntity {
  name: string;
  type: string;
  mention_count: number;
}

export interface Neo4jAgencyDoc {
  name: string;
  document_count: number;
}

export interface Neo4jConstraint {
  name: string;
  type: string;
  entityType: string;
}

export interface Neo4jIndex {
  name: string;
  type: string;
  entityType: string;
  state: string;
}

export interface Neo4jSchemaOverviewItem {
  from: string;
  relationship: string;
  to: string;
}

export interface Neo4jStatus {
  status: string;
  connection: {
    uri: string;
  };
  node_counts: Record<string, number>;
  relationship_counts: Record<string, number>;
  top_entities: Neo4jTopEntity[];
  top_agencies: Neo4jAgencyDoc[];
  constraints: Neo4jConstraint[];
  indexes: Neo4jIndex[];
  schema_overview: Neo4jSchemaOverviewItem[];
}

export interface Neo4jSchemaNode {
  label: string;
  count: number;
  color: string;
}

export interface Neo4jSchemaEdge {
  from: string;
  to: string;
  type: string;
  count: number;
}

export interface Neo4jSchema {
  nodes: Neo4jSchemaNode[];
  edges: Neo4jSchemaEdge[];
}

export const fetchNeo4jStatus = async (): Promise<Neo4jStatus> => {
  const response = await api.get<Neo4jStatus>('/neo4j/status');
  return response.data;
};

export const fetchNeo4jSchema = async (): Promise<Neo4jSchema> => {
  const response = await api.get<Neo4jSchema>('/neo4j/schema');
  return response.data;
};

// Experiment Tracker
export interface Experiment {
  id: number;
  name: string;
  status: string;
  total_questions: number;
  completed_questions: number;
  started_at: string | null;
  completed_at: string | null;
  aggregate_metrics: AggregateMetrics | null;
  config: { modes: string[]; sample_percent: number } | null;
  created_at: string;
}

export interface ModeMetrics {
  mean_confidence: number;
  mean_relevance: number;
  mean_entity_coverage: number;
  mean_response_time_ms: number;
  mean_tokens: number;
  mean_sts: number;
  mean_nvs: number;
  mean_hds: number;
  mean_cscs: number;
}

export interface WinCounts {
  weaviate_only: number;
  weaviate_graph: number;
  tie: number;
}

export interface CategoryBreakdown {
  weaviate_only: ModeMetrics;
  weaviate_graph: ModeMetrics;
  wins: WinCounts;
}

// Legacy interfaces kept for backward compatibility
export interface LegacyAggregateMetrics {
  weaviate_only: ModeMetrics;
  weaviate_graph: ModeMetrics;
  wins: WinCounts;
  by_category: Record<string, CategoryBreakdown>;
}

// New N-mode aggregate metrics
export interface AggregateMetrics {
  // New format: dynamic mode keys
  by_mode?: Record<string, ModeMetrics>;
  wins?: Record<string, number>; // mode_id -> win count, plus "tie"
  by_category?: Record<string, {
    by_mode: Record<string, ModeMetrics>;
    wins: Record<string, number>;
  }>;
  // Legacy format fields (backward compatibility)
  weaviate_only?: ModeMetrics;
  weaviate_graph?: ModeMetrics;
}

export interface ExperimentResult {
  id: number;
  question_id: string;
  question_text: string;
  category: string;
  mode: string;
  answer: string;
  confidence: number;
  avg_relevance_score: number;
  entity_coverage: number;
  response_time_ms: number;
  total_tokens: number;
  documents_returned: number;
  sts: number;
  nvs: number;
  hds: number;
  cscs: number;
}

export interface QuestionComparison {
  question_id: string;
  question_text: string;
  category: string;
  // New format: dynamic mode keys
  modes?: Record<string, ExperimentResult>;
  // Legacy format (backward compatibility)
  weaviate_only?: ExperimentResult | null;
  weaviate_graph?: ExperimentResult | null;
  winner: string;
}

export interface SearchMode {
  id: string;
  label: string;
  available: boolean;
}

export interface TestQuestion {
  id: string;
  category: string;
  question: string;
  expected_entities: string[];
}

export const createExperiment = async (name: string, modes?: string[], samplePercent?: number): Promise<Experiment> => {
  const response = await api.post<Experiment>('/experiments', { name, modes, sample_percent: samplePercent });
  return response.data;
};

export const fetchSearchModes = async (): Promise<SearchMode[]> => {
  const response = await api.get<SearchMode[]>('/experiments/modes');
  return response.data;
};

export const deleteExperiment = async (id: number): Promise<void> => {
  await api.delete(`/experiments/${id}`);
};

export const fetchExperiments = async (): Promise<Experiment[]> => {
  const response = await api.get<Experiment[]>('/experiments');
  return response.data;
};

export const fetchExperiment = async (id: number): Promise<Experiment> => {
  const response = await api.get<Experiment>(`/experiments/${id}`);
  return response.data;
};

export const fetchExperimentResults = async (id: number, category?: string, mode?: string): Promise<ExperimentResult[]> => {
  const params: Record<string, string> = {};
  if (category) params.category = category;
  if (mode) params.mode = mode;
  const response = await api.get<ExperimentResult[]>(`/experiments/${id}/results`, { params });
  return response.data;
};

export const fetchExperimentComparison = async (id: number): Promise<QuestionComparison[]> => {
  const response = await api.get<QuestionComparison[]>(`/experiments/${id}/comparison`);
  return response.data;
};

export const fetchTestQuestions = async (): Promise<TestQuestion[]> => {
  const response = await api.get<TestQuestion[]>('/experiments/questions');
  return response.data;
};

export default api;
