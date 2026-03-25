// Agency types
export interface Agency {
  id: number;
  name: string;
  full_name: string;
  base_url: string | null;
  description: string | null;
  labels: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;
  asset_count: number;
}

export interface AgencyDetail extends Agency {
  assets: AssetSummary[];
}

// Asset types
export interface AssetSummary {
  id: number;
  name: string;
  description: string | null;
}

export interface Asset {
  id: number;
  name: string;
  description: string | null;
  acquisition_config: Record<string, unknown>;
  labels: Record<string, unknown> | null;
  agency_id: number;
  created_at: string;
  updated_at: string;
  workflow_count: number;
}

export interface AssetDetail extends Asset {
  agency_name: string;
  workflows: WorkflowSummary[];
}

// Workflow types
export interface WorkflowSummary {
  id: number;
  name: string;
}

export interface Workflow {
  id: number;
  name: string;
  steps: WorkflowStep[];
  asset_id: number;
  created_at: string;
  updated_at: string;
}

export interface WorkflowDetail extends Workflow {
  asset_name: string;
  agency_name: string;
}

export interface WorkflowStep {
  name: string;
  type: string;
  config?: Record<string, unknown>;
}

// Metrics types
export interface MetricsSummary {
  overall_dis: number;
  avg_quality: number;
  avg_efficiency: number;
  avg_execution_success: number;
  workflow_count: number;
  latest_update: string | null;
}

export interface WorkflowDISHistory {
  id: number;
  workflow_name: string;
  dis_score: number;
  quality_score: number;
  efficiency_score: number;
  execution_success_score: number;
  recorded_at: string;
}

export interface OverallDISHistory {
  id: number;
  overall_dis: number;
  avg_quality: number;
  avg_efficiency: number;
  avg_execution_success: number;
  workflow_count: number;
  recorded_at: string;
}

export interface MetricsHistory {
  overall_history: OverallDISHistory[];
  workflow_history: WorkflowDISHistory[];
}

// Stats entry for min/avg/max
export interface QualityStatEntry {
  value: number;
  workflow: string;
}

export interface TimeStatEntry {
  value_ms: number;
  workflow: string;
}

// File type stats
export interface FileTypeStats {
  avg_time_ms: number;
  avg_quality: number;
  count: number;
}

// Document type distribution
export interface DocumentTypeEntry {
  type: string;
  count: number;
  percentage: number;
}

// Dashboard Stats types
export interface DashboardStats {
  summary: {
    overall_dis: number;
    dis_trend: number;
    total_assets: number;
    total_agencies: number;
    workflows_executed: number;
    workflows_successful: number;
    success_rate: number;
    eligible_coverage: number;
    avg_quality: number;
    avg_efficiency: number;
    avg_execution_success: number;
  };
  step_coverage: {
    acquisition: { count: number; total: number; percentage: number };
    parse: { count: number; total: number; percentage: number };
    enrichment: { count: number; total: number; percentage: number };
  };
  quality_stats: {
    min: QualityStatEntry;
    avg: QualityStatEntry;
    max: QualityStatEntry;
  };
  time_stats: {
    min: TimeStatEntry;
    avg: TimeStatEntry;
    max: TimeStatEntry;
  };
  file_type_breakdown: {
    CSV: FileTypeStats;
    JSON: FileTypeStats;
    PDF: FileTypeStats;
    XLSX: FileTypeStats;
  };
  parser_metrics: {
    total_tables: number;
    total_sections: number;
    total_tokens: number;
    avg_parse_quality: number;
    estimated_cost: number;
    document_type_distribution: DocumentTypeEntry[];
  };
  enrichment_metrics: {
    total_entities: number;
    total_topics: number;
    estimated_cost: number;
  };
  workflow_dis: WorkflowDIS[];
}

export interface WorkflowDIS {
  name: string;
  dis_score: number;
  dis_trend: number;
  quality_score: number;
  quality_trend: number;
  efficiency_score: number;
  efficiency_trend: number;
  execution_success_score: number;
  execution_trend: number;
}
