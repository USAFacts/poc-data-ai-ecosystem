import { useEffect, useRef, useState } from 'react';
import { Network } from 'vis-network/standalone';

// ---------------------------------------------------------------------------
// Architecture data — nodes and edges representing the full pipeline
// ---------------------------------------------------------------------------

interface ArchNode {
  id: string;
  label: string;
  group: string;
  description: string;
  level: number;
}

interface ArchEdge {
  from: string;
  to: string;
  label: string;
  dashes?: boolean;
  color?: string;
}

const NODES: ArchNode[] = [
  // --- Data Sources (Level 0) ---
  // These are the ultimate origins of data. Both serve the ingestion pipeline
  // (batch) AND are searchable at query time via Firecrawl (live).
  { id: 'gov_sites', label: '.gov\nWebsites', group: 'source', description: 'Federal government websites (USCIS, DHS, OHSS, Census, FBI, etc.). Serve two roles: batch ingestion of documents AND live web search at query time via Firecrawl.', level: 0 },
  { id: 'usafacts', label: 'USAFacts.org', group: 'source', description: 'Curated US government data and statistics. Accessed at query time via Firecrawl web search (not batch-ingested).', level: 0 },

  // --- Ingestion Pipeline (Level 1) ---
  // Batch processing: downloads, parses, enriches, and indexes documents.
  { id: 'acquisition', label: 'Acquisition', group: 'pipeline', description: 'Downloads documents (PDF, XLSX, CSV, JSON) from .gov source URLs via HTTP/API. Batch process, not real-time.', level: 1 },
  { id: 'parse', label: 'Parse', group: 'pipeline', description: 'Extracts text, tables, and structure. Uses Claude Vision for PDFs and PyMuPDF for text extraction.', level: 1 },
  { id: 'chunk', label: 'Chunk', group: 'pipeline', description: 'Splits documents into document-level, section-level, and table-level chunks for granular retrieval.', level: 1 },
  { id: 'enrich', label: 'Enrich', group: 'pipeline', description: 'LLM enrichment via Claude: entity extraction, topic classification, summaries, and 384-dim embeddings (all-MiniLM-L6-v2).', level: 1 },
  { id: 'sync', label: 'Sync', group: 'pipeline', description: 'Pushes enriched data to Weaviate (vectors + chunks) and Neo4j (knowledge graph). Deduplicates by asset key.', level: 1 },

  // --- Storage Layer (Level 2) ---
  { id: 'minio', label: 'MinIO\nObject Store', group: 'storage', description: 'Stores raw documents (landing-zone), parsed content (parsed-zone), and enriched documents (enrichment-zone).', level: 2 },
  { id: 'postgres', label: 'PostgreSQL', group: 'storage', description: 'Relational database for pipeline metadata, workflow state, experiment results, and asset management.', level: 2 },
  { id: 'weaviate', label: 'Weaviate\nVector DB', group: 'storage', description: 'Hybrid search engine (BM25 + vector). GovDocument (37 docs) + GovChunk (2K+ chunks) with 384-dim embeddings.', level: 2 },
  { id: 'neo4j', label: 'Neo4j\nGraph DB', group: 'storage', description: 'Knowledge graph with Document, Entity, Agency, TimePeriod nodes connected by MENTIONS, PUBLISHED_BY, COVERS_PERIOD, RELATED_TO, BELONGS_TO relationships.', level: 2 },

  // --- Retrieval Layer (Level 3) ---
  // Query-time processing: decomposes the question, searches multiple stores,
  // optionally fetches live web results, reranks, and applies trust weights.
  { id: 'query_decomp', label: 'Query\nDecomposition', group: 'retrieval', description: 'Claude analyzes the user query to extract entities, keywords, and intent. Matches entities against canonical names in Neo4j.', level: 3 },
  { id: 'hybrid_search', label: 'Hybrid Search\n(BM25 + Vector)', group: 'retrieval', description: 'Weaviate hybrid search combining keyword matching (BM25) and semantic similarity (cosine on embeddings), alpha=0.5.', level: 3 },
  { id: 'graph_expand', label: 'Graph\nExpansion', group: 'retrieval', description: 'Neo4j traversal: finds related entities at depth 2 and documents that mention the same entities. Enriches results with cross-document context.', level: 3 },
  { id: 'web_search', label: 'Live Web Search\n(Firecrawl)', group: 'retrieval', description: 'Real-time search via Firecrawl API against .gov websites and USAFacts.org. Provides current information not yet ingested. Results deduplicated against local documents.', level: 3 },
  { id: 'reranker', label: 'Cross-Encoder\nReranker', group: 'retrieval', description: 'ms-marco-MiniLM-L-6-v2 cross-encoder reranker. Blends reranker scores (60%) with original Weaviate hybrid scores (40%).', level: 3 },
  { id: 'trust_weight', label: 'Trust\nWeighting', group: 'retrieval', description: 'Applies source trust hierarchy: Local=1.0, USAFacts=0.85, .gov=0.70, Other=0.60. Adds recency bonus for time-sensitive queries.', level: 3 },

  // --- Synthesis Layer (Level 4) ---
  { id: 'claude', label: 'Claude\nSonnet 4.5', group: 'llm', description: 'Answer generation with full document context (sections, tables, web snippets). Generates inline citations and optional chart specifications.', level: 4 },
  { id: 'metrics', label: 'Answer Quality\nMetrics', group: 'llm', description: 'STS (source traceability), NVS (numerical verification), HDS (hallucination detection), CSCS (cross-store consistency).', level: 4 },

  // --- Presentation Layer (Level 5) ---
  { id: 'qa_ui', label: 'Q&A\nInterface', group: 'frontend', description: 'Chat-style Q&A with retrieval mode selector (V, V+G, V+G+W), markdown answers, interactive charts, source citations, and quality metrics.', level: 5 },
  { id: 'dashboard', label: 'Executive\nDashboard', group: 'frontend', description: 'DIS score, pipeline step coverage, quality metrics, agency/asset overview, Weaviate and Neo4j status.', level: 5 },
  { id: 'experiments', label: 'Experiment\nTracker', group: 'frontend', description: 'Ablation testing across 4 retrieval modes (V/VG/VW/VGW) with 350 stratified questions, per-mode metrics, and statistical comparison.', level: 5 },
];

const EDGES: ArchEdge[] = [
  // === BATCH PATH: Sources → Ingestion → Storage ===
  // .gov sites are the source for batch document ingestion
  { from: 'gov_sites', to: 'acquisition', label: 'batch download' },

  // Ingestion pipeline flow (sequential)
  { from: 'acquisition', to: 'parse', label: '' },
  { from: 'parse', to: 'chunk', label: '' },
  { from: 'chunk', to: 'enrich', label: '' },
  { from: 'enrich', to: 'sync', label: '' },

  // Pipeline writes to storage at each stage
  { from: 'acquisition', to: 'minio', label: 'landing-zone' },
  { from: 'parse', to: 'minio', label: 'parsed-zone' },
  { from: 'enrich', to: 'minio', label: 'enrichment-zone' },
  { from: 'sync', to: 'weaviate', label: 'vectors + chunks' },
  { from: 'sync', to: 'neo4j', label: 'graph nodes' },
  { from: 'sync', to: 'postgres', label: 'metadata' },

  // === QUERY-TIME PATH: Retrieval → Synthesis → Presentation ===
  // Query decomposition routes to different retrieval strategies
  { from: 'query_decomp', to: 'hybrid_search', label: 'keywords + embedding' },
  { from: 'query_decomp', to: 'graph_expand', label: 'entities', dashes: true },
  { from: 'query_decomp', to: 'web_search', label: 'query text', dashes: true },

  // Each retrieval strategy accesses its data source
  { from: 'hybrid_search', to: 'weaviate', label: '' },
  { from: 'graph_expand', to: 'neo4j', label: '' },

  // Web search reaches the SAME sources at query time (not a separate source)
  { from: 'web_search', to: 'gov_sites', label: 'live search', dashes: true },
  { from: 'web_search', to: 'usafacts', label: 'live search', dashes: true },

  // All retrieval results flow through reranking and trust weighting
  { from: 'hybrid_search', to: 'reranker', label: 'candidates' },
  { from: 'reranker', to: 'trust_weight', label: 'reranked' },
  { from: 'web_search', to: 'trust_weight', label: 'web results', dashes: true },
  { from: 'graph_expand', to: 'trust_weight', label: 'graph context', dashes: true },

  // Synthesis: Claude receives ranked sources + full document content
  { from: 'trust_weight', to: 'claude', label: 'ranked sources' },
  { from: 'minio', to: 'claude', label: 'full doc context' },
  { from: 'claude', to: 'metrics', label: 'answer + citations' },

  // Presentation
  { from: 'metrics', to: 'qa_ui', label: 'answer + charts + metrics' },
  { from: 'postgres', to: 'dashboard', label: 'stats' },
  { from: 'postgres', to: 'experiments', label: 'results' },
];

// ---------------------------------------------------------------------------
// Group colors and styles
// ---------------------------------------------------------------------------

const GROUP_CONFIG: Record<string, { color: string; bg: string; shape: string; borderColor: string }> = {
  source:    { color: '#F59E0B', bg: '#FEF3C7', shape: 'box', borderColor: '#D97706' },
  pipeline:  { color: '#3B82F6', bg: '#DBEAFE', shape: 'box', borderColor: '#2563EB' },
  storage:   { color: '#10B981', bg: '#D1FAE5', shape: 'database', borderColor: '#059669' },
  retrieval: { color: '#8B5CF6', bg: '#EDE9FE', shape: 'box', borderColor: '#7C3AED' },
  llm:       { color: '#EC4899', bg: '#FCE7F3', shape: 'diamond', borderColor: '#DB2777' },
  frontend:  { color: '#06B6D4', bg: '#CFFAFE', shape: 'box', borderColor: '#0891B2' },
};

const GROUP_LABELS: Record<string, string> = {
  source: 'Data Sources',
  pipeline: 'Ingestion Pipeline',
  storage: 'Storage Layer',
  retrieval: 'Retrieval & Ranking',
  llm: 'Synthesis (LLM)',
  frontend: 'Presentation',
};

// ---------------------------------------------------------------------------
// Retrieval path definitions
// ---------------------------------------------------------------------------

interface RetrievalPath {
  id: string;
  label: string;
  short: string;
  color: string;
  description: string;
  nodes: string[];
}

const RETRIEVAL_PATHS: RetrievalPath[] = [
  {
    id: 'v', label: 'Weaviate Only', short: 'V', color: '#3B82F6',
    description: 'Fastest path. Query decomposition → Weaviate hybrid search (BM25 + vector) → cross-encoder reranker → Claude synthesis.',
    nodes: ['query_decomp', 'hybrid_search', 'weaviate', 'reranker', 'trust_weight', 'minio', 'claude', 'metrics', 'qa_ui'],
  },
  {
    id: 'vg', label: 'Weaviate + Graph', short: 'V+G', color: '#10B981',
    description: 'Adds Neo4j graph expansion for entity-aware retrieval, related-document discovery, and cross-document context enrichment.',
    nodes: ['query_decomp', 'hybrid_search', 'weaviate', 'graph_expand', 'neo4j', 'reranker', 'trust_weight', 'minio', 'claude', 'metrics', 'qa_ui'],
  },
  {
    id: 'vw', label: 'Weaviate + Web', short: 'V+W', color: '#F59E0B',
    description: 'Adds live Firecrawl web search against .gov and USAFacts.org for current information not yet ingested. Trust-weighted merge with local results.',
    nodes: ['query_decomp', 'hybrid_search', 'weaviate', 'web_search', 'gov_sites', 'usafacts', 'reranker', 'trust_weight', 'minio', 'claude', 'metrics', 'qa_ui'],
  },
  {
    id: 'vgw', label: 'All Sources', short: 'V+G+W', color: '#8B5CF6',
    description: 'Full pipeline: Weaviate hybrid search + Neo4j graph expansion + live web search. Most comprehensive but slowest. All sources trust-weighted and deduplicated.',
    nodes: ['query_decomp', 'hybrid_search', 'weaviate', 'graph_expand', 'neo4j', 'web_search', 'gov_sites', 'usafacts', 'reranker', 'trust_weight', 'minio', 'claude', 'metrics', 'qa_ui'],
  },
];

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function ArchitecturePage() {
  const containerRef = useRef<HTMLDivElement>(null);
  const networkRef = useRef<Network | null>(null);
  const [selectedNode, setSelectedNode] = useState<ArchNode | null>(null);
  const [highlightPath, setHighlightPath] = useState<string | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    const highlightedNodes = highlightPath
      ? new Set(RETRIEVAL_PATHS.find(p => p.id === highlightPath)?.nodes ?? [])
      : null;

    const nodes = NODES.map((n) => {
      const gc = GROUP_CONFIG[n.group];
      const dimmed = highlightedNodes && !highlightedNodes.has(n.id);
      return {
        id: n.id,
        label: n.label,
        level: n.level,
        shape: gc.shape,
        color: {
          background: dimmed ? '#F1F5F9' : gc.bg,
          border: dimmed ? '#CBD5E1' : gc.borderColor,
          highlight: { background: gc.bg, border: gc.color },
        },
        font: {
          color: dimmed ? '#94A3B8' : '#1E293B',
          size: 13,
          face: 'Inter, system-ui, sans-serif',
          multi: 'html' as const,
        },
        borderWidth: dimmed ? 1 : 2,
        margin: { top: 10, bottom: 10, left: 12, right: 12 },
        widthConstraint: { minimum: 90, maximum: 130 },
      };
    });

    const edges = EDGES.map((e, i) => {
      const dimmed = highlightedNodes && !(highlightedNodes.has(e.from) && highlightedNodes.has(e.to));
      return {
        id: `e-${i}`,
        from: e.from,
        to: e.to,
        label: e.label,
        arrows: { to: { enabled: true, scaleFactor: 0.6 } },
        dashes: e.dashes || false,
        color: {
          color: dimmed ? '#E2E8F0' : e.color || '#94A3B8',
          highlight: '#475569',
          opacity: dimmed ? 0.3 : 0.8,
        },
        font: { size: 9, color: dimmed ? '#CBD5E1' : '#64748B', face: 'Inter, system-ui, sans-serif', align: 'middle' as const },
        smooth: { enabled: true, type: 'cubicBezier', roundness: 0.3 },
        width: dimmed ? 1 : 1.5,
      };
    });

    const options = {
      layout: {
        hierarchical: {
          enabled: true,
          direction: 'UD',
          sortMethod: 'directed' as const,
          levelSeparation: 130,
          nodeSpacing: 160,
          treeSpacing: 200,
        },
      },
      physics: { enabled: false },
      interaction: {
        hover: true,
        tooltipDelay: 100,
        zoomView: true,
        dragView: true,
      },
      nodes: {
        borderWidthSelected: 3,
      },
    };

    const network = new Network(containerRef.current, { nodes, edges }, options);
    networkRef.current = network;

    network.on('click', (params) => {
      if (params.nodes.length > 0) {
        const nodeId = params.nodes[0] as string;
        const node = NODES.find(n => n.id === nodeId);
        setSelectedNode(node || null);
      } else {
        setSelectedNode(null);
      }
    });

    // Fit after stabilization
    setTimeout(() => network.fit({ animation: false }), 100);

    return () => {
      network.destroy();
      networkRef.current = null;
    };
  }, [highlightPath]);

  return (
    <div>
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-slate-900">System Architecture</h1>
        <p className="text-sm text-slate-500 mt-1">End-to-end data pipeline from ingestion to Q&A synthesis</p>
      </div>

      {/* Legend */}
      <div className="bg-white rounded-lg border border-slate-200 p-4 mb-4">
        <div className="flex flex-wrap gap-4">
          {Object.entries(GROUP_LABELS).map(([key, label]) => {
            const gc = GROUP_CONFIG[key];
            return (
              <div key={key} className="flex items-center gap-2">
                <span className="w-4 h-3 rounded-sm border" style={{ backgroundColor: gc.bg, borderColor: gc.borderColor }} />
                <span className="text-xs text-slate-600 font-medium">{label}</span>
              </div>
            );
          })}
          <div className="flex items-center gap-2 ml-2 pl-2 border-l border-slate-200">
            <span className="w-6 border-t border-slate-400" />
            <span className="text-xs text-slate-500">Required</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-6 border-t border-dashed border-slate-400" />
            <span className="text-xs text-slate-500">Optional path</span>
          </div>
        </div>
      </div>

      {/* Retrieval path selector */}
      <div className="bg-white rounded-lg border border-slate-200 p-4 mb-4">
        <p className="text-xs font-medium text-slate-500 uppercase tracking-wide mb-3">Highlight Retrieval Path</p>
        <div className="flex flex-wrap gap-2">
          <button
            onClick={() => setHighlightPath(null)}
            className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              highlightPath === null
                ? 'bg-slate-800 text-white'
                : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
            }`}
          >
            Show All
          </button>
          {RETRIEVAL_PATHS.map((path) => (
            <button
              key={path.id}
              onClick={() => setHighlightPath(highlightPath === path.id ? null : path.id)}
              className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors`}
              style={
                highlightPath === path.id
                  ? { backgroundColor: path.color, color: '#fff' }
                  : { backgroundColor: '#F1F5F9', color: '#475569' }
              }
            >
              {path.short} — {path.label}
            </button>
          ))}
        </div>
        {highlightPath && (
          <p className="text-xs text-slate-500 mt-2">
            {RETRIEVAL_PATHS.find(p => p.id === highlightPath)?.description}
          </p>
        )}
      </div>

      {/* Graph */}
      <div className="bg-white rounded-lg border border-slate-200 overflow-hidden">
        <div
          ref={containerRef}
          style={{ height: '700px', width: '100%' }}
        />
      </div>

      {/* Node detail panel */}
      {selectedNode && (
        <div className="bg-white rounded-lg border border-slate-200 p-5 mt-4">
          <div className="flex items-center gap-3 mb-2">
            <span
              className="w-4 h-4 rounded-sm border"
              style={{
                backgroundColor: GROUP_CONFIG[selectedNode.group]?.bg,
                borderColor: GROUP_CONFIG[selectedNode.group]?.borderColor,
              }}
            />
            <h3 className="text-base font-semibold text-slate-900">{selectedNode.label.replace('\n', ' ')}</h3>
            <span className="text-xs text-slate-400 uppercase">{GROUP_LABELS[selectedNode.group]}</span>
          </div>
          <p className="text-sm text-slate-600">{selectedNode.description}</p>
        </div>
      )}

      {/* Path details table */}
      <div className="bg-white rounded-lg border border-slate-200 p-5 mt-4">
        <h3 className="text-base font-semibold text-slate-900 mb-4">Retrieval Paths Comparison</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-[10px] text-slate-500 uppercase bg-slate-50">
                <th className="text-left py-3 px-4 font-medium">Path</th>
                <th className="text-left py-3 px-4 font-medium">Components</th>
                <th className="text-left py-3 px-4 font-medium">Best For</th>
                <th className="text-center py-3 px-4 font-medium">Speed</th>
                <th className="text-center py-3 px-4 font-medium">Coverage</th>
                <th className="text-center py-3 px-4 font-medium">Recency</th>
              </tr>
            </thead>
            <tbody className="text-slate-700">
              <tr className="border-t border-slate-100">
                <td className="py-3 px-4 font-medium"><span style={{ color: '#3B82F6' }}>V</span> — Weaviate Only</td>
                <td className="py-3 px-4 text-xs">Hybrid Search → Reranker → Claude</td>
                <td className="py-3 px-4 text-xs">Simple factual lookups, fast responses</td>
                <td className="py-3 px-4 text-center">{"⚡⚡⚡"}</td>
                <td className="py-3 px-4 text-center">{"⬤◯◯"}</td>
                <td className="py-3 px-4 text-center">{"◯◯◯"}</td>
              </tr>
              <tr className="border-t border-slate-100">
                <td className="py-3 px-4 font-medium"><span style={{ color: '#10B981' }}>V+G</span> — + Graph</td>
                <td className="py-3 px-4 text-xs">Hybrid Search + Neo4j Expansion → Reranker → Claude</td>
                <td className="py-3 px-4 text-xs">Entity-centric queries, cross-document connections</td>
                <td className="py-3 px-4 text-center">{"⚡⚡"}</td>
                <td className="py-3 px-4 text-center">{"⬤⬤◯"}</td>
                <td className="py-3 px-4 text-center">{"◯◯◯"}</td>
              </tr>
              <tr className="border-t border-slate-100">
                <td className="py-3 px-4 font-medium"><span style={{ color: '#F59E0B' }}>V+W</span> — + Web</td>
                <td className="py-3 px-4 text-xs">Hybrid Search + Firecrawl → Trust Weighting → Claude</td>
                <td className="py-3 px-4 text-xs">Current events, data not yet ingested</td>
                <td className="py-3 px-4 text-center">{"⚡"}</td>
                <td className="py-3 px-4 text-center">{"⬤◯◯"}</td>
                <td className="py-3 px-4 text-center">{"⬤⬤⬤"}</td>
              </tr>
              <tr className="border-t border-slate-100">
                <td className="py-3 px-4 font-medium"><span style={{ color: '#8B5CF6' }}>V+G+W</span> — All Sources</td>
                <td className="py-3 px-4 text-xs">Hybrid Search + Neo4j + Firecrawl → Trust Weighting → Claude</td>
                <td className="py-3 px-4 text-xs">Comprehensive answers, ablation testing</td>
                <td className="py-3 px-4 text-center">{"⚡"}</td>
                <td className="py-3 px-4 text-center">{"⬤⬤⬤"}</td>
                <td className="py-3 px-4 text-center">{"⬤⬤⬤"}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      {/* Trust hierarchy */}
      <div className="bg-white rounded-lg border border-slate-200 p-5 mt-4">
        <h3 className="text-base font-semibold text-slate-900 mb-4">Source Trust Hierarchy</h3>
        <div className="space-y-3">
          {[
            { label: 'Local (Weaviate + Neo4j)', weight: 1.0, color: '#3B82F6', desc: 'Ingested, parsed, enriched, and quality-checked documents. Highest fidelity.' },
            { label: 'USAFacts.org', weight: 0.85, color: '#10B981', desc: 'Curated government data with editorial quality. Recency bonus +0.15 for time-sensitive queries.' },
            { label: '.gov Web Search', weight: 0.70, color: '#F59E0B', desc: 'Authoritative government sources, broader scope. Recency bonus +0.25 for time-sensitive queries.' },
            { label: 'Other Web', weight: 0.60, color: '#94A3B8', desc: 'Fallback sources. Lowest trust tier.' },
          ].map((tier) => (
            <div key={tier.label} className="flex items-center gap-4">
              <div className="w-20 text-right">
                <span className="text-sm font-bold" style={{ color: tier.color }}>{tier.weight.toFixed(2)}</span>
              </div>
              <div className="flex-1">
                <div className="h-4 bg-slate-100 rounded-full overflow-hidden">
                  <div
                    className="h-4 rounded-full"
                    style={{ width: `${tier.weight * 100}%`, backgroundColor: tier.color }}
                  />
                </div>
              </div>
              <div className="w-72">
                <p className="text-sm font-medium text-slate-800">{tier.label}</p>
                <p className="text-[11px] text-slate-500">{tier.desc}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
