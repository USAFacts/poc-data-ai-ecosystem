import { useEffect, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Network } from 'vis-network/standalone';
import {
  fetchNeo4jStatus,
  fetchNeo4jSchema,
  type Neo4jSchema,
} from '../api/client';

export default function Neo4jPage() {
  const {
    data: status,
    isLoading: statusLoading,
    error: statusError,
    refetch: refetchStatus,
    isFetching: statusFetching,
  } = useQuery({
    queryKey: ['neo4jStatus'],
    queryFn: fetchNeo4jStatus,
    staleTime: 10000,
  });

  const {
    data: schema,
    isLoading: schemaLoading,
    refetch: refetchSchema,
  } = useQuery({
    queryKey: ['neo4jSchema'],
    queryFn: fetchNeo4jSchema,
    staleTime: 10000,
  });

  const isFetching = statusFetching;

  const handleRefresh = () => {
    refetchStatus();
    refetchSchema();
  };

  if (statusLoading || schemaLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (statusError) {
    return (
      <div className="bg-red-50 text-red-700 p-4 rounded-lg border border-red-200">
        Failed to load Neo4j status. Make sure the backend is running.
      </div>
    );
  }

  if (!status) return null;

  const totalNodes = Object.values(status.node_counts).reduce((a, b) => a + b, 0);
  const totalRels = Object.values(status.relationship_counts).reduce((a, b) => a + b, 0);
  const nodeTypeCount = Object.keys(status.node_counts).length;
  const relTypeCount = Object.keys(status.relationship_counts).length;
  const isConnected = status.status === 'connected';

  return (
    <div>
      {/* Header */}
      <div className="mb-6 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold text-slate-900">Neo4j Knowledge Graph</h1>
          <span
            className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${
              isConnected ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
            }`}
          >
            <span className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`}></span>
            {isConnected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
        <button
          onClick={handleRefresh}
          disabled={isFetching}
          className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 disabled:opacity-50"
        >
          <svg
            className={`w-4 h-4 ${isFetching ? 'animate-spin' : ''}`}
            viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
          >
            <path d="M21 2v6h-6" />
            <path d="M3 12a9 9 0 0 1 15-6.7L21 8" />
            <path d="M3 22v-6h6" />
            <path d="M21 12a9 9 0 0 1-15 6.7L3 16" />
          </svg>
          Refresh
        </button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
        <SummaryCard label="Total Nodes" value={totalNodes.toLocaleString()} />
        <SummaryCard label="Total Relationships" value={totalRels.toLocaleString()} />
        <SummaryCard label="Node Types" value={nodeTypeCount.toString()} />
        <SummaryCard label="Relationship Types" value={relTypeCount.toString()} />
      </div>

      {/* Node & Relationship Counts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        <NodeCountsTable nodeCounts={status.node_counts} />
        <RelCountsTable relCounts={status.relationship_counts} />
      </div>

      {/* Graph Schema Visualization */}
      {schema && <GraphSchemaVisualization schema={schema} />}

      {/* Top Entities */}
      {status.top_entities.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6 mb-6 mt-6">
          <h2 className="text-lg font-semibold text-slate-900 mb-4">Top Entities</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="text-left py-2 pr-4 font-medium text-slate-600">Name</th>
                  <th className="text-left py-2 pr-4 font-medium text-slate-600">Type</th>
                  <th className="text-right py-2 font-medium text-slate-600">Mentions</th>
                </tr>
              </thead>
              <tbody>
                {status.top_entities.map((entity, i) => (
                  <tr key={i} className="border-b border-slate-100 last:border-0">
                    <td className="py-2 pr-4 text-slate-800 font-medium">{entity.name}</td>
                    <td className="py-2 pr-4">
                      <span className="inline-flex px-2 py-0.5 rounded bg-green-50 text-green-700 text-xs font-medium">
                        {entity.type}
                      </span>
                    </td>
                    <td className="py-2 text-right text-slate-600">{entity.mention_count.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Top Agencies */}
      {status.top_agencies.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6 mb-6">
          <h2 className="text-lg font-semibold text-slate-900 mb-4">Top Agencies by Document Count</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="text-left py-2 pr-4 font-medium text-slate-600">Agency</th>
                  <th className="text-right py-2 font-medium text-slate-600">Documents</th>
                </tr>
              </thead>
              <tbody>
                {status.top_agencies.map((agency, i) => (
                  <tr key={i} className="border-b border-slate-100 last:border-0">
                    <td className="py-2 pr-4 text-slate-800 font-medium">{agency.name}</td>
                    <td className="py-2 text-right text-slate-600">{agency.document_count.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Constraints */}
      {status.constraints.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6 mb-6">
          <h2 className="text-lg font-semibold text-slate-900 mb-4">Constraints</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="text-left py-2 pr-4 font-medium text-slate-600">Name</th>
                  <th className="text-left py-2 pr-4 font-medium text-slate-600">Type</th>
                  <th className="text-left py-2 font-medium text-slate-600">Entity Type</th>
                </tr>
              </thead>
              <tbody>
                {status.constraints.map((c, i) => (
                  <tr key={i} className="border-b border-slate-100 last:border-0">
                    <td className="py-2 pr-4 font-mono text-xs text-slate-800">{c.name}</td>
                    <td className="py-2 pr-4 text-slate-600">{c.type}</td>
                    <td className="py-2 text-slate-600">{c.entityType}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Indexes */}
      {status.indexes.length > 0 && (
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6 mb-6">
          <h2 className="text-lg font-semibold text-slate-900 mb-4">Indexes</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="text-left py-2 pr-4 font-medium text-slate-600">Name</th>
                  <th className="text-left py-2 pr-4 font-medium text-slate-600">Type</th>
                  <th className="text-left py-2 pr-4 font-medium text-slate-600">Entity Type</th>
                  <th className="text-left py-2 font-medium text-slate-600">State</th>
                </tr>
              </thead>
              <tbody>
                {status.indexes.map((idx, i) => (
                  <tr key={i} className="border-b border-slate-100 last:border-0">
                    <td className="py-2 pr-4 font-mono text-xs text-slate-800">{idx.name}</td>
                    <td className="py-2 pr-4 text-slate-600">{idx.type}</td>
                    <td className="py-2 pr-4 text-slate-600">{idx.entityType}</td>
                    <td className="py-2">
                      <span
                        className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${
                          idx.state === 'ONLINE'
                            ? 'bg-green-50 text-green-700'
                            : 'bg-yellow-50 text-yellow-700'
                        }`}
                      >
                        {idx.state}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
      <p className="text-sm text-slate-500 mb-1">{label}</p>
      <p className="text-2xl font-bold text-slate-900">{value}</p>
    </div>
  );
}

const nodeColorMap: Record<string, string> = {
  Document: 'bg-blue-500',
  Entity: 'bg-green-500',
  Agency: 'bg-amber-500',
  TimePeriod: 'bg-purple-500',
};

function NodeCountsTable({ nodeCounts }: { nodeCounts: Record<string, number> }) {
  const entries = Object.entries(nodeCounts).sort((a, b) => b[1] - a[1]);

  return (
    <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
      <h2 className="text-lg font-semibold text-slate-900 mb-4">Node Counts by Label</h2>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200">
            <th className="text-left py-2 pr-4 font-medium text-slate-600">Label</th>
            <th className="text-right py-2 font-medium text-slate-600">Count</th>
          </tr>
        </thead>
        <tbody>
          {entries.map(([label, count]) => (
            <tr key={label} className="border-b border-slate-100 last:border-0">
              <td className="py-2 pr-4 text-slate-800">
                <span className="inline-flex items-center gap-2">
                  <span className={`w-2.5 h-2.5 rounded-full ${nodeColorMap[label] || 'bg-slate-400'}`}></span>
                  {label}
                </span>
              </td>
              <td className="py-2 text-right text-slate-600 font-medium">{count.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RelCountsTable({ relCounts }: { relCounts: Record<string, number> }) {
  const entries = Object.entries(relCounts).sort((a, b) => b[1] - a[1]);

  return (
    <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
      <h2 className="text-lg font-semibold text-slate-900 mb-4">Relationship Counts by Type</h2>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200">
            <th className="text-left py-2 pr-4 font-medium text-slate-600">Type</th>
            <th className="text-right py-2 font-medium text-slate-600">Count</th>
          </tr>
        </thead>
        <tbody>
          {entries.map(([type, count]) => (
            <tr key={type} className="border-b border-slate-100 last:border-0">
              <td className="py-2 pr-4 font-mono text-xs text-slate-800">{type}</td>
              <td className="py-2 text-right text-slate-600 font-medium">{count.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function GraphSchemaVisualization({ schema }: { schema: Neo4jSchema }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const networkRef = useRef<Network | null>(null);

  useEffect(() => {
    if (!containerRef.current || schema.nodes.length === 0) return;

    const maxCount = Math.max(...schema.nodes.map((n) => n.count), 1);

    const nodes = schema.nodes.map((node) => ({
      id: node.label,
      label: `${node.label}\n(${node.count.toLocaleString()})`,
      color: {
        background: node.color,
        border: node.color,
        highlight: { background: node.color, border: '#1E293B' },
      },
      size: 20 + (node.count / maxCount) * 40,
      font: { color: '#FFFFFF', size: 14, face: 'Inter, system-ui, sans-serif' },
      shape: 'dot',
    }));

    const edges = schema.edges.map((edge, i) => ({
      id: `edge-${i}`,
      from: edge.from,
      to: edge.to,
      label: `${edge.type}\n(${edge.count.toLocaleString()})`,
      arrows: 'to',
      color: { color: '#94A3B8', highlight: '#475569' },
      font: { size: 11, color: '#64748B', face: 'Inter, system-ui, sans-serif' },
      smooth: { enabled: true, type: 'curvedCW' as const, roundness: 0.2 },
    }));

    const options = {
      physics: {
        enabled: true,
        repulsion: {
          centralGravity: 0.1,
          springLength: 200,
          springConstant: 0.05,
          nodeDistance: 250,
        },
        solver: 'repulsion' as const,
        stabilization: { iterations: 150 },
      },
      interaction: {
        hover: true,
        tooltipDelay: 200,
      },
      layout: {
        improvedLayout: true,
      },
    };

    const network = new Network(containerRef.current, { nodes, edges }, options);
    networkRef.current = network;

    return () => {
      network.destroy();
      networkRef.current = null;
    };
  }, [schema]);

  return (
    <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
      <h2 className="text-lg font-semibold text-slate-900 mb-4">Graph Schema Overview</h2>
      {schema.nodes.length === 0 ? (
        <p className="text-sm text-slate-500">No schema data available.</p>
      ) : (
        <>
          <div className="flex gap-4 mb-4">
            {schema.nodes.map((node) => (
              <span key={node.label} className="inline-flex items-center gap-1.5 text-xs text-slate-600">
                <span className="w-3 h-3 rounded-full" style={{ backgroundColor: node.color }}></span>
                {node.label}
              </span>
            ))}
          </div>
          <div
            ref={containerRef}
            className="w-full border border-slate-200 rounded-lg"
            style={{ height: '500px' }}
          />
        </>
      )}
    </div>
  );
}
