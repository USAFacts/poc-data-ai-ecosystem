import { useState } from 'react';

interface ToolParam {
  name: string;
  type: string;
  required?: boolean;
  default?: string | number;
}

interface McpTool {
  name: string;
  category: string;
  description: string;
  params: ToolParam[];
}

const MCP_TOOLS: McpTool[] = [
  { name: 'search_documents', category: 'Weaviate', description: 'BM25 keyword search across 37 government documents', params: [{ name: 'query', type: 'string', required: true }, { name: 'limit', type: 'number', default: 10 }] },
  { name: 'search_chunks', category: 'Weaviate', description: 'Search 2000+ text chunks with optional agency filter', params: [{ name: 'query', type: 'string', required: true }, { name: 'limit', type: 'number', default: 10 }, { name: 'agency', type: 'string' }] },
  { name: 'get_weaviate_stats', category: 'Weaviate', description: 'Collection schemas, object counts, and connection info', params: [] },
  { name: 'find_entity', category: 'Neo4j', description: 'Look up an entity — its type, documents, and related entities', params: [{ name: 'name', type: 'string', required: true }] },
  { name: 'query_graph', category: 'Neo4j', description: 'Run a read-only Cypher query against the knowledge graph', params: [{ name: 'cypher', type: 'string', required: true }] },
  { name: 'get_graph_overview', category: 'Neo4j', description: 'Node/relationship counts and top entities', params: [] },
  { name: 'find_path_between_entities', category: 'Neo4j', description: 'Shortest path between two entities', params: [{ name: 'from_entity', type: 'string', required: true }, { name: 'to_entity', type: 'string', required: true }, { name: 'max_depth', type: 'number', default: 4 }] },
  { name: 'list_documents_in_storage', category: 'MinIO', description: 'Browse files in landing/parsed/enrichment zones', params: [{ name: 'zone', type: 'string', default: 'enrichment-zone' }, { name: 'agency', type: 'string' }, { name: 'limit', type: 'number', default: 50 }] },
  { name: 'read_document', category: 'MinIO', description: 'Read full document content (JSON, text)', params: [{ name: 'path', type: 'string', required: true }] },
  { name: 'list_assets', category: 'PostgreSQL', description: 'All data assets with agencies and descriptions', params: [] },
  { name: 'list_agencies', category: 'PostgreSQL', description: 'Government agencies with asset counts', params: [] },
  { name: 'get_pipeline_stats', category: 'PostgreSQL', description: 'Overall pipeline statistics', params: [] },
  { name: 'search_gov_websites', category: 'Web Search', description: 'Live search on .gov websites for current information', params: [{ name: 'query', type: 'string', required: true }, { name: 'limit', type: 'number', default: 5 }] },
  { name: 'search_usafacts', category: 'Web Search', description: 'Search USAFacts.org for curated US government data and statistics', params: [{ name: 'query', type: 'string', required: true }, { name: 'limit', type: 'number', default: 5 }] },
];

const CATEGORY_COLORS: Record<string, string> = {
  Weaviate: 'bg-blue-100 text-blue-700 border-blue-200',
  Neo4j: 'bg-green-100 text-green-700 border-green-200',
  MinIO: 'bg-amber-100 text-amber-700 border-amber-200',
  PostgreSQL: 'bg-purple-100 text-purple-700 border-purple-200',
  'Web Search': 'bg-orange-100 text-orange-700 border-orange-200',
};

const CATEGORY_DOT_COLORS: Record<string, string> = {
  Weaviate: 'bg-blue-500',
  Neo4j: 'bg-green-500',
  MinIO: 'bg-amber-500',
  PostgreSQL: 'bg-purple-500',
  'Web Search': 'bg-orange-500',
};

const MCP_RESOURCES = [
  { uri: 'data://schema/overview', description: 'Overview of all data sources and their schemas' },
  { uri: 'data://agencies', description: 'List of government agencies with asset counts' },
];

export default function McpInspectorPage() {
  const [selectedTool, setSelectedTool] = useState<string | null>(null);
  const [categoryFilter, setCategoryFilter] = useState<string>('');

  const categories = [...new Set(MCP_TOOLS.map(t => t.category))];
  const filteredTools = categoryFilter
    ? MCP_TOOLS.filter(t => t.category === categoryFilter)
    : MCP_TOOLS;

  const selected = MCP_TOOLS.find(t => t.name === selectedTool);

  return (
    <div>
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-slate-900">MCP Inspector</h1>
        <p className="text-sm text-slate-500 mt-1">
          Model Context Protocol server for Claude Desktop — direct access to all data sources
        </p>
      </div>

      {/* Connection status */}
      <div className="bg-white rounded-lg border border-slate-200 p-5 mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-base font-semibold text-slate-900">gov-data MCP Server</h2>
            <p className="text-xs text-slate-500 mt-1">
              <code className="bg-slate-100 px-1.5 py-0.5 rounded text-[11px]">uv run mcp-data-server</code>
              <span className="mx-2">|</span>
              Transport: stdio
              <span className="mx-2">|</span>
              14 tools, 2 resources
            </p>
          </div>
          <div className="text-right">
            <p className="text-xs text-slate-500">Claude Desktop Config</p>
            <code className="text-[10px] text-slate-400 font-mono">~/Library/Application Support/Claude/claude_desktop_config.json</code>
          </div>
        </div>

        {/* Data source connections */}
        <div className="grid grid-cols-5 gap-3 mt-4">
          {[
            { name: 'Weaviate', port: '8085', desc: 'Vector search', color: 'blue' },
            { name: 'Neo4j', port: '7687', desc: 'Knowledge graph', color: 'green' },
            { name: 'MinIO', port: '9000', desc: 'Object storage', color: 'amber' },
            { name: 'PostgreSQL', port: '5432', desc: 'Pipeline metadata', color: 'purple' },
            { name: 'Web Search', port: 'API', desc: '.gov + USAFacts.org', color: 'orange' },
          ].map(src => (
            <div key={src.name} className={`rounded-lg border p-3 bg-${src.color}-50 border-${src.color}-200`}>
              <div className="flex items-center gap-2">
                <span className={`w-2 h-2 rounded-full ${CATEGORY_DOT_COLORS[src.name]}`} />
                <span className="text-xs font-medium text-slate-700">{src.name}</span>
              </div>
              <p className="text-[10px] text-slate-500 mt-1">localhost:{src.port}</p>
              <p className="text-[10px] text-slate-400">{src.desc}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Tools */}
      <div className="bg-white rounded-lg border border-slate-200 p-5 mb-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-base font-semibold text-slate-900">Tools ({filteredTools.length})</h2>
          <div className="flex gap-1.5">
            <button
              onClick={() => setCategoryFilter('')}
              className={`px-2.5 py-1 rounded text-[11px] font-medium transition-colors ${
                !categoryFilter ? 'bg-slate-800 text-white' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
              }`}
            >
              All
            </button>
            {categories.map(cat => (
              <button
                key={cat}
                onClick={() => setCategoryFilter(categoryFilter === cat ? '' : cat)}
                className={`px-2.5 py-1 rounded text-[11px] font-medium transition-colors ${
                  categoryFilter === cat ? 'bg-slate-800 text-white' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                }`}
              >
                {cat}
              </button>
            ))}
          </div>
        </div>

        <div className="space-y-2">
          {filteredTools.map(tool => (
            <div
              key={tool.name}
              onClick={() => setSelectedTool(selectedTool === tool.name ? null : tool.name)}
              className={`rounded-lg border p-3 cursor-pointer transition-colors ${
                selectedTool === tool.name ? 'border-blue-300 bg-blue-50' : 'border-slate-200 hover:bg-slate-50'
              }`}
            >
              <div className="flex items-center gap-3">
                <span className={`w-2 h-2 rounded-full ${CATEGORY_DOT_COLORS[tool.category]}`} />
                <code className="text-xs font-mono font-semibold text-slate-800">{tool.name}</code>
                <span className={`text-[10px] px-1.5 py-0.5 rounded border font-medium ${CATEGORY_COLORS[tool.category]}`}>
                  {tool.category}
                </span>
                <span className="text-xs text-slate-500 ml-auto">{tool.description}</span>
              </div>

              {selectedTool === tool.name && selected && selected.params.length > 0 && (
                <div className="mt-3 ml-5 border-t border-slate-200 pt-3">
                  <p className="text-[10px] font-medium text-slate-500 uppercase tracking-wide mb-2">Parameters</p>
                  <div className="space-y-1.5">
                    {selected.params.map(p => (
                      <div key={p.name} className="flex items-center gap-3 text-xs">
                        <code className="font-mono text-slate-700 w-28">{p.name}</code>
                        <span className="text-slate-400 w-16">{p.type}</span>
                        {p.required && <span className="text-red-400 text-[10px]">required</span>}
                        {'default' in p && <span className="text-slate-400 text-[10px]">default: {String(p.default)}</span>}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Resources */}
      <div className="bg-white rounded-lg border border-slate-200 p-5 mb-6">
        <h2 className="text-base font-semibold text-slate-900 mb-4">Resources</h2>
        <div className="space-y-2">
          {MCP_RESOURCES.map(res => (
            <div key={res.uri} className="flex items-center gap-3 p-3 rounded-lg border border-slate-200">
              <code className="text-xs font-mono text-blue-600">{res.uri}</code>
              <span className="text-xs text-slate-500">{res.description}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Usage instructions */}
      <div className="bg-white rounded-lg border border-slate-200 p-5">
        <h2 className="text-base font-semibold text-slate-900 mb-3">Usage</h2>
        <div className="space-y-4 text-sm text-slate-600">
          <div>
            <h3 className="font-medium text-slate-800 mb-1">Claude Desktop</h3>
            <p className="text-xs text-slate-500">
              Restart Claude Desktop after configuration. The tools appear under the hammer icon.
              Ask Claude to "search government data for H-1B statistics" and it will use the tools autonomously.
            </p>
          </div>
          <div>
            <h3 className="font-medium text-slate-800 mb-1">MCP Inspector (interactive testing)</h3>
            <p className="text-xs text-slate-500 mb-2">Run this command to open the web-based inspector:</p>
            <code className="block bg-slate-800 text-green-400 rounded-lg px-4 py-2.5 text-xs font-mono">
              cd mcp-data-server && uv run mcp dev src/mcp_data_server/server.py
            </code>
            <p className="text-xs text-slate-400 mt-1.5">Opens at http://localhost:6274 — test each tool interactively</p>
          </div>
          <div>
            <h3 className="font-medium text-slate-800 mb-1">Claude Code</h3>
            <p className="text-xs text-slate-500 mb-2">Add to your project's <code className="bg-slate-100 px-1 rounded">.mcp.json</code>:</p>
            <pre className="bg-slate-800 text-green-400 rounded-lg px-4 py-2.5 text-[11px] font-mono overflow-x-auto">{`{
  "mcpServers": {
    "gov-data": {
      "command": "uv",
      "args": ["--directory", "./mcp-data-server", "run", "mcp-data-server"]
    }
  }
}`}</pre>
          </div>
        </div>
      </div>
    </div>
  );
}
