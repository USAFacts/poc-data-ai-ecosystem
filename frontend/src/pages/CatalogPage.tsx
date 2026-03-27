import { useState } from 'react';
import api from '../api/client';

interface CatalogResult {
  source: 'local' | 'gov' | 'usafacts';
  title: string;
  agency: string;
  asset: string | null;
  doc_id: string | null;
  snippet: string;
  section: string | null;
  page_number: number | null;
  relevance_score: number;
  url: string | null;
  download_path?: string | null;
}

interface CatalogResponse {
  query: string;
  mode: string;
  total_results: number;
  elapsed_ms: number;
  results: CatalogResult[];
}

const SOURCE_STYLES: Record<string, { label: string; badge: string; dot: string }> = {
  local: { label: 'Local Document', badge: 'bg-blue-100 text-blue-700', dot: 'bg-blue-500' },
  gov: { label: '.gov Website', badge: 'bg-green-100 text-green-700', dot: 'bg-green-500' },
  usafacts: { label: 'USAFacts.org', badge: 'bg-amber-100 text-amber-700', dot: 'bg-amber-500' },
};

const LIMIT_OPTIONS = [5, 10, 15, 20];

export default function CatalogPage() {
  const [query, setQuery] = useState('');
  const [limit, setLimit] = useState(10);
  const [mode, setMode] = useState('vgw');
  const [results, setResults] = useState<CatalogResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;

    setIsLoading(true);
    setError(null);

    try {
      const response = await api.post<CatalogResponse>(`/search/catalog?limit=${limit}`, {
        query: query.trim(),
        mode,
      });
      setResults(response.data);
    } catch (err) {
      setError('Search failed. Make sure the backend is running.');
      setResults(null);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div>
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-slate-900">Catalog</h1>
        <p className="text-sm text-slate-500 mt-1">
          Search across local documents, .gov websites, and USAFacts.org — no AI answers, just sources
        </p>
      </div>

      {/* Search bar */}
      <form onSubmit={handleSearch} className="bg-white rounded-lg border border-slate-200 p-4 mb-6">
        <div className="flex gap-3">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search documents, .gov sites, and USAFacts.org..."
            className="flex-1 bg-slate-50 rounded-lg px-4 py-2.5 text-sm text-slate-900 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:bg-white border border-slate-200"
            disabled={isLoading}
          />
          <button
            type="submit"
            disabled={isLoading || !query.trim()}
            className="px-5 py-2.5 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {isLoading ? 'Searching...' : 'Search'}
          </button>
        </div>

        {/* Controls row */}
        <div className="flex items-center gap-6 mt-3">
          {/* Result count */}
          <div className="flex items-center gap-2">
            <span className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Results</span>
            {LIMIT_OPTIONS.map((n) => (
              <button
                key={n}
                type="button"
                onClick={() => setLimit(n)}
                className={`px-2 py-0.5 rounded text-xs font-medium transition-colors ${
                  limit === n
                    ? 'bg-blue-100 text-blue-700 ring-1 ring-blue-300'
                    : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                }`}
              >
                {n}
              </button>
            ))}
          </div>

          {/* Source mode */}
          <div className="flex items-center gap-2">
            <span className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Sources</span>
            {[
              { id: 'v', label: 'Local only' },
              { id: 'vg', label: 'Local + Graph' },
              { id: 'vgw', label: 'All sources' },
            ].map((m) => (
              <button
                key={m.id}
                type="button"
                onClick={() => setMode(m.id)}
                className={`px-2.5 py-0.5 rounded text-xs font-medium transition-colors ${
                  mode === m.id
                    ? 'bg-blue-100 text-blue-700 ring-1 ring-blue-300'
                    : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                }`}
              >
                {m.label}
              </button>
            ))}
          </div>
        </div>
      </form>

      {/* Error */}
      {error && (
        <div className="bg-red-50 text-red-700 p-4 rounded-lg border border-red-200 mb-6 text-sm">
          {error}
        </div>
      )}

      {/* Results */}
      {results && (
        <>
          {/* Summary bar */}
          <div className="flex items-center justify-between mb-4">
            <p className="text-sm text-slate-600">
              <span className="font-medium">{results.total_results}</span> results for "<span className="font-medium">{results.query}</span>"
            </p>
            <p className="text-xs text-slate-400">
              {results.elapsed_ms < 1000 ? `${results.elapsed_ms}ms` : `${(results.elapsed_ms / 1000).toFixed(1)}s`}
              <span className="mx-1.5">|</span>
              Mode: {results.mode.toUpperCase()}
            </p>
          </div>

          {results.results.length === 0 ? (
            <div className="bg-white rounded-lg border border-slate-200 p-12 text-center">
              <p className="text-slate-500">No results found. Try different keywords or broaden your sources.</p>
            </div>
          ) : (
            <div className="space-y-3">
              {results.results.map((result, i) => (
                <ResultCard key={i} result={result} rank={i + 1} />
              ))}
            </div>
          )}
        </>
      )}

      {/* Empty state */}
      {!results && !isLoading && !error && (
        <div className="bg-white rounded-lg border border-slate-200 p-16 text-center">
          <svg className="w-12 h-12 text-slate-300 mx-auto mb-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
            <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
          <p className="text-slate-500 text-sm">Enter a search query to find documents across all sources</p>
          <div className="flex flex-wrap gap-2 justify-center mt-4">
            {['H-1B visa statistics', 'USCIS backlog', 'immigration trends', 'crime rate US', 'DACA recipients'].map((s) => (
              <button
                key={s}
                onClick={() => { setQuery(s); }}
                className="px-3 py-1.5 bg-slate-100 text-slate-600 rounded-full text-xs hover:bg-slate-200 transition-colors"
              >
                {s}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function ResultCard({ result, rank }: { result: CatalogResult; rank: number }) {
  const style = SOURCE_STYLES[result.source] || SOURCE_STYLES.local;

  return (
    <div className="bg-white rounded-lg border border-slate-200 p-4 hover:shadow-sm transition-shadow">
      <div className="flex items-start gap-3">
        {/* Rank */}
        <span className="text-lg font-bold text-slate-200 w-6 text-right flex-shrink-0 mt-0.5">
          {rank}
        </span>

        <div className="flex-1 min-w-0">
          {/* Title + badges */}
          <div className="flex items-start gap-2 flex-wrap">
            <h3 className="text-sm font-semibold text-slate-900 leading-tight">
              {result.url ? (
                <a href={result.url} target="_blank" rel="noopener noreferrer" className="hover:text-blue-600 transition-colors">
                  {result.title || 'Untitled'}
                  <svg className="w-3 h-3 inline ml-1 opacity-40" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                    <polyline points="15 3 21 3 21 9" /><line x1="10" y1="14" x2="21" y2="3" />
                  </svg>
                </a>
              ) : (
                result.title || result.asset || 'Untitled'
              )}
            </h3>
            <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-medium ${style.badge}`}>
              <span className={`w-1.5 h-1.5 rounded-full ${style.dot}`} />
              {style.label}
            </span>
          </div>

          {/* Metadata row */}
          <div className="flex items-center gap-3 mt-1.5 text-xs text-slate-400 flex-wrap">
            {result.agency && (
              <span className="flex items-center gap-1">
                <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M6 22V4a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v18Z" /></svg>
                {result.agency}
              </span>
            )}
            {result.asset && (
              <span className="text-slate-400">{result.asset}</span>
            )}
            {result.section && (
              <span>{result.section}</span>
            )}
            {result.page_number && (
              <span>Page {result.page_number}</span>
            )}
            {result.download_path && (
              <a
                href={`/api/data/objects/download?path=${encodeURIComponent(result.download_path)}`}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-blue-500 hover:text-blue-700"
              >
                <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" y1="15" x2="12" y2="3" />
                </svg>
                Download
              </a>
            )}
            <span className="ml-auto font-medium text-slate-500">
              {(result.relevance_score * 100).toFixed(0)}%
            </span>
          </div>
          {/* Source URL for local docs */}
          {result.source === 'local' && result.url && (
            <div className="mt-1">
              <a href={result.url} target="_blank" rel="noopener noreferrer" className="text-[11px] text-blue-400 hover:text-blue-600 truncate block">
                {result.url}
              </a>
            </div>
          )}

          {/* Snippet */}
          {result.snippet && (
            <p className="mt-2 text-xs text-slate-600 leading-relaxed line-clamp-3">
              {result.snippet}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
