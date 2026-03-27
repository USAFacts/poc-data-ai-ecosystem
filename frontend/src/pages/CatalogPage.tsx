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

const SOURCE_STYLES: Record<string, { label: string; bg: string; dot: string; border: string }> = {
  local: { label: 'Local Document', bg: 'bg-blue-50', dot: 'bg-blue-500', border: 'border-blue-200' },
  gov: { label: '.gov Website', bg: 'bg-green-50', dot: 'bg-green-500', border: 'border-green-200' },
  usafacts: { label: 'USAFacts.org', bg: 'bg-amber-50', dot: 'bg-amber-500', border: 'border-amber-200' },
};

const LIMIT_OPTIONS = [5, 10, 15, 20];

const SUGGESTIONS = [
  { label: 'Immigration backlogs', query: 'USCIS backlog' },
  { label: 'H-1B visa data', query: 'H-1B visa statistics' },
  { label: 'Immigration trends', query: 'immigration trends' },
  { label: 'Crime statistics', query: 'crime rate US' },
  { label: 'DACA program', query: 'DACA recipients' },
  { label: 'Refugee admissions', query: 'refugee admissions' },
];

export default function CatalogPage() {
  const [query, setQuery] = useState('');
  const [limit, setLimit] = useState(10);
  const [mode, setMode] = useState('vgw');
  const [results, setResults] = useState<CatalogResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async (e?: React.FormEvent) => {
    e?.preventDefault();
    if (!query.trim()) return;

    setIsLoading(true);
    setError(null);

    try {
      const response = await api.post<CatalogResponse>(`/search/catalog?limit=${limit}`, {
        query: query.trim(),
        mode,
      });
      setResults(response.data);
    } catch {
      setError('Search failed. Make sure the backend is running.');
      setResults(null);
    } finally {
      setIsLoading(false);
    }
  };

  const showWelcome = !results && !isLoading && !error;

  return (
    <div className="h-[calc(100vh-64px)] overflow-y-auto">
      <div className="max-w-3xl mx-auto px-6 py-8">

        {/* Welcome state */}
        {showWelcome && (
          <div className="flex flex-col items-center pt-8">
            <h1 className="text-4xl font-bold text-[#4A7C59] mb-1">Catalog</h1>
            <h2 className="text-2xl font-semibold text-slate-800 mb-8">Find documents across all sources</h2>

            {/* Search bar — pill style */}
            <form onSubmit={handleSearch} className="w-full max-w-xl mb-4">
              <div className="flex items-center bg-white rounded-full border border-slate-200 shadow-sm px-5 py-3 focus-within:ring-2 focus-within:ring-[#4A7C59]/30 focus-within:border-[#4A7C59]/50 transition-all">
                <input
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Search documents, .gov sites, and USAFacts.org..."
                  className="flex-1 text-sm text-slate-800 placeholder-slate-400 bg-transparent outline-none"
                  disabled={isLoading}
                />
                <button
                  type="submit"
                  disabled={!query.trim() || isLoading}
                  className="w-8 h-8 rounded-full bg-[#4A7C59] text-white flex items-center justify-center hover:bg-[#3D6B4A] disabled:opacity-30 transition-colors flex-shrink-0 ml-2"
                >
                  <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
                  </svg>
                </button>
              </div>
            </form>

            {/* Controls */}
            <div className="flex items-center gap-6 mb-10">
              <div className="flex items-center gap-2">
                <span className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Results</span>
                {LIMIT_OPTIONS.map((n) => (
                  <button
                    key={n}
                    type="button"
                    onClick={() => setLimit(n)}
                    className={`px-2.5 py-1 rounded-full text-[11px] font-medium transition-colors ${
                      limit === n ? 'bg-[#0A3161] text-white' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                    }`}
                  >
                    {n}
                  </button>
                ))}
              </div>
              <div className="flex items-center gap-2">
                <span className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Sources</span>
                {[
                  { id: 'v', label: 'Local' },
                  { id: 'vg', label: 'Local + Graph' },
                  { id: 'vgw', label: 'All' },
                ].map((m) => (
                  <button
                    key={m.id}
                    type="button"
                    onClick={() => setMode(m.id)}
                    className={`px-2.5 py-1 rounded-full text-[11px] font-medium transition-colors ${
                      mode === m.id ? 'bg-[#0A3161] text-white' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                    }`}
                  >
                    {m.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Suggestion cards — 2x3 grid */}
            <div className="grid grid-cols-2 sm:grid-cols-3 gap-3 w-full max-w-xl">
              {SUGGESTIONS.map((s) => (
                <button
                  key={s.query}
                  onClick={() => { setQuery(s.query); }}
                  className="text-left p-4 rounded-2xl bg-[#F0F5F1] border border-[#E0E8E2] hover:bg-[#E5EDE7] hover:border-[#C8D5CB] transition-all"
                >
                  <p className="text-sm font-medium text-slate-800">{s.label}</p>
                  <p className="text-[11px] text-slate-500 mt-0.5">{s.query}</p>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="bg-red-50 text-red-700 p-4 rounded-2xl border border-red-200 mb-6 text-sm">
            {error}
          </div>
        )}

        {/* Loading */}
        {isLoading && (
          <div className="flex flex-col items-center pt-20">
            <div className="flex space-x-2 mb-4">
              <div className="w-3 h-3 rounded-full bg-[#4A7C59] animate-bounce" style={{ animationDelay: '0ms' }} />
              <div className="w-3 h-3 rounded-full bg-[#4A7C59] animate-bounce" style={{ animationDelay: '150ms' }} />
              <div className="w-3 h-3 rounded-full bg-[#4A7C59] animate-bounce" style={{ animationDelay: '300ms' }} />
            </div>
            <p className="text-sm text-slate-500">Searching across sources...</p>
          </div>
        )}

        {/* Results */}
        {results && !isLoading && (
          <>
            {/* Sticky search bar at top when results shown */}
            <div className="mb-6">
              <form onSubmit={handleSearch} className="w-full mb-3">
                <div className="flex items-center bg-white rounded-full border border-slate-200 shadow-sm px-5 py-2.5 focus-within:ring-2 focus-within:ring-[#4A7C59]/30 focus-within:border-[#4A7C59]/50 transition-all">
                  <input
                    type="text"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Search again..."
                    className="flex-1 text-sm text-slate-800 placeholder-slate-400 bg-transparent outline-none"
                    disabled={isLoading}
                  />
                  <button
                    type="submit"
                    disabled={!query.trim() || isLoading}
                    className="w-7 h-7 rounded-full bg-[#4A7C59] text-white flex items-center justify-center hover:bg-[#3D6B4A] disabled:opacity-30 transition-colors flex-shrink-0 ml-2"
                  >
                    <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                      <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
                    </svg>
                  </button>
                </div>
              </form>

              {/* Controls + summary */}
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <div className="flex items-center gap-1.5">
                    {LIMIT_OPTIONS.map((n) => (
                      <button key={n} type="button" onClick={() => { setLimit(n); }}
                        className={`px-2 py-0.5 rounded-full text-[10px] font-medium transition-colors ${limit === n ? 'bg-[#0A3161] text-white' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'}`}
                      >{n}</button>
                    ))}
                  </div>
                  <div className="flex items-center gap-1.5">
                    {[{ id: 'v', l: 'Local' }, { id: 'vg', l: 'L+G' }, { id: 'vgw', l: 'All' }].map((m) => (
                      <button key={m.id} type="button" onClick={() => setMode(m.id)}
                        className={`px-2 py-0.5 rounded-full text-[10px] font-medium transition-colors ${mode === m.id ? 'bg-[#0A3161] text-white' : 'bg-slate-100 text-slate-500 hover:bg-slate-200'}`}
                      >{m.l}</button>
                    ))}
                  </div>
                </div>
                <p className="text-xs text-slate-400">
                  {results.total_results} results · {results.elapsed_ms < 1000 ? `${results.elapsed_ms}ms` : `${(results.elapsed_ms / 1000).toFixed(1)}s`}
                </p>
              </div>
            </div>

            {results.results.length === 0 ? (
              <div className="bg-white rounded-2xl border border-slate-200 p-12 text-center">
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
      </div>
    </div>
  );
}

function ResultCard({ result, rank }: { result: CatalogResult; rank: number }) {
  const style = SOURCE_STYLES[result.source] || SOURCE_STYLES.local;

  return (
    <div className="bg-white rounded-2xl border border-slate-200 p-5 hover:shadow-sm transition-shadow">
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
                <a href={result.url} target="_blank" rel="noopener noreferrer" className="hover:text-[#4A7C59] transition-colors">
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
            <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-medium border ${style.bg} ${style.border}`}>
              <span className={`w-1.5 h-1.5 rounded-full ${style.dot}`} />
              {style.label}
            </span>
          </div>

          {/* Metadata row */}
          <div className="flex items-center gap-3 mt-1.5 text-xs text-slate-400 flex-wrap">
            {result.agency && (
              <span>{result.agency}</span>
            )}
            {result.asset && (
              <span className="text-slate-300">·</span>
            )}
            {result.asset && (
              <span>{result.asset}</span>
            )}
            {result.section && (
              <><span className="text-slate-300">·</span><span>{result.section}</span></>
            )}
            {result.page_number && (
              <><span className="text-slate-300">·</span><span>Page {result.page_number}</span></>
            )}
            {result.download_path && (
              <a
                href={`/api/data/objects/download?path=${encodeURIComponent(result.download_path)}`}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-1 text-[#4A7C59] hover:text-[#3D6B4A]"
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

          {/* Source URL */}
          {result.source === 'local' && result.url && (
            <a href={result.url} target="_blank" rel="noopener noreferrer" className="text-[11px] text-[#4A7C59]/60 hover:text-[#4A7C59] truncate block mt-1">
              {result.url}
            </a>
          )}

          {/* Snippet */}
          {result.snippet && (
            <p className="mt-2.5 text-xs text-slate-600 leading-relaxed line-clamp-3">
              {result.snippet}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
