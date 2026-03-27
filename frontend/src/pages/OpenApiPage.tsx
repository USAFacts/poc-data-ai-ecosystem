import { useState, useEffect } from 'react';

export default function OpenApiPage() {
  const [spec, setSpec] = useState<string>('');
  const [isLoading, setIsLoading] = useState(true);
  const [copied, setCopied] = useState(false);

  const apiBase = import.meta.env.VITE_API_URL || '/api';
  const specUrl = apiBase.replace('/api', '') + '/openapi.json';

  useEffect(() => {
    fetch(specUrl.startsWith('http') ? specUrl : `http://localhost:8000/openapi.json`)
      .then(r => r.json())
      .then(data => {
        setSpec(JSON.stringify(data, null, 2));
        setIsLoading(false);
      })
      .catch(() => {
        // Try via the proxy
        fetch('/api/../openapi.json')
          .then(r => r.text())
          .then(text => { setSpec(text); setIsLoading(false); })
          .catch(() => { setSpec('{"error": "Failed to load OpenAPI spec"}'); setIsLoading(false); });
      });
  }, [specUrl]);

  const handleCopy = () => {
    navigator.clipboard.writeText(spec);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownload = () => {
    const blob = new Blob([spec], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'openapi.json';
    a.click();
    URL.revokeObjectURL(url);
  };

  // Parse for summary stats
  let endpointCount = 0;
  let tagCount = 0;
  let title = '';
  let version = '';
  try {
    const parsed = JSON.parse(spec);
    endpointCount = Object.keys(parsed.paths || {}).length;
    tagCount = (parsed.tags || []).length;
    title = parsed.info?.title || '';
    version = parsed.info?.version || '';
  } catch { /* ignore */ }

  return (
    <div>
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-slate-900">OpenAPI Specification</h1>
          <p className="text-sm text-slate-500 mt-1">
            {title && `${title} v${version} — `}{endpointCount} endpoints, {tagCount} tags
          </p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={handleCopy}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-50"
          >
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <rect x="9" y="9" width="13" height="13" rx="2" /><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
            </svg>
            {copied ? 'Copied!' : 'Copy'}
          </button>
          <button
            onClick={handleDownload}
            className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-50"
          >
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" /><polyline points="7 10 12 15 17 10" /><line x1="12" y1="15" x2="12" y2="3" />
            </svg>
            Download
          </button>
        </div>
      </div>

      {isLoading ? (
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
        </div>
      ) : (
        <div className="bg-slate-900 rounded-lg border border-slate-700 overflow-hidden">
          <pre className="p-5 text-xs text-green-400 font-mono overflow-auto max-h-[calc(100vh-220px)] leading-relaxed">
            {spec}
          </pre>
        </div>
      )}
    </div>
  );
}
