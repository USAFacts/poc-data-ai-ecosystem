export default function SwaggerPage() {
  // Point to the backend directly since Swagger UI loads sub-resources
  // from relative paths that the SPA router would intercept
  const backendUrl = import.meta.env.VITE_API_URL
    ? import.meta.env.VITE_API_URL.replace('/api', '/docs')
    : 'http://localhost:8000/docs';

  return (
    <div className="h-full flex flex-col -m-8">
      <div className="flex items-center justify-between px-6 py-3 border-b border-slate-200 bg-white">
        <div>
          <h1 className="text-lg font-semibold text-slate-900">Swagger UI</h1>
          <p className="text-xs text-slate-500">Interactive API documentation — test endpoints directly</p>
        </div>
        <a
          href={backendUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-blue-600 hover:text-blue-800 flex items-center gap-1"
        >
          Open in new tab
          <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
            <polyline points="15 3 21 3 21 9" /><line x1="10" y1="14" x2="21" y2="3" />
          </svg>
        </a>
      </div>
      <iframe
        src={backendUrl}
        className="flex-1 w-full border-0"
        title="Swagger UI"
      />
    </div>
  );
}
