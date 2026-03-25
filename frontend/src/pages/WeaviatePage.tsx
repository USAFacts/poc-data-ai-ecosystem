import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { fetchWeaviateStatus, WeaviateCollection } from '../api/client';

export default function WeaviatePage() {
  const { data, isLoading, error, refetch, isFetching } = useQuery({
    queryKey: ['weaviateDetailedStatus'],
    queryFn: fetchWeaviateStatus,
    staleTime: 10000,
  });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 text-red-700 p-4 rounded-lg border border-red-200">
        Failed to load Weaviate status. Make sure the backend is running.
      </div>
    );
  }

  if (!data) return null;

  return (
    <div>
      {/* Header */}
      <div className="mb-6 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold text-slate-900">Weaviate Vector Database</h1>
          <span
            className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${
              data.status === 'connected'
                ? 'bg-green-100 text-green-700'
                : 'bg-red-100 text-red-700'
            }`}
          >
            <span
              className={`w-2 h-2 rounded-full ${
                data.status === 'connected' ? 'bg-green-500' : 'bg-red-500'
              }`}
            ></span>
            {data.status === 'connected' ? 'Connected' : 'Disconnected'}
          </span>
        </div>
        <button
          onClick={() => refetch()}
          disabled={isFetching}
          className="inline-flex items-center gap-2 px-4 py-2 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-50 disabled:opacity-50"
        >
          <svg
            className={`w-4 h-4 ${isFetching ? 'animate-spin' : ''}`}
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
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
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
          <p className="text-sm text-slate-500 mb-1">Total Objects</p>
          <p className="text-2xl font-bold text-slate-900">
            {data.total_objects.toLocaleString()}
          </p>
        </div>
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
          <p className="text-sm text-slate-500 mb-1">Collections</p>
          <p className="text-2xl font-bold text-slate-900">
            {data.collections.length}
          </p>
        </div>
        <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6">
          <p className="text-sm text-slate-500 mb-1">Host</p>
          <p className="text-lg font-semibold text-slate-900">
            {data.connection.host}:{data.connection.port}
          </p>
          <p className="text-xs text-slate-400 mt-1">gRPC: {data.connection.grpc_port}</p>
        </div>
      </div>

      {/* Collections */}
      <div className="space-y-4">
        <h2 className="text-lg font-semibold text-slate-900">Collections</h2>
        {data.collections.length === 0 ? (
          <div className="bg-white rounded-lg shadow-sm border border-slate-200 p-6 text-slate-500 text-center">
            No collections found.
          </div>
        ) : (
          data.collections.map((collection) => (
            <CollectionCard key={collection.name} collection={collection} />
          ))
        )}
      </div>
    </div>
  );
}

function CollectionCard({ collection }: { collection: WeaviateCollection }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="bg-white rounded-lg shadow-sm border border-slate-200">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between p-6 text-left hover:bg-slate-50 transition-colors"
      >
        <div className="flex items-center gap-4">
          <div>
            <h3 className="text-base font-semibold text-slate-900">
              {collection.name}
            </h3>
            <p className="text-sm text-slate-500">
              {collection.object_count.toLocaleString()} objects &middot;{' '}
              {collection.properties.length} properties
            </p>
          </div>
        </div>
        <svg
          className={`w-5 h-5 text-slate-400 transition-transform duration-200 ${
            expanded ? 'rotate-180' : ''
          }`}
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </button>

      {expanded && (
        <div className="border-t border-slate-200 p-6">
          {collection.properties.length === 0 ? (
            <p className="text-sm text-slate-500">No properties defined.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-slate-200">
                    <th className="text-left py-2 pr-4 font-medium text-slate-600">
                      Property Name
                    </th>
                    <th className="text-left py-2 pr-4 font-medium text-slate-600">
                      Data Type
                    </th>
                    <th className="text-left py-2 font-medium text-slate-600">
                      Tokenization
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {collection.properties.map((prop) => (
                    <tr
                      key={prop.name}
                      className="border-b border-slate-100 last:border-0"
                    >
                      <td className="py-2 pr-4 font-mono text-slate-800">
                        {prop.name}
                      </td>
                      <td className="py-2 pr-4">
                        <span className="inline-flex px-2 py-0.5 rounded bg-blue-50 text-blue-700 text-xs font-medium">
                          {prop.data_type}
                        </span>
                      </td>
                      <td className="py-2 text-slate-600">
                        {prop.tokenization ?? (
                          <span className="text-slate-400">none</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
