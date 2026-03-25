import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { fetchAgencies, fetchAgency } from '../api/client';
import type { Agency, AgencyDetail } from '../types';

export default function AgenciesPage() {
  const [selectedAgency, setSelectedAgency] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [categoryFilter, setCategoryFilter] = useState('');

  const { data: agencies, isLoading, error } = useQuery({
    queryKey: ['agencies'],
    queryFn: fetchAgencies,
  });

  const { data: agencyDetail, isLoading: detailLoading } = useQuery({
    queryKey: ['agency', selectedAgency],
    queryFn: () => fetchAgency(selectedAgency!),
    enabled: !!selectedAgency,
  });

  const categories = useMemo(() => {
    if (!agencies) return [];
    const cats = new Set<string>();
    agencies.forEach((a: Agency) => {
      const cat = a.labels?.category as string | undefined;
      if (cat) cats.add(cat);
    });
    return Array.from(cats).sort();
  }, [agencies]);

  const filteredAgencies = useMemo(() => {
    if (!agencies) return [];
    return agencies.filter((agency: Agency) => {
      const matchesSearch = !searchQuery ||
        agency.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        agency.full_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        (agency.description?.toLowerCase().includes(searchQuery.toLowerCase()) ?? false);
      const matchesCategory = !categoryFilter ||
        (agency.labels?.category as string) === categoryFilter;
      return matchesSearch && matchesCategory;
    });
  }, [agencies, searchQuery, categoryFilter]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-slate-500">Loading agencies...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 text-red-700 p-4 rounded-lg border border-red-200">
        Failed to load agencies. Make sure the backend is running.
      </div>
    );
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-slate-900">Agencies</h1>
          <p className="text-sm text-slate-500">Browse registered agencies and their assets</p>
        </div>
        <span className="px-3 py-1 text-sm bg-slate-100 text-slate-600 rounded-full">
          {filteredAgencies.length} of {agencies?.length || 0} agencies
        </span>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 mb-6">
        <div className="relative">
          <svg className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            type="text"
            placeholder="Search agencies..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-10 pr-4 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
        <select
          value={categoryFilter}
          onChange={(e) => setCategoryFilter(e.target.value)}
          className="w-full px-4 py-2 text-sm border border-slate-200 rounded-lg bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
        >
          <option value="">All Categories</option>
          {categories.map((cat) => (
            <option key={cat} value={cat}>
              {cat.charAt(0).toUpperCase() + cat.slice(1)}
            </option>
          ))}
        </select>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2">
          <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
            <table className="min-w-full divide-y divide-slate-200">
              <thead className="bg-slate-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    Agency
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    Full Name
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    Category
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    Assets
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-slate-500 uppercase tracking-wider">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-slate-200">
                {filteredAgencies.map((agency) => (
                  <tr
                    key={agency.id}
                    className={`hover:bg-slate-50 cursor-pointer transition-colors ${
                      selectedAgency === agency.name ? 'bg-blue-50' : ''
                    }`}
                    onClick={() => setSelectedAgency(agency.name)}
                  >
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm font-medium text-slate-900">
                        {agency.name}
                      </div>
                    </td>
                    <td className="px-6 py-4">
                      <div className="text-sm text-slate-500">
                        {agency.full_name}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {typeof agency.labels?.category === 'string' && (
                        <span className="px-2 py-1 text-xs font-medium rounded-full bg-slate-100 text-slate-700 capitalize">
                          {agency.labels.category}
                        </span>
                      )}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className="px-2 py-1 text-xs font-semibold rounded-full bg-green-100 text-green-800">
                        {agency.asset_count} assets
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedAgency(agency.name);
                        }}
                        className="text-blue-600 hover:text-blue-900"
                      >
                        View Details
                      </button>
                    </td>
                  </tr>
                ))}
                {filteredAgencies.length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-6 py-8 text-center text-sm text-slate-500">
                      No agencies match your filters.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-6">
          <h2 className="text-base font-semibold text-slate-900 mb-4">
            Agency Details
          </h2>

          {detailLoading && (
            <div className="text-slate-500 text-sm">Loading details...</div>
          )}

          {!selectedAgency && !detailLoading && (
            <p className="text-slate-500 text-sm">
              Select an agency to view its details and assets
            </p>
          )}

          {agencyDetail && !detailLoading && (
            <AgencyDetailPanel agency={agencyDetail} />
          )}
        </div>
      </div>
    </div>
  );
}

function AgencyDetailPanel({ agency }: { agency: AgencyDetail }) {
  return (
    <div>
      <div className="mb-4">
        <h3 className="text-sm font-medium text-slate-900">{agency.name}</h3>
        <p className="text-xs text-slate-500">{agency.full_name}</p>
      </div>

      {agency.description && (
        <div className="mb-4">
          <p className="text-[10px] text-slate-500 uppercase font-medium">Description</p>
          <p className="text-sm text-slate-700 mt-1">{agency.description}</p>
        </div>
      )}

      {agency.base_url && (
        <div className="mb-4">
          <p className="text-[10px] text-slate-500 uppercase font-medium">Base URL</p>
          <a
            href={agency.base_url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm text-blue-600 hover:underline mt-1 block break-all"
          >
            {agency.base_url}
          </a>
        </div>
      )}

      <div className="mt-6">
        <p className="text-[10px] text-slate-500 uppercase font-medium mb-2">
          Assets ({agency.assets.length})
        </p>
        <div className="space-y-2 max-h-64 overflow-y-auto">
          {agency.assets.map((asset) => (
            <div
              key={asset.id}
              className="p-3 bg-slate-50 rounded-lg border border-slate-200"
            >
              <p className="text-sm font-medium text-slate-900">{asset.name}</p>
              {asset.description && (
                <p className="text-xs text-slate-500 mt-1">{asset.description}</p>
              )}
            </div>
          ))}
          {agency.assets.length === 0 && (
            <p className="text-sm text-slate-500">No assets registered</p>
          )}
        </div>
      </div>
    </div>
  );
}
