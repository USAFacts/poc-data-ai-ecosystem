import { useState, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from 'recharts';
import {
  fetchExperiments,
  fetchExperiment,
  fetchExperimentComparison,
  fetchSearchModes,
  createExperiment,
  deleteExperiment,
  type Experiment,
  type AggregateMetrics,
  type ModeMetrics,
  type QuestionComparison,
  type ExperimentResult,
} from '../api/client';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
type View = 'list' | 'detail' | 'comparison';

// ---------------------------------------------------------------------------
// Mode constants
// ---------------------------------------------------------------------------
const MODE_COLORS: Record<string, string> = {
  v: '#3B82F6',   // blue
  vg: '#10B981',  // green
  vw: '#F59E0B',  // amber
  vgw: '#8B5CF6', // purple
};

const MODE_LABELS: Record<string, string> = {
  v: 'Weaviate Only',
  vg: 'Weaviate + Graph',
  vw: 'Weaviate + Web',
  vgw: 'All Sources',
};

const MODE_TAILWIND_BG: Record<string, string> = {
  v: 'bg-blue-50 border-blue-200 text-blue-700',
  vg: 'bg-green-50 border-green-200 text-green-700',
  vw: 'bg-amber-50 border-amber-200 text-amber-700',
  vgw: 'bg-purple-50 border-purple-200 text-purple-700',
};

const MODE_TAILWIND_NUM: Record<string, string> = {
  v: 'text-blue-600',
  vg: 'text-green-600',
  vw: 'text-amber-600',
  vgw: 'text-purple-600',
};

/**
 * Normalize AggregateMetrics from either legacy (weaviate_only/weaviate_graph)
 * or new (by_mode) format into a consistent by_mode structure.
 */
function normalizeMetrics(metrics: AggregateMetrics): {
  by_mode: Record<string, ModeMetrics>;
  wins: Record<string, number>;
  by_category: Record<string, { by_mode: Record<string, ModeMetrics>; wins: Record<string, number> }>;
} {
  // New format already has by_mode
  if (metrics.by_mode) {
    return {
      by_mode: metrics.by_mode,
      wins: metrics.wins ?? {},
      by_category: metrics.by_category ?? {},
    };
  }

  // Legacy format: convert weaviate_only / weaviate_graph
  const by_mode: Record<string, ModeMetrics> = {};
  const wins: Record<string, number> = {};

  if (metrics.weaviate_only) by_mode['v'] = metrics.weaviate_only;
  if (metrics.weaviate_graph) by_mode['vg'] = metrics.weaviate_graph;

  // Legacy wins
  const legacyWins = metrics.wins as Record<string, number> | undefined;
  if (legacyWins) {
    if (legacyWins.weaviate_only !== undefined) wins['v'] = legacyWins.weaviate_only;
    if (legacyWins.weaviate_graph !== undefined) wins['vg'] = legacyWins.weaviate_graph;
    if (legacyWins.tie !== undefined) wins['tie'] = legacyWins.tie;
  }

  // Legacy by_category
  const by_category: Record<string, { by_mode: Record<string, ModeMetrics>; wins: Record<string, number> }> = {};
  const legacyCat = metrics.by_category as Record<string, any> | undefined;
  if (legacyCat) {
    for (const [cat, cb] of Object.entries(legacyCat)) {
      const catModes: Record<string, ModeMetrics> = {};
      const catWins: Record<string, number> = {};
      if (cb.by_mode) {
        // New format inside category
        Object.assign(catModes, cb.by_mode);
        Object.assign(catWins, cb.wins ?? {});
      } else {
        // Legacy format inside category
        if (cb.weaviate_only) catModes['v'] = cb.weaviate_only;
        if (cb.weaviate_graph) catModes['vg'] = cb.weaviate_graph;
        if (cb.wins) {
          if (cb.wins.weaviate_only !== undefined) catWins['v'] = cb.wins.weaviate_only;
          if (cb.wins.weaviate_graph !== undefined) catWins['vg'] = cb.wins.weaviate_graph;
          if (cb.wins.tie !== undefined) catWins['tie'] = cb.wins.tie;
        }
      }
      by_category[cat] = { by_mode: catModes, wins: catWins };
    }
  }

  return { by_mode, wins, by_category };
}

/**
 * Normalize QuestionComparison to always use modes Record.
 */
function normalizeComparison(comp: QuestionComparison): {
  question_id: string;
  question_text: string;
  category: string;
  modes: Record<string, ExperimentResult>;
  winner: string;
} {
  if (comp.modes) {
    return {
      question_id: comp.question_id,
      question_text: comp.question_text,
      category: comp.category,
      modes: comp.modes,
      winner: comp.winner,
    };
  }
  // Legacy
  const modes: Record<string, ExperimentResult> = {};
  if (comp.weaviate_only) modes['v'] = comp.weaviate_only;
  if (comp.weaviate_graph) modes['vg'] = comp.weaviate_graph;

  // Normalize winner name
  let winner = comp.winner;
  if (winner === 'weaviate_only') winner = 'v';
  else if (winner === 'weaviate_graph') winner = 'vg';

  return { question_id: comp.question_id, question_text: comp.question_text, category: comp.category, modes, winner };
}

function getModeLabel(modeId: string): string {
  return MODE_LABELS[modeId] ?? modeId;
}

function getModeColor(modeId: string): string {
  return MODE_COLORS[modeId] ?? '#6B7280';
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function statusColor(status: string) {
  switch (status) {
    case 'completed': return 'bg-green-100 text-green-800';
    case 'running': return 'bg-blue-100 text-blue-800';
    case 'failed': return 'bg-red-100 text-red-800';
    case 'pending': return 'bg-yellow-100 text-yellow-800';
    default: return 'bg-slate-100 text-slate-800';
  }
}

function formatDuration(start: string | null, end: string | null): string {
  if (!start) return '--';
  const s = new Date(start).getTime();
  const e = end ? new Date(end).getTime() : Date.now();
  const diff = e - s;
  if (diff < 1000) return `${diff}ms`;
  const secs = Math.round(diff / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  const remSecs = secs % 60;
  return `${mins}m ${remSecs}s`;
}

function formatDate(d: string | null): string {
  if (!d) return '--';
  return new Date(d).toLocaleString();
}

function pct(value: number, max: number): number {
  if (max === 0) return 0;
  return Math.round((value / max) * 100);
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------
export default function ExperimentTrackerPage() {
  const [view, setView] = useState<View>('list');
  const [selectedId, setSelectedId] = useState<number | null>(null);

  const goToDetail = (id: number) => { setSelectedId(id); setView('detail'); };
  const goToComparison = () => setView('comparison');
  const goToList = () => { setView('list'); setSelectedId(null); };
  const goBackToDetail = () => setView('detail');

  if (view === 'list') return <ListView onSelect={goToDetail} onDelete={() => {}} />;
  if (view === 'detail' && selectedId !== null) {
    return <DetailView id={selectedId} onBack={goToList} onViewQuestions={goToComparison} />;
  }
  if (view === 'comparison' && selectedId !== null) {
    return <ComparisonView id={selectedId} onBack={goBackToDetail} />;
  }
  return null;
}

// ===========================================================================
// VIEW 1: List
// ===========================================================================
function ListView({ onSelect }: { onSelect: (id: number) => void; onDelete?: () => void }) {
  const queryClient = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState('');
  const [selectedModes, setSelectedModes] = useState<string[]>(['v', 'vg']);
  const [samplePercent, setSamplePercent] = useState(100);

  const { data: experiments, isLoading, error } = useQuery({
    queryKey: ['experiments'],
    queryFn: fetchExperiments,
    refetchInterval: (query) => {
      const data = query.state.data as Experiment[] | undefined;
      if (data?.some((e) => e.status === 'running')) return 5000;
      return false;
    },
  });

  const { data: availableModes } = useQuery({
    queryKey: ['searchModes'],
    queryFn: fetchSearchModes,
  });

  const deleteMutation = useMutation({
    mutationFn: (id: number) => deleteExperiment(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['experiments'] });
    },
  });

  const onDelete = (id: number) => deleteMutation.mutate(id);

  const mutation = useMutation({
    mutationFn: ({ name, modes, samplePercent: sp }: { name: string; modes: string[]; samplePercent: number }) =>
      createExperiment(name, modes, sp),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['experiments'] });
      setNewName('');
      setSelectedModes(['v', 'vg']);
      setSamplePercent(100);
      setShowCreate(false);
    },
  });

  const toggleMode = (modeId: string) => {
    setSelectedModes((prev) =>
      prev.includes(modeId) ? prev.filter((m) => m !== modeId) : [...prev, modeId]
    );
  };

  // Estimate total questions (assume ~200 base; actual count comes from backend)
  const estimatedQuestions = Math.round((samplePercent / 100) * 200);

  if (isLoading) {
    return <PageShell><Loading message="Loading experiments..." /></PageShell>;
  }

  if (error) {
    return (
      <PageShell>
        <ErrorBox message="Failed to load experiments. Make sure the backend is running." />
      </PageShell>
    );
  }

  return (
    <PageShell>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-slate-900">Experiment Tracker</h1>
          <p className="text-sm text-slate-500">Compare retrieval modes across stratified question samples</p>
        </div>
        <button
          onClick={() => setShowCreate(true)}
          className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
        >
          Run New Experiment
        </button>
      </div>

      {/* Inline create form */}
      {showCreate && (
        <div className="bg-white border border-slate-200 rounded-xl p-5 mb-6 space-y-4">
          {/* Name */}
          <div>
            <label className="block text-xs font-medium text-slate-600 uppercase tracking-wide mb-1">
              Experiment Name
            </label>
            <input
              type="text"
              placeholder="Experiment name..."
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              className="w-full px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              autoFocus
            />
          </div>

          {/* Mode checkboxes */}
          <div>
            <label className="block text-xs font-medium text-slate-600 uppercase tracking-wide mb-2">
              Search Modes
            </label>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
              {(availableModes ?? [
                { id: 'v', label: 'Weaviate Only', available: true },
                { id: 'vg', label: 'Weaviate + Graph', available: true },
                { id: 'vw', label: 'Weaviate + Web', available: false },
                { id: 'vgw', label: 'All Sources', available: false },
              ]).map((mode) => {
                const checked = selectedModes.includes(mode.id);
                const disabled = !mode.available;
                return (
                  <label
                    key={mode.id}
                    className={`flex items-center gap-2 px-3 py-2 rounded-lg border text-sm cursor-pointer transition-colors ${
                      disabled
                        ? 'bg-slate-50 border-slate-200 text-slate-400 cursor-not-allowed'
                        : checked
                        ? 'bg-blue-50 border-blue-300 text-blue-800'
                        : 'bg-white border-slate-200 text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={disabled}
                      onChange={() => toggleMode(mode.id)}
                      className="rounded border-slate-300 text-blue-600 focus:ring-blue-500"
                    />
                    <span>
                      {mode.label}
                      <span className="text-[10px] text-slate-400 ml-1">({mode.id})</span>
                    </span>
                  </label>
                );
              })}
            </div>
          </div>

          {/* Sample percent slider */}
          <div>
            <label className="block text-xs font-medium text-slate-600 uppercase tracking-wide mb-2">
              Sample Size
            </label>
            <div className="flex items-center gap-4">
              <input
                type="range"
                min={10}
                max={100}
                step={10}
                value={samplePercent}
                onChange={(e) => setSamplePercent(Number(e.target.value))}
                className="flex-1 h-2 bg-slate-200 rounded-lg appearance-none cursor-pointer accent-blue-600"
              />
              <span className="text-sm font-medium text-slate-700 w-32 text-right">
                {samplePercent}% — ~{estimatedQuestions} questions
              </span>
            </div>
          </div>

          {/* Buttons */}
          <div className="flex items-center gap-3 pt-1">
            <button
              disabled={!newName.trim() || selectedModes.length < 2 || mutation.isPending}
              onClick={() => mutation.mutate({ name: newName.trim(), modes: selectedModes, samplePercent })}
              className="px-4 py-2 bg-green-600 text-white text-sm font-medium rounded-lg hover:bg-green-700 disabled:opacity-50 transition-colors"
            >
              {mutation.isPending ? 'Starting...' : 'Start'}
            </button>
            <button
              onClick={() => { setShowCreate(false); setNewName(''); setSelectedModes(['v', 'vg']); setSamplePercent(100); }}
              className="px-3 py-2 text-sm text-slate-500 hover:text-slate-700"
            >
              Cancel
            </button>
            {selectedModes.length < 2 && (
              <span className="text-xs text-amber-600">Select at least 2 modes to compare</span>
            )}
          </div>
        </div>
      )}

      {mutation.isError && (
        <ErrorBox message="Failed to create experiment. Please try again." />
      )}

      {/* Experiments table */}
      {!experiments || experiments.length === 0 ? (
        <EmptyState message="No experiments yet. Click 'Run New Experiment' to get started." />
      ) : (
        <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-[10px] text-slate-500 uppercase bg-slate-50">
                <th className="text-left py-3 px-4 font-medium">Name</th>
                <th className="text-left py-3 px-4 font-medium">Status</th>
                <th className="text-left py-3 px-4 font-medium">Progress</th>
                <th className="text-left py-3 px-4 font-medium">Duration</th>
                <th className="text-left py-3 px-4 font-medium">Created</th>
                <th className="py-3 px-4 w-10"></th>
              </tr>
            </thead>
            <tbody className="text-slate-700">
              {experiments.map((exp) => (
                <tr
                  key={exp.id}
                  onClick={() => onSelect(exp.id)}
                  className="border-t border-slate-100 hover:bg-slate-50 cursor-pointer"
                >
                  <td className="py-3 px-4 font-medium">{exp.name}</td>
                  <td className="py-3 px-4">
                    <StatusBadge status={exp.status} />
                  </td>
                  <td className="py-3 px-4">
                    <div className="flex items-center gap-2">
                      <div className="w-24 bg-slate-200 rounded-full h-2">
                        <div
                          className="bg-blue-600 h-2 rounded-full transition-all"
                          style={{ width: `${pct(exp.completed_questions, exp.total_questions)}%` }}
                        />
                      </div>
                      <span className="text-xs text-slate-500">
                        {exp.completed_questions}/{exp.total_questions}
                      </span>
                    </div>
                  </td>
                  <td className="py-3 px-4 text-slate-500">
                    {formatDuration(exp.started_at, exp.completed_at)}
                  </td>
                  <td className="py-3 px-4 text-slate-500">{formatDate(exp.created_at)}</td>
                  <td className="py-3 px-4">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        if (confirm(`Delete experiment "${exp.name}"?`)) {
                          onDelete(exp.id);
                        }
                      }}
                      className="p-1.5 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded transition-colors"
                      title="Delete experiment"
                    >
                      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                        <polyline points="3 6 5 6 21 6" />
                        <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2" />
                        <line x1="10" y1="11" x2="10" y2="17" />
                        <line x1="14" y1="11" x2="14" y2="17" />
                      </svg>
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </PageShell>
  );
}

// ===========================================================================
// VIEW 2: Detail
// ===========================================================================
function DetailView({
  id,
  onBack,
  onViewQuestions,
}: {
  id: number;
  onBack: () => void;
  onViewQuestions: () => void;
}) {
  const { data: experiment, isLoading, error } = useQuery({
    queryKey: ['experiment', id],
    queryFn: () => fetchExperiment(id),
    refetchInterval: (query) => {
      const data = query.state.data as Experiment | undefined;
      if (data?.status === 'running') return 5000;
      return false;
    },
  });

  if (isLoading) return <PageShell><Loading message="Loading experiment..." /></PageShell>;
  if (error || !experiment) return <PageShell><ErrorBox message="Failed to load experiment." /></PageShell>;

  const metrics = experiment.aggregate_metrics;
  const normalized = metrics ? normalizeMetrics(metrics) : null;
  const isRunning = experiment.status === 'running';
  const config = experiment.config;

  return (
    <PageShell>
      {/* Header */}
      <div className="mb-6">
        <button onClick={onBack} className="text-sm text-blue-600 hover:text-blue-800 mb-2 flex items-center gap-1">
          <BackArrow /> Back to Experiments
        </button>
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold text-slate-900">{experiment.name}</h1>
          <StatusBadge status={experiment.status} />
        </div>
        <p className="text-sm text-slate-500 mt-1">
          Created {formatDate(experiment.created_at)}
          {experiment.completed_at && ` | Completed ${formatDate(experiment.completed_at)}`}
          {` | Duration: ${formatDuration(experiment.started_at, experiment.completed_at)}`}
        </p>
        {config && (
          <p className="text-sm text-slate-500 mt-1">
            Modes: {config.modes.map((m) => getModeLabel(m)).join(', ')}
            {' | '}Sample: {config.sample_percent}% ({experiment.total_questions} questions)
          </p>
        )}
      </div>

      {/* Progress bar for running */}
      {isRunning && (
        <div className="bg-white rounded-xl border border-slate-200 p-4 mb-6">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-slate-700">Running...</span>
            <span className="text-sm text-slate-500">
              {experiment.completed_questions} / {experiment.total_questions} questions
            </span>
          </div>
          <div className="w-full bg-slate-200 rounded-full h-3">
            <div
              className="bg-blue-600 h-3 rounded-full transition-all animate-pulse"
              style={{ width: `${pct(experiment.completed_questions, experiment.total_questions)}%` }}
            />
          </div>
        </div>
      )}

      {/* Metrics sections (only if completed and metrics available) */}
      {normalized && (
        <>
          <SummaryCards normalized={normalized} />
          <WinScoreboard normalized={normalized} />
          <MetricComparisonBars normalized={normalized} />
          <MetricComparisonChart normalized={normalized} />
          <CategoryBreakdownTable normalized={normalized} />

          <div className="mt-6">
            <button
              onClick={onViewQuestions}
              className="px-6 py-3 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
            >
              View Questions
            </button>
          </div>
        </>
      )}

      {!normalized && !isRunning && (
        <EmptyState message="No metrics available for this experiment." />
      )}
    </PageShell>
  );
}

// -- Normalized metrics type used by detail components --
type NormalizedMetrics = {
  by_mode: Record<string, ModeMetrics>;
  wins: Record<string, number>;
  by_category: Record<string, { by_mode: Record<string, ModeMetrics>; wins: Record<string, number> }>;
};

// -- Summary Cards (N modes) --
function SummaryCards({ normalized }: { normalized: NormalizedMetrics }) {
  const modeIds = Object.keys(normalized.by_mode);

  // Find best confidence and relevance across modes
  const bestConf = Math.max(...modeIds.map((m) => normalized.by_mode[m].mean_confidence));
  const bestRel = Math.max(...modeIds.map((m) => normalized.by_mode[m].mean_relevance));

  return (
    <div className={`grid gap-4 mb-6`} style={{ gridTemplateColumns: `repeat(${Math.min(modeIds.length, 4)}, minmax(0, 1fr))` }}>
      {modeIds.map((modeId) => {
        const m = normalized.by_mode[modeId];
        const isConfBest = m.mean_confidence === bestConf;
        const isRelBest = m.mean_relevance === bestRel;
        const color = getModeColor(modeId);
        return (
          <div
            key={modeId}
            className="bg-white rounded-xl p-4 border shadow-sm"
            style={isConfBest || isRelBest ? { borderColor: color, boxShadow: `0 0 0 1px ${color}20` } : {}}
          >
            <h4 className="text-[10px] font-medium text-slate-500 uppercase tracking-wide mb-2">
              {getModeLabel(modeId)}
            </h4>
            <div className="space-y-1">
              <div className="flex justify-between">
                <span className="text-xs text-slate-500">Confidence</span>
                <span className={`text-sm font-bold ${isConfBest ? '' : 'text-slate-600'}`} style={isConfBest ? { color } : {}}>
                  {m.mean_confidence.toFixed(3)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-xs text-slate-500">Relevance</span>
                <span className={`text-sm font-bold ${isRelBest ? '' : 'text-slate-600'}`} style={isRelBest ? { color } : {}}>
                  {m.mean_relevance.toFixed(3)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-xs text-slate-500">Entity Cov.</span>
                <span className="text-sm font-medium text-slate-600">{m.mean_entity_coverage.toFixed(3)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-xs text-slate-500">STS</span>
                <span className="text-sm font-medium text-slate-600">{(m.mean_sts ?? 0).toFixed(3)}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-xs text-slate-500">HDS</span>
                <span className="text-sm font-medium text-slate-600">{(m.mean_hds ?? 0).toFixed(1)}</span>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// -- Win Scoreboard (N modes + tie) --
function WinScoreboard({ normalized }: { normalized: NormalizedMetrics }) {
  const { wins } = normalized;
  const modeIds = Object.keys(normalized.by_mode);
  const entries = [...modeIds.map((m) => ({ key: m, label: `${getModeLabel(m)} Wins`, value: wins[m] ?? 0 })), { key: 'tie', label: 'Ties', value: wins['tie'] ?? 0 }];

  return (
    <div className="grid gap-4 mb-6" style={{ gridTemplateColumns: `repeat(${entries.length}, minmax(0, 1fr))` }}>
      {entries.map((entry) => {
        const tw = entry.key === 'tie' ? 'bg-slate-50 border-slate-200 text-slate-700' : (MODE_TAILWIND_BG[entry.key] ?? 'bg-slate-50 border-slate-200 text-slate-700');
        const numTw = entry.key === 'tie' ? 'text-slate-600' : (MODE_TAILWIND_NUM[entry.key] ?? 'text-slate-600');
        return (
          <div key={entry.key} className={`rounded-xl p-6 border text-center ${tw}`}>
            <p className={`text-4xl font-bold ${numTw}`}>{entry.value}</p>
            <p className="text-sm font-medium mt-1">{entry.label}</p>
          </div>
        );
      })}
    </div>
  );
}

// -- Metric Comparison Bars (N modes) --
function MetricComparisonBars({ normalized }: { normalized: NormalizedMetrics }) {
  const modeIds = Object.keys(normalized.by_mode);

  const metricDefs: { label: string; key: keyof ModeMetrics; unit?: string; lowerBetter?: boolean }[] = [
    { label: 'Confidence', key: 'mean_confidence' },
    { label: 'Relevance', key: 'mean_relevance' },
    { label: 'Entity Coverage', key: 'mean_entity_coverage' },
    { label: 'Source Traceability (STS)', key: 'mean_sts' },
    { label: 'Numerical Verification (NVS)', key: 'mean_nvs' },
    { label: 'Hallucination Flags (HDS)', key: 'mean_hds', lowerBetter: true },
    { label: 'Cross-Store Consistency (CSCS)', key: 'mean_cscs' },
    { label: 'Response Time', key: 'mean_response_time_ms', unit: 'ms', lowerBetter: true },
    { label: 'Tokens', key: 'mean_tokens', unit: '', lowerBetter: true },
  ];

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 mb-6">
      <h3 className="text-base font-semibold text-slate-900 mb-4">Metric Comparison</h3>
      <div className="space-y-5">
        {metricDefs.map((def) => {
          const values = modeIds.map((m) => normalized.by_mode[m][def.key] ?? 0);
          const max = Math.max(...values) || 1;
          const bestVal = def.lowerBetter ? Math.min(...values) : Math.max(...values);

          return (
            <div key={def.label}>
              <p className="text-xs font-medium text-slate-600 mb-1">{def.label}</p>
              <div className="space-y-1">
                {modeIds.map((modeId) => {
                  const val = normalized.by_mode[modeId][def.key] ?? 0;
                  const barPct = Math.round((val / max) * 100);
                  const isBest = val === bestVal;
                  const color = getModeColor(modeId);

                  return (
                    <div key={modeId} className="flex items-center gap-2">
                      <span className="text-[10px] text-slate-400 w-24 truncate">{getModeLabel(modeId)}</span>
                      <div className="flex-1 bg-slate-100 rounded-full h-4 overflow-hidden">
                        <div
                          className="h-4 rounded-full transition-all"
                          style={{ width: `${barPct}%`, backgroundColor: color, opacity: isBest ? 1 : 0.5 }}
                        />
                      </div>
                      <span className="text-xs text-slate-600 w-20 text-right font-medium">
                        {def.unit === 'ms' ? `${Math.round(val)}ms` : val.toFixed(3)}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// -- Recharts Comparison Chart (N modes) --
function MetricComparisonChart({ normalized }: { normalized: NormalizedMetrics }) {
  const modeIds = Object.keys(normalized.by_mode);

  const data = ['Confidence', 'Relevance', 'Entity Cov.', 'STS', 'NVS', 'CSCS'].map((metricLabel) => {
    const keyMap: Record<string, keyof ModeMetrics> = {
      'Confidence': 'mean_confidence',
      'Relevance': 'mean_relevance',
      'Entity Cov.': 'mean_entity_coverage',
      'STS': 'mean_sts',
      'NVS': 'mean_nvs',
      'CSCS': 'mean_cscs',
    };
    const key = keyMap[metricLabel];
    const row: Record<string, string | number> = { metric: metricLabel };
    for (const modeId of modeIds) {
      row[modeId] = Number((normalized.by_mode[modeId][key] ?? 0).toFixed(3));
    }
    return row;
  });

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 mb-6">
      <h3 className="text-base font-semibold text-slate-900 mb-4">Summary Comparison</h3>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={data} barGap={4}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="metric" tick={{ fontSize: 12 }} />
          <YAxis tick={{ fontSize: 12 }} domain={[0, 1]} />
          <Tooltip />
          <Legend />
          {modeIds.map((modeId) => (
            <Bar key={modeId} dataKey={modeId} name={getModeLabel(modeId)} fill={getModeColor(modeId)} radius={[4, 4, 0, 0]} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

// -- Category Breakdown (N modes) --
function CategoryBreakdownTable({ normalized }: { normalized: NormalizedMetrics }) {
  const categories = Object.entries(normalized.by_category);
  if (categories.length === 0) return null;

  const modeIds = Object.keys(normalized.by_mode);

  const metricKeys: { label: string; key: keyof ModeMetrics }[] = [
    { label: 'Confidence', key: 'mean_confidence' },
    { label: 'Relevance', key: 'mean_relevance' },
    { label: 'Entity Cov.', key: 'mean_entity_coverage' },
    { label: 'STS', key: 'mean_sts' },
    { label: 'NVS', key: 'mean_nvs' },
    { label: 'CSCS', key: 'mean_cscs' },
  ];

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 mb-6">
      <h3 className="text-base font-semibold text-slate-900 mb-4">Category Breakdown</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-[10px] text-slate-500 uppercase bg-slate-50">
              <th className="text-left py-3 px-3 font-medium">Category</th>
              {metricKeys.map((mk) => (
                <th key={mk.label} className="text-center py-3 px-3 font-medium" colSpan={modeIds.length}>
                  {mk.label}
                </th>
              ))}
              <th className="text-center py-3 px-3 font-medium">Wins</th>
            </tr>
            <tr className="text-[9px] text-slate-400 uppercase">
              <th />
              {metricKeys.map((mk) =>
                modeIds.map((m) => (
                  <th key={`${mk.label}-${m}`} className="py-1 px-2">{m.toUpperCase()}</th>
                ))
              )}
              <th />
            </tr>
          </thead>
          <tbody className="text-slate-700">
            {categories.map(([cat, catData]) => {
              return (
                <tr key={cat} className="border-t border-slate-100">
                  <td className="py-3 px-3 font-medium capitalize">{cat}</td>
                  {metricKeys.map((mk) => {
                    const vals = modeIds.map((m) => catData.by_mode[m]?.[mk.key] ?? 0);
                    const bestVal = Math.max(...vals);
                    return modeIds.map((m) => {
                      const val = catData.by_mode[m]?.[mk.key] ?? 0;
                      const isBest = val === bestVal && bestVal > 0;
                      return (
                        <td
                          key={`${mk.label}-${m}`}
                          className="py-3 px-2 text-center"
                          style={isBest ? { color: getModeColor(m), fontWeight: 600 } : {}}
                        >
                          {val.toFixed(3)}
                        </td>
                      );
                    });
                  })}
                  <td className="py-3 px-3 text-center text-xs whitespace-nowrap">
                    {modeIds.map((m) => catData.wins[m] ?? 0).join('/')}
                    /{catData.wins['tie'] ?? 0}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ===========================================================================
// VIEW 3: Question Comparison
// ===========================================================================
function ComparisonView({ id, onBack }: { id: number; onBack: () => void }) {
  const [categoryFilter, setCategoryFilter] = useState('');
  const [searchText, setSearchText] = useState('');
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const { data: comparisons, isLoading, error } = useQuery({
    queryKey: ['experimentComparison', id],
    queryFn: () => fetchExperimentComparison(id),
  });

  const normalizedComparisons = useMemo(() => {
    if (!comparisons) return [];
    return comparisons.map(normalizeComparison);
  }, [comparisons]);

  const categories = useMemo(() => {
    return [...new Set(normalizedComparisons.map((c) => c.category))].sort();
  }, [normalizedComparisons]);

  const filtered = useMemo(() => {
    return normalizedComparisons.filter((c) => {
      if (categoryFilter && c.category !== categoryFilter) return false;
      if (searchText && !c.question_text.toLowerCase().includes(searchText.toLowerCase())) return false;
      return true;
    });
  }, [normalizedComparisons, categoryFilter, searchText]);

  // Determine the mode IDs from the first comparison that has data
  const modeIds = useMemo(() => {
    for (const comp of normalizedComparisons) {
      const keys = Object.keys(comp.modes);
      if (keys.length > 0) return keys;
    }
    return [];
  }, [normalizedComparisons]);

  if (isLoading) return <PageShell><Loading message="Loading comparisons..." /></PageShell>;
  if (error) return <PageShell><ErrorBox message="Failed to load question comparisons." /></PageShell>;

  return (
    <PageShell>
      <div className="mb-6">
        <button onClick={onBack} className="text-sm text-blue-600 hover:text-blue-800 mb-2 flex items-center gap-1">
          <BackArrow /> Back to Experiment Detail
        </button>
        <h1 className="text-2xl font-semibold text-slate-900">Question Comparisons</h1>
        <p className="text-sm text-slate-500">{filtered.length} of {normalizedComparisons.length} questions shown</p>
      </div>

      {/* Filters */}
      <div className="flex gap-3 mb-6">
        <select
          value={categoryFilter}
          onChange={(e) => setCategoryFilter(e.target.value)}
          className="px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
        >
          <option value="">All Categories</option>
          {categories.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
        <input
          type="text"
          placeholder="Search questions..."
          value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          className="flex-1 px-3 py-2 border border-slate-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      {/* Question cards */}
      {filtered.length === 0 ? (
        <EmptyState message="No questions match the current filters." />
      ) : (
        <div className="space-y-4">
          {filtered.map((comp) => (
            <QuestionCard
              key={comp.question_id}
              comparison={comp}
              modeIds={modeIds}
              expanded={expandedId === comp.question_id}
              onToggle={() => setExpandedId(expandedId === comp.question_id ? null : comp.question_id)}
            />
          ))}
        </div>
      )}
    </PageShell>
  );
}

function QuestionCard({
  comparison,
  modeIds,
  expanded,
  onToggle,
}: {
  comparison: { question_id: string; question_text: string; category: string; modes: Record<string, ExperimentResult>; winner: string };
  modeIds: string[];
  expanded: boolean;
  onToggle: () => void;
}) {
  const { winner } = comparison;
  const winnerColor = getModeColor(winner);

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
      {/* Header */}
      <div
        className="px-6 py-4 cursor-pointer hover:bg-slate-50"
        onClick={onToggle}
      >
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <p className="text-sm font-medium text-slate-900">{comparison.question_text}</p>
            <div className="flex items-center gap-2 mt-1">
              <span className="px-2 py-0.5 bg-slate-100 text-slate-600 rounded text-[10px] uppercase font-medium">
                {comparison.category}
              </span>
              <span
                className="px-2 py-0.5 rounded text-[10px] uppercase font-medium"
                style={{
                  backgroundColor: winner === 'tie' ? '#f1f5f9' : `${winnerColor}15`,
                  color: winner === 'tie' ? '#64748b' : winnerColor,
                }}
              >
                Winner: {getModeLabel(winner)}
              </span>
            </div>
          </div>
          <span className="text-slate-400 ml-2">{expanded ? '\u25B2' : '\u25BC'}</span>
        </div>
      </div>

      {/* Comparison columns - N modes */}
      <div
        className="border-t border-slate-100"
        style={{ display: 'grid', gridTemplateColumns: `repeat(${modeIds.length}, minmax(0, 1fr))` }}
      >
        {modeIds.map((modeId) => (
          <ResultColumn
            key={modeId}
            result={comparison.modes[modeId] ?? null}
            modeId={modeId}
            isWinner={winner === modeId}
            showAnswer={expanded}
          />
        ))}
      </div>
    </div>
  );
}

function ResultColumn({
  result,
  modeId,
  isWinner,
  showAnswer,
}: {
  result: ExperimentResult | null;
  modeId: string;
  isWinner: boolean;
  showAnswer: boolean;
}) {
  const color = getModeColor(modeId);

  if (!result) {
    return (
      <div className="p-4 text-center text-sm text-slate-400">No data</div>
    );
  }

  return (
    <div
      className="p-4"
      style={isWinner ? { borderLeft: `4px solid ${color}`, backgroundColor: `${color}08` } : {}}
    >
      <p className="text-xs font-semibold uppercase mb-3" style={{ color }}>{getModeLabel(modeId)}</p>
      <div className="space-y-2 text-sm">
        <div>
          <span className="text-slate-500 text-xs">Confidence</span>
          <div className="flex items-center gap-2">
            <div className="flex-1 bg-slate-200 rounded-full h-2">
              <div
                className="h-2 rounded-full"
                style={{ width: `${Math.round(result.confidence * 100)}%`, backgroundColor: color }}
              />
            </div>
            <span className="text-xs font-medium w-12 text-right">{result.confidence.toFixed(3)}</span>
          </div>
        </div>
        <CompactStat label="Relevance" value={result.avg_relevance_score.toFixed(3)} />
        <CompactStat label="Entity Coverage" value={`${(result.entity_coverage * 100).toFixed(1)}%`} />
        <CompactStat label="STS" value={`${((result.sts ?? 0) * 100).toFixed(0)}%`} />
        <CompactStat label="NVS" value={`${((result.nvs ?? 0) * 100).toFixed(0)}%`} />
        <CompactStat label="HDS" value={(result.hds ?? 0).toString()} />
        <CompactStat label="CSCS" value={`${((result.cscs ?? 0) * 100).toFixed(0)}%`} />
        <CompactStat label="Response Time" value={`${Math.round(result.response_time_ms)}ms`} />
        <CompactStat label="Tokens" value={result.total_tokens.toLocaleString()} />
      </div>
      {showAnswer && (
        <div className="mt-3 pt-3 border-t border-slate-200">
          <p className="text-xs font-medium text-slate-500 mb-1">Answer</p>
          <p className="text-xs text-slate-700 whitespace-pre-wrap max-h-40 overflow-y-auto">{result.answer}</p>
        </div>
      )}
    </div>
  );
}

function CompactStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex justify-between">
      <span className="text-slate-500 text-xs">{label}</span>
      <span className="text-xs font-medium text-slate-700">{value}</span>
    </div>
  );
}

// ===========================================================================
// Shared UI Components
// ===========================================================================
function PageShell({ children }: { children: React.ReactNode }) {
  return <div>{children}</div>;
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium capitalize ${statusColor(status)}`}>
      {status === 'running' && (
        <span className="w-1.5 h-1.5 rounded-full bg-blue-500 mr-1.5 animate-pulse" />
      )}
      {status}
    </span>
  );
}

function Loading({ message }: { message: string }) {
  return (
    <div className="flex items-center justify-center h-64">
      <div className="text-slate-500">{message}</div>
    </div>
  );
}

function ErrorBox({ message }: { message: string }) {
  return (
    <div className="bg-red-50 text-red-700 p-4 rounded-lg border border-red-200">
      {message}
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="text-center py-16 text-slate-400 text-sm">
      {message}
    </div>
  );
}

function BackArrow() {
  return (
    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="15 18 9 12 15 6" />
    </svg>
  );
}
