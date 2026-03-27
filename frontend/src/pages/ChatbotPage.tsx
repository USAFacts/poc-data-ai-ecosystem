import { useState, useRef, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { searchQuery, fetchAvailableEntities, type SearchResult, type DocumentReference, type AnswerMetrics, type TraceStep } from '../api/client';
import ChartRenderer from '../components/ChatBot/ChartRenderer';

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  searchResult?: SearchResult;
}

export default function ChatbotPage() {
  const [messages, setMessages] = useState<Message[]>([
    {
      id: '1',
      role: 'assistant',
      content: 'Hello! I\'m your Q&A assistant. Ask me questions about government data, USCIS forms, visa programs, or any topics in our document collection. I\'ll find relevant documents and provide answers with proper attribution.',
      timestamp: new Date(),
    },
  ]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [selectedMode, setSelectedMode] = useState('vgw');
  const [liveTraceStep, setLiveTraceStep] = useState(-1);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Fetch available entities for suggestions
  const { data: availableEntities } = useQuery({
    queryKey: ['availableEntities'],
    queryFn: fetchAvailableEntities,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: 'user',
      content: input.trim(),
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);
    setLiveTraceStep(0);

    // Animate through stages while waiting for the response
    const stageTimers = [
      setTimeout(() => setLiveTraceStep(1), 2000),   // Retrieval after ~2s
      setTimeout(() => setLiveTraceStep(2), 4000),   // Doc loading after ~4s
      setTimeout(() => setLiveTraceStep(3), 6000),   // Synthesis after ~6s
    ];

    try {
      const result = await searchQuery(userMessage.content, selectedMode);

      // Clear stage timers
      stageTimers.forEach(clearTimeout);

      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: result.answer,
        timestamp: new Date(),
        searchResult: result,
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      stageTimers.forEach(clearTimeout);
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: 'I encountered an error while searching. Please make sure the backend is running and try again.',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
      setLiveTraceStep(-1);
    }
  };

  const handleSuggestionClick = (suggestion: string) => {
    setInput(suggestion);
  };

  const suggestedQuestions = [
    'What is the foreign-born population by state?',
    'What immigration bills are currently before Congress?',
    'What are the poverty and unemployment rates for immigrants vs native-born?',
    'How has refugee admissions changed over the past 10 years?',
    'Which states have the highest share of naturalized citizens?',
    'What is the median household income by immigration status?',
  ];

  // Default forms, programs, and agencies to show when API returns empty
  const defaultForms = ['I-130', 'I-140', 'I-485', 'I-765', 'N-400', 'I-526'];
  const defaultPrograms = ['H-1B', 'H-2B', 'DACA', 'TPS', 'EB-5', 'VAWA'];
  const defaultAgencies = ['USCIS', 'OHSS', 'Census Bureau', 'Federal Register'];

  // Merge API data with defaults (prefer API data if available)
  // Filter out empty strings from API responses
  const apiFormsFiltered = availableEntities?.forms?.filter(f => f.trim()) || [];
  const apiProgramsFiltered = availableEntities?.programs?.filter(p => p.trim()) || [];
  const displayForms = apiFormsFiltered.length > 0 ? apiFormsFiltered : defaultForms;
  const displayPrograms = apiProgramsFiltered.length > 0 ? apiProgramsFiltered : defaultPrograms;
  const displayAgencies = defaultAgencies; // Always show agencies

  const hasMessages = messages.length > 1 || messages[0]?.searchResult;
  const showWelcome = !hasMessages && !isLoading;

  return (
    <div className="h-[calc(100vh-64px)] flex flex-col overflow-hidden">
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-3xl mx-auto px-6 py-8">

          {/* Welcome state — USAFacts style */}
          {showWelcome && (
            <div className="flex flex-col items-center pt-8">
              <h1 className="text-4xl font-bold text-[#4A7C59] mb-1">Hello</h1>
              <h2 className="text-3xl font-semibold text-slate-800 mb-8">What's your question?</h2>

              {/* Search bar — pill style */}
              <form onSubmit={handleSubmit} className="w-full max-w-xl mb-4">
                <div className="flex items-center bg-white rounded-full border border-slate-200 shadow-sm px-5 py-3 focus-within:ring-2 focus-within:ring-[#4A7C59]/30 focus-within:border-[#4A7C59]/50 transition-all">
                  <input
                    type="text"
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    placeholder="Ask about government data, forms, or programs..."
                    className="flex-1 text-sm text-slate-800 placeholder-slate-400 bg-transparent outline-none"
                    disabled={isLoading}
                  />
                  <button
                    type="submit"
                    disabled={!input.trim() || isLoading}
                    className="w-8 h-8 rounded-full bg-[#4A7C59] text-white flex items-center justify-center hover:bg-[#3D6B4A] disabled:opacity-30 transition-colors flex-shrink-0 ml-2"
                  >
                    <SendIcon className="w-4 h-4" />
                  </button>
                </div>
              </form>

              {/* Retrieval mode selector */}
              <div className="flex items-center gap-2 mb-10">
                <span className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Retrieval</span>
                {[
                  { id: 'v', short: 'V', label: 'Weaviate' },
                  { id: 'vg', short: 'V+G', label: 'Weaviate + Graph' },
                  { id: 'vgw', short: 'V+G+W', label: 'All Sources' },
                ].map((mode) => (
                  <button
                    key={mode.id}
                    type="button"
                    onClick={() => setSelectedMode(mode.id)}
                    className={`px-2.5 py-1 rounded-full text-[11px] font-medium transition-colors ${
                      selectedMode === mode.id
                        ? 'bg-[#0A3161] text-white'
                        : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                    }`}
                    title={mode.label}
                  >
                    {mode.short}
                  </button>
                ))}
              </div>

              {/* Suggestion cards — 2x3 grid */}
              <div className="grid grid-cols-2 gap-3 w-full max-w-xl mb-8">
                {suggestedQuestions.map((question, i) => (
                  <button
                    key={i}
                    onClick={() => { setInput(question); }}
                    className="text-left p-3.5 rounded-2xl bg-[#F0F5F1] border border-[#E0E8E2] hover:bg-[#E5EDE7] hover:border-[#C8D5CB] transition-all group"
                  >
                    <p className="text-sm font-medium text-slate-800">{question}</p>
                  </button>
                ))}
              </div>

              {/* Topic pills */}
              <div className="w-full max-w-xl space-y-3">
                <div>
                  <p className="text-[10px] uppercase tracking-wider text-slate-400 font-medium mb-2">Browse by topic</p>
                  <div className="flex flex-wrap gap-1.5">
                    {displayAgencies.map((a, i) => (
                      <button key={`a-${i}`} onClick={() => handleSuggestionClick(`What data do we have from ${a}?`)} className="px-2.5 py-1 bg-amber-50 text-amber-700 rounded-full text-xs hover:bg-amber-100 border border-amber-200 transition-colors">{a}</button>
                    ))}
                    {displayForms.slice(0, 6).map((f, i) => (
                      <button key={`f-${i}`} onClick={() => handleSuggestionClick(`What is ${f}?`)} className="px-2.5 py-1 bg-blue-50 text-blue-700 rounded-full text-xs hover:bg-blue-100 border border-blue-200 transition-colors">{f}</button>
                    ))}
                    {displayPrograms.slice(0, 6).map((p, i) => (
                      <button key={`p-${i}`} onClick={() => handleSuggestionClick(`Tell me about ${p}`)} className="px-2.5 py-1 bg-green-50 text-green-700 rounded-full text-xs hover:bg-green-100 border border-green-200 transition-colors">{p}</button>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Conversation — card-based, not bubbles */}
          {!showWelcome && (
            <div className="space-y-6">
              {messages.filter(m => m.role === 'assistant' && m.searchResult).map((message) => (
                <div key={message.id}>
                  {/* User query as a clean header */}
                  <div className="mb-4">
                    <p className="text-lg font-semibold text-slate-800">
                      {messages.find(m => m.role === 'user' && parseInt(m.id) < parseInt(message.id))?.content || 'Your question'}
                    </p>
                  </div>

                  {/* Reasoning trace */}
                  {message.searchResult?.trace && message.searchResult.trace.length > 0 && (
                    <CompletedTrace steps={message.searchResult.trace} />
                  )}

                  {/* Answer card */}
                  <div className="bg-white rounded-2xl border border-slate-200 p-6 mb-4">
                    <div className="text-sm leading-relaxed">
                      <MarkdownContent content={message.content} isUser={false} />
                    </div>
                  </div>

                  {/* Charts */}
                  {message.searchResult?.charts && message.searchResult.charts.length > 0 && (
                    <div className="mb-4">
                      <ChartRenderer charts={message.searchResult.charts} />
                    </div>
                  )}

                  {/* Metrics + entities row */}
                  <div className="flex flex-wrap items-center gap-2 mb-4">
                    {message.searchResult?.metrics && (
                      <MetricsPanel metrics={message.searchResult.metrics} />
                    )}
                  </div>

                  {message.searchResult && (
                    <div className="flex flex-wrap gap-2 items-center mb-4">
                      {message.searchResult.query_decomposition.entities.map((entity, i) => (
                        <span key={i} className="inline-flex items-center gap-1 px-2.5 py-1 bg-purple-50 text-purple-700 rounded-full text-xs border border-purple-200">
                          <EntityTypeIcon type={entity.type} />{entity.text}
                        </span>
                      ))}
                      {message.searchResult.usage && (
                        <span className="text-[10px] text-slate-400 ml-auto">
                          {message.searchResult.usage.total_tokens.toLocaleString()} tokens · {message.searchResult.usage.documents_returned} docs
                        </span>
                      )}
                    </div>
                  )}

                  {/* Sources */}
                  {message.searchResult && message.searchResult.documents.length > 0 && (
                    <div className="mb-4">
                      <p className="text-xs font-semibold text-slate-500 mb-2 flex items-center gap-1">
                        <SourceIcon className="w-3 h-3" />
                        Sources ({message.searchResult.documents.length})
                      </p>
                      <div className="space-y-2">
                        {message.searchResult.documents.map((doc, i) => (
                          <CollapsibleSourceCard key={i} doc={doc} />
                        ))}
                      </div>
                    </div>
                  )}

                  {message.searchResult && (
                    <CollapsibleRawResponse searchResult={message.searchResult} />
                  )}
                </div>
              ))}

              {isLoading && (
                <LiveTrace currentStep={liveTraceStep} mode={selectedMode} />
              )}
              <div ref={messagesEndRef} />
            </div>
          )}
        </div>
      </div>

      {/* Sticky bottom input — only when conversation is active */}
      {!showWelcome && (
        <div className="border-t border-slate-200 bg-white px-6 py-3 flex-shrink-0">
          <div className="max-w-3xl mx-auto">
            <div className="flex items-center gap-2 mb-2">
              <span className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Retrieval</span>
              {[
                { id: 'v', short: 'V', label: 'Weaviate' },
                { id: 'vg', short: 'V+G', label: 'Weaviate + Graph' },
                { id: 'vgw', short: 'V+G+W', label: 'All Sources' },
              ].map((mode) => (
                <button
                  key={mode.id}
                  type="button"
                  onClick={() => setSelectedMode(mode.id)}
                  className={`px-2.5 py-1 rounded-full text-[11px] font-medium transition-colors ${
                    selectedMode === mode.id
                      ? 'bg-[#0A3161] text-white'
                      : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                  }`}
                  title={mode.label}
                >
                  {mode.short}
                </button>
              ))}
            </div>
            <form onSubmit={handleSubmit} className="flex items-center gap-3">
              <div className="flex-1 flex items-center bg-slate-50 rounded-full border border-slate-200 px-4 py-2.5 focus-within:ring-2 focus-within:ring-[#4A7C59]/30 focus-within:border-[#4A7C59]/50 transition-all">
                <input
                  type="text"
                  value={input}
                  onChange={(e) => setInput(e.target.value)}
                  placeholder="Ask a follow-up question..."
                  className="flex-1 text-sm text-slate-800 placeholder-slate-400 bg-transparent outline-none"
                  disabled={isLoading}
                />
                <button
                  type="submit"
                  disabled={!input.trim() || isLoading}
                  className="w-8 h-8 rounded-full bg-[#4A7C59] text-white flex items-center justify-center hover:bg-[#3D6B4A] disabled:opacity-30 transition-colors flex-shrink-0 ml-2"
                >
                  <SendIcon className="w-4 h-4" />
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

// Live Trace — shown during processing, vertical with animation
const LIVE_STAGES = [
  { stage: 'Query Decomposition', description: 'Analyzing question, extracting entities and intent...' },
  { stage: 'Retrieval & Ranking', description: 'Searching documents, reranking by relevance...' },
  { stage: 'Document Loading', description: 'Loading full document context from storage...' },
  { stage: 'Answer Synthesis', description: 'Claude is generating an answer from sources...' },
];

function LiveTrace({ currentStep, mode }: { currentStep: number; mode: string }) {
  const modeLabels: Record<string, string> = { v: 'Weaviate', vg: 'Weaviate + Graph', vw: 'Weaviate + Web', vgw: 'All Sources' };

  return (
    <div className="ml-4 mb-2">
      <div className="space-y-0">
        {LIVE_STAGES.map((stage, i) => {
          const isActive = i === currentStep;
          const isDone = i < currentStep;
          const isPending = i > currentStep;

          return (
            <div key={i} className="flex items-start gap-2.5 py-1.5">
              {/* Vertical line + dot */}
              <div className="flex flex-col items-center w-5 mt-0.5">
                <div className={`w-3 h-3 rounded-full border-2 flex-shrink-0 ${
                  isDone ? 'bg-green-500 border-green-500' :
                  isActive ? 'bg-blue-500 border-blue-500 animate-pulse' :
                  'bg-white border-slate-300'
                }`}>
                  {isDone && (
                    <svg className="w-3 h-3 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="4">
                      <polyline points="20 6 9 17 4 12" />
                    </svg>
                  )}
                </div>
                {i < LIVE_STAGES.length - 1 && (
                  <div className={`w-0.5 h-5 ${isDone ? 'bg-green-300' : 'bg-slate-200'}`} />
                )}
              </div>

              {/* Content */}
              <div className={`flex-1 ${isPending ? 'opacity-40' : ''}`}>
                <p className={`text-xs font-medium ${isActive ? 'text-blue-700' : isDone ? 'text-green-700' : 'text-slate-400'}`}>
                  {stage.stage}
                  {i === 0 && <span className="font-normal text-slate-400 ml-1">({modeLabels[mode] || mode})</span>}
                </p>
                {(isActive || isDone) && (
                  <p className={`text-[11px] ${isActive ? 'text-blue-500' : 'text-green-500'}`}>
                    {isActive ? stage.description : 'Done'}
                  </p>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// Completed Trace — shown after answer, compact vertical with real outcomes
function CompletedTrace({ steps }: { steps: TraceStep[] }) {
  const [expanded, setExpanded] = useState(false);
  const totalMs = steps.reduce((sum, s) => sum + s.duration_ms, 0);
  const totalStr = totalMs < 1000 ? `${totalMs}ms` : `${(totalMs / 1000).toFixed(1)}s`;

  return (
    <div className="ml-4 mt-2">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1.5 text-[10px] text-slate-400 hover:text-slate-600 transition-colors"
      >
        <svg className={`w-3 h-3 transition-transform ${expanded ? 'rotate-90' : ''}`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <polyline points="9 18 15 12 9 6" />
        </svg>
        <span className="font-medium">Reasoning trace</span>
        <span>({steps.length} steps, {totalStr})</span>
      </button>

      {expanded && (
        <div className="mt-2 space-y-0">
          {steps.map((step, i) => {
            const dur = step.duration_ms;
            const timeStr = dur === 0 ? '' : dur < 1000 ? `${dur}ms` : `${(dur / 1000).toFixed(1)}s`;

            return (
              <div key={i} className="flex items-start gap-2.5 py-1">
                <div className="flex flex-col items-center w-5 mt-0.5">
                  <div className="w-2.5 h-2.5 rounded-full bg-green-500 flex-shrink-0" />
                  {i < steps.length - 1 && <div className="w-0.5 h-4 bg-green-200" />}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-[11px] font-medium text-slate-700">{step.stage}</span>
                    {timeStr && <span className="text-[10px] text-slate-400">{timeStr}</span>}
                  </div>
                  <p className="text-[10px] text-slate-500 truncate">{step.detail}</p>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

// Answer Quality Metrics Panel
function MetricsPanel({ metrics }: { metrics: AnswerMetrics }) {
  const items = [
    { label: 'Source Traceability', abbr: 'STS', value: metrics.sts, threshold: 1.0, format: 'pct' as const, higherBetter: true },
    { label: 'Numerical Verification', abbr: 'NVS', value: metrics.nvs, threshold: 0.95, format: 'pct' as const, higherBetter: true },
    { label: 'Hallucination Flags', abbr: 'HDS', value: metrics.hds, threshold: 0, format: 'int' as const, higherBetter: false },
    { label: 'Cross-Store Consistency', abbr: 'CSCS', value: metrics.cscs, threshold: 0.95, format: 'pct' as const, higherBetter: true },
  ];

  return (
    <div className="flex gap-3 mt-3">
      {items.map((item) => {
        const passes = item.higherBetter
          ? item.value >= item.threshold
          : item.value <= item.threshold;
        const color = passes ? 'text-green-600' : item.value > (item.higherBetter ? 0.7 : 2) ? 'text-amber-500' : 'text-red-500';
        const displayValue = item.format === 'pct'
          ? `${(item.value * 100).toFixed(0)}%`
          : item.value.toString();

        return (
          <div key={item.abbr} className="flex items-center gap-1.5 px-2.5 py-1.5 bg-slate-50 rounded-lg border border-slate-200" title={`${item.label}: ${displayValue} (threshold: ${item.format === 'pct' ? (item.threshold * 100) + '%' : item.threshold})`}>
            <span className="text-[10px] font-bold text-slate-400 uppercase">{item.abbr}</span>
            <span className={`text-xs font-semibold ${color}`}>{displayValue}</span>
            <span className={`w-1.5 h-1.5 rounded-full ${passes ? 'bg-green-500' : 'bg-amber-500'}`} />
          </div>
        );
      })}
    </div>
  );
}

// Collapsible Raw Response Component
function CollapsibleRawResponse({ searchResult }: { searchResult: SearchResult }) {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <div className="mt-3 ml-4">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="flex items-center gap-1 text-xs text-slate-400 hover:text-slate-600 transition-colors"
      >
        <ChevronIcon className={`w-3 h-3 transition-transform ${isExpanded ? 'rotate-90' : ''}`} />
        <CodeIcon className="w-3 h-3" />
        <span>Raw Response</span>
      </button>

      {isExpanded && (
        <div className="mt-2 bg-slate-900 rounded-lg p-4 overflow-x-auto">
          <pre className="text-xs text-green-400 font-mono whitespace-pre-wrap">
            {JSON.stringify(searchResult, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

// Collapsible Source Card Component
function CollapsibleSourceCard({ doc }: { doc: DocumentReference }) {
  const [isExpanded, setIsExpanded] = useState(false);

  return (
    <div className="bg-slate-50 rounded-lg border border-slate-200 overflow-hidden">
      {/* Header - Always visible */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full text-left p-3 hover:bg-slate-100 transition-colors"
      >
        <div className="flex items-start justify-between gap-2">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <ChevronIcon className={`w-4 h-4 text-slate-400 transition-transform ${isExpanded ? 'rotate-90' : ''}`} />
              <p className="text-sm font-medium text-slate-800 truncate">
                {doc.document_title}
              </p>
              {doc.file_format && (
                <span className="px-1.5 py-0.5 bg-slate-200 text-slate-600 rounded text-[10px] font-medium flex-shrink-0">
                  {doc.file_format}
                </span>
              )}
            </div>
            <p className="text-xs text-slate-500 mt-0.5 ml-6">
              {doc.agency_name}
              {doc.sheet_name && <span className="text-green-600"> • Sheet: {doc.sheet_name}</span>}
              {doc.page_number && <span className="text-purple-600"> • Page {doc.page_number}{doc.page_count ? ` of ${doc.page_count}` : ''}</span>}
              {!doc.sheet_name && !doc.page_number && doc.page_count && <span> • {doc.page_count} pages</span>}
            </p>
          </div>
          <RelevanceIndicator score={doc.relevance_score} />
        </div>
      </button>

      {/* Expanded Content */}
      {isExpanded && (
        <div className="px-4 pb-4 pt-2 border-t border-slate-200 space-y-3">
          {/* Location Details */}
          {(doc.sheet_name || doc.page_number || doc.section) && (
            <div className="flex flex-wrap gap-2">
              {doc.sheet_name && (
                <span className="inline-flex items-center gap-1 px-2 py-1 bg-green-100 text-green-700 rounded text-xs">
                  <SheetIcon className="w-3 h-3" />
                  {doc.sheet_name}
                </span>
              )}
              {doc.page_number && (
                <span className="inline-flex items-center gap-1 px-2 py-1 bg-purple-100 text-purple-700 rounded text-xs">
                  <PageIcon className="w-3 h-3" />
                  Page {doc.page_number}{doc.page_count ? ` of ${doc.page_count}` : ''}
                </span>
              )}
              {doc.section && (
                <span className="inline-flex items-center gap-1 px-2 py-1 bg-blue-100 text-blue-700 rounded text-xs">
                  <SectionIcon className="w-3 h-3" />
                  {doc.section}
                </span>
              )}
            </div>
          )}

          {/* Section Summary */}
          {doc.section_summary && (
            <div>
              <p className="text-xs text-slate-500 font-medium mb-1">Section Summary</p>
              <p className="text-xs text-slate-600">{doc.section_summary}</p>
            </div>
          )}

          {/* Excerpt */}
          {doc.snippet && (
            <div>
              <p className="text-xs text-slate-500 font-medium mb-1">Excerpt</p>
              <div className="text-xs text-slate-600 bg-white p-3 rounded border-l-2 border-blue-300">
                <MarkdownContent content={doc.snippet} isUser={false} />
              </div>
            </div>
          )}

          {/* Relevance Bar */}
          <div>
            <p className="text-xs text-slate-500 font-medium mb-1">Relevance</p>
            <div className="flex items-center gap-2">
              <div className="flex-1 bg-slate-200 rounded-full h-1.5">
                <div
                  className="bg-green-500 h-1.5 rounded-full"
                  style={{ width: `${doc.relevance_score * 100}%` }}
                />
              </div>
              <span className="text-xs font-semibold text-green-600">
                {(doc.relevance_score * 100).toFixed(0)}%
              </span>
            </div>
          </div>

          {/* Source Link */}
          {doc.source_url && (
            <a
              href={doc.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 hover:underline"
            >
              <ExternalLinkIcon className="w-3 h-3" />
              View Original Source
            </a>
          )}
        </div>
      )}
    </div>
  );
}

// Markdown Content Renderer with Table Support
function MarkdownContent({ content, isUser }: { content: string; isUser: boolean }) {
  const lines = content.split('\n');
  const elements: JSX.Element[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Check for table (starts with |)
    if (line.trim().startsWith('|')) {
      const tableLines: string[] = [];
      while (i < lines.length && lines[i].trim().startsWith('|')) {
        tableLines.push(lines[i]);
        i++;
      }
      elements.push(
        <MarkdownTable key={`table-${i}`} lines={tableLines} isUser={isUser} />
      );
      continue;
    }

    // Check for blockquote
    if (line.startsWith('>')) {
      const quoteContent = line.slice(1).trim();
      elements.push(
        <div key={i} className={`border-l-2 ${isUser ? 'border-blue-300' : 'border-slate-300'} pl-3 my-2 italic`}>
          <FormattedText text={quoteContent} />
        </div>
      );
      i++;
      continue;
    }

    // Check for headers
    if (line.startsWith('#')) {
      const level = line.match(/^#+/)?.[0].length || 1;
      const headerText = line.replace(/^#+\s*/, '');
      const headerClass = level === 1 ? 'text-lg font-bold' :
                          level === 2 ? 'text-base font-semibold' :
                          'text-sm font-medium';
      elements.push(
        <div key={i} className={`${headerClass} mt-2 mb-1`}>
          <FormattedText text={headerText} />
        </div>
      );
      i++;
      continue;
    }

    // Check for bullet list
    if (line.match(/^\s*[-*+]\s/)) {
      const listItems: string[] = [];
      while (i < lines.length && lines[i].match(/^\s*[-*+]\s/)) {
        listItems.push(lines[i].replace(/^\s*[-*+]\s/, ''));
        i++;
      }
      elements.push(
        <ul key={`list-${i}`} className="list-disc list-inside my-2 space-y-1">
          {listItems.map((item, idx) => (
            <li key={idx} className="text-sm">
              <FormattedText text={item} />
            </li>
          ))}
        </ul>
      );
      continue;
    }

    // Check for numbered list
    if (line.match(/^\s*\d+\.\s/)) {
      const listItems: string[] = [];
      while (i < lines.length && lines[i].match(/^\s*\d+\.\s/)) {
        listItems.push(lines[i].replace(/^\s*\d+\.\s/, ''));
        i++;
      }
      elements.push(
        <ol key={`olist-${i}`} className="list-decimal list-inside my-2 space-y-1">
          {listItems.map((item, idx) => (
            <li key={idx} className="text-sm">
              <FormattedText text={item} />
            </li>
          ))}
        </ol>
      );
      continue;
    }

    // Regular paragraph
    if (line.trim()) {
      elements.push(
        <span key={i}>
          <FormattedText text={line} />
          {i < lines.length - 1 && <br />}
        </span>
      );
    } else if (i > 0 && i < lines.length - 1) {
      // Empty line = paragraph break
      elements.push(<br key={i} />);
    }
    i++;
  }

  return <>{elements}</>;
}

// Markdown Table Renderer
function MarkdownTable({ lines, isUser }: { lines: string[]; isUser: boolean }) {
  if (lines.length < 2) return null;

  const parseRow = (line: string): string[] => {
    return line
      .split('|')
      .map(cell => cell.trim())
      .filter((_, idx, arr) => idx > 0 && idx < arr.length - 1);
  };

  const headers = parseRow(lines[0]);
  // Skip separator line (line with ---)
  const dataLines = lines.slice(2);
  const rows = dataLines.map(parseRow);

  return (
    <div className="my-3 overflow-x-auto">
      <table className={`min-w-full text-xs border-collapse ${isUser ? 'border-blue-300' : 'border-slate-300'}`}>
        <thead>
          <tr className={isUser ? 'bg-blue-500' : 'bg-slate-200'}>
            {headers.map((header, idx) => (
              <th
                key={idx}
                className={`px-3 py-2 text-left font-semibold border ${
                  isUser ? 'border-blue-400 text-white' : 'border-slate-300 text-slate-700'
                }`}
              >
                {header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIdx) => (
            <tr key={rowIdx} className={rowIdx % 2 === 0 ? (isUser ? 'bg-blue-400/20' : 'bg-white') : (isUser ? 'bg-blue-400/10' : 'bg-slate-50')}>
              {row.map((cell, cellIdx) => (
                <td
                  key={cellIdx}
                  className={`px-3 py-1.5 border ${
                    isUser ? 'border-blue-400/50' : 'border-slate-200'
                  }`}
                >
                  <FormattedText text={cell} />
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// Inline text formatting (bold, italic, code)
function FormattedText({ text }: { text: string }) {
  // Handle bold (**text**), italic (*text*), and inline code (`code`)
  const parts = text.split(/(\*\*.*?\*\*|\*.*?\*|`.*?`)/g);

  return (
    <>
      {parts.map((part, i) => {
        if (part.startsWith('**') && part.endsWith('**')) {
          return <strong key={i}>{part.slice(2, -2)}</strong>;
        }
        if (part.startsWith('*') && part.endsWith('*') && !part.startsWith('**')) {
          return <em key={i}>{part.slice(1, -1)}</em>;
        }
        if (part.startsWith('`') && part.endsWith('`')) {
          return (
            <code key={i} className="px-1 py-0.5 bg-slate-200 text-slate-800 rounded text-xs font-mono">
              {part.slice(1, -1)}
            </code>
          );
        }
        return <span key={i}>{part}</span>;
      })}
    </>
  );
}

function RelevanceIndicator({ score }: { score: number }) {
  const percentage = Math.round(score * 100);
  const color = percentage >= 70 ? 'text-green-600 bg-green-100' :
                percentage >= 40 ? 'text-yellow-600 bg-yellow-100' :
                'text-slate-600 bg-slate-100';

  return (
    <span className={`text-xs font-semibold px-1.5 py-0.5 rounded flex-shrink-0 ${color}`}>
      {percentage}%
    </span>
  );
}

function EntityTypeIcon({ type }: { type: string }) {
  switch (type) {
    case 'form':
      return <span className="text-[10px]">📄</span>;
    case 'visa_type':
      return <span className="text-[10px]">🛂</span>;
    case 'program':
      return <span className="text-[10px]">🏛️</span>;
    case 'fiscal_year':
    case 'date':
      return <span className="text-[10px]">📅</span>;
    case 'metric':
      return <span className="text-[10px]">📊</span>;
    default:
      return <span className="text-[10px]">🔖</span>;
  }
}

// Icons

function SendIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="22" y1="2" x2="11" y2="13" />
      <polygon points="22 2 15 22 11 13 2 9 22 2" />
    </svg>
  );
}

function SourceIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <polyline points="14 2 14 8 20 8" />
      <line x1="16" y1="13" x2="8" y2="13" />
      <line x1="16" y1="17" x2="8" y2="17" />
      <polyline points="10 9 9 9 8 9" />
    </svg>
  );
}

function ChevronIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="9 18 15 12 9 6" />
    </svg>
  );
}

function SheetIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
      <line x1="3" y1="9" x2="21" y2="9" />
      <line x1="3" y1="15" x2="21" y2="15" />
      <line x1="9" y1="3" x2="9" y2="21" />
      <line x1="15" y1="3" x2="15" y2="21" />
    </svg>
  );
}

function PageIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <polyline points="14 2 14 8 20 8" />
    </svg>
  );
}

function SectionIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="4" y1="6" x2="20" y2="6" />
      <line x1="4" y1="12" x2="14" y2="12" />
      <line x1="4" y1="18" x2="18" y2="18" />
    </svg>
  );
}

function ExternalLinkIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
      <polyline points="15 3 21 3 21 9" />
      <line x1="10" y1="14" x2="21" y2="3" />
    </svg>
  );
}

function CodeIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="16 18 22 12 16 6" />
      <polyline points="8 6 2 12 8 18" />
    </svg>
  );
}
