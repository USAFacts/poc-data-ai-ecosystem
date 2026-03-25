import { useState, useRef, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { searchQuery, fetchAvailableEntities, type SearchResult, type DocumentReference, type AnswerMetrics } from '../api/client';
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
  const [selectedMode, setSelectedMode] = useState('vg');
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

    try {
      const result = await searchQuery(userMessage.content, selectedMode);

      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: result.answer,
        timestamp: new Date(),
        searchResult: result,
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: 'assistant',
        content: 'I encountered an error while searching. Please make sure the backend is running and try again.',
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSuggestionClick = (suggestion: string) => {
    setInput(suggestion);
  };

  const suggestedQuestions = [
    'Which Family-Based form has the largest pending backlog?',
    'How many forms were received across all categories in FY2025 Q3?',
    'Which countries have the most H-2B beneficiaries?',
    'How many TPS beneficiaries are there by country?',
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

  return (
    <div className="h-[calc(100vh-2rem)] flex flex-col">
      <div className="mb-4">
        <h1 className="text-2xl font-semibold text-slate-900">Q&A</h1>
        <p className="text-sm text-slate-500">Ask questions about government data with full source attribution</p>
      </div>

      <div className="flex-1 flex gap-6 min-h-0">
        {/* Chat Area */}
        <div className="flex-1 flex flex-col bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
          {/* Messages */}
          <div className="flex-1 overflow-y-auto p-6 space-y-6">
            {messages.map((message) => (
              <div key={message.id}>
                <div className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                  <div
                    className={`max-w-[85%] rounded-2xl px-4 py-3 ${
                      message.role === 'user'
                        ? 'bg-blue-600 text-white'
                        : 'bg-slate-100 text-slate-800'
                    }`}
                  >
                    <div className="text-sm leading-relaxed">
                      <MarkdownContent content={message.content} isUser={message.role === 'user'} />
                    </div>
                    <p className={`text-[10px] mt-2 ${
                      message.role === 'user' ? 'text-blue-200' : 'text-slate-400'
                    }`}>
                      {message.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                    </p>
                  </div>
                </div>

                {/* Chart visualizations */}
                {message.searchResult?.charts && message.searchResult.charts.length > 0 && (
                  <div className="mt-4">
                    <ChartRenderer charts={message.searchResult.charts} />
                  </div>
                )}

                {/* Answer Quality Metrics */}
                {message.searchResult?.metrics && (
                  <div className="ml-4">
                    <MetricsPanel metrics={message.searchResult.metrics} />
                  </div>
                )}

                {/* Query Decomposition and Usage Metrics (for assistant messages) */}
                {message.searchResult && (
                  <div className="mt-2 ml-4">
                    <div className="flex flex-wrap gap-2 items-center">
                      {message.searchResult.query_decomposition.entities.map((entity, i) => (
                        <span
                          key={i}
                          className="inline-flex items-center gap-1 px-2 py-1 bg-purple-100 text-purple-700 rounded-full text-xs"
                        >
                          <EntityTypeIcon type={entity.type} />
                          {entity.text}
                        </span>
                      ))}
                      {message.searchResult.query_decomposition.entities.length > 0 && (
                        <span className="inline-flex items-center px-2 py-1 bg-slate-100 text-slate-600 rounded-full text-xs">
                          Intent: {message.searchResult.query_decomposition.intent}
                        </span>
                      )}
                    </div>
                    {/* Usage Metrics */}
                    {message.searchResult.usage && (
                      <div className="flex flex-wrap gap-3 mt-2 text-[10px] text-slate-500">
                        {message.searchResult.claude_used ? (
                          <span className="inline-flex items-center gap-1 text-green-600">
                            <ClaudeIcon className="w-3 h-3" />
                            Claude AI
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 text-orange-600">
                            <WarningIcon className="w-3 h-3" />
                            Fallback mode (API key not configured)
                          </span>
                        )}
                        <span className="inline-flex items-center gap-1">
                          <TokenIcon className="w-3 h-3" />
                          {message.searchResult.usage.total_tokens.toLocaleString()} tokens
                          {message.searchResult.claude_used && (
                            <span className="text-slate-400">
                              ({message.searchResult.usage.context_window_used_percent}% context)
                            </span>
                          )}
                        </span>
                        <span className="inline-flex items-center gap-1">
                          <DataIcon className="w-3 h-3" />
                          {message.searchResult.usage.data_volume_display} processed
                        </span>
                        <span className="inline-flex items-center gap-1">
                          <SearchIcon className="w-3 h-3" />
                          {message.searchResult.usage.documents_returned}/{message.searchResult.usage.documents_searched} docs
                        </span>
                      </div>
                    )}
                  </div>
                )}

                {/* Collapsible Source Documents */}
                {message.searchResult && message.searchResult.documents.length > 0 && (
                  <div className="mt-3 ml-4">
                    <p className="text-xs font-semibold text-slate-500 mb-2 flex items-center gap-1">
                      <SourceIcon className="w-3 h-3" />
                      Sources ({message.searchResult.documents.length} documents)
                    </p>
                    <div className="space-y-2">
                      {message.searchResult.documents.map((doc, i) => (
                        <CollapsibleSourceCard key={i} doc={doc} />
                      ))}
                    </div>
                  </div>
                )}

                {/* Collapsible Raw Response */}
                {message.searchResult && (
                  <CollapsibleRawResponse searchResult={message.searchResult} />
                )}
              </div>
            ))}

            {isLoading && (
              <div className="flex justify-start">
                <div className="bg-gradient-to-r from-orange-50 to-amber-50 border border-orange-200 rounded-2xl px-4 py-3">
                  <div className="flex items-center gap-3">
                    <div className="flex space-x-1">
                      <div className="w-2 h-2 bg-orange-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                      <div className="w-2 h-2 bg-orange-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                      <div className="w-2 h-2 bg-orange-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
                    </div>
                    <span className="text-xs text-orange-700">Claude is analyzing your question and searching documents...</span>
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>

          {/* Input Area */}
          <div className="border-t border-slate-200 p-4">
            <div className="flex items-center gap-2 mb-2">
              <span className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Retrieval</span>
              {[
                { id: 'v', label: 'Weaviate', short: 'V', color: 'blue' },
                { id: 'vg', label: 'Weaviate + Graph', short: 'V+G', color: 'green' },
                { id: 'vgw', label: 'All Sources', short: 'V+G+W', color: 'purple' },
              ].map((mode) => (
                <button
                  key={mode.id}
                  type="button"
                  onClick={() => setSelectedMode(mode.id)}
                  className={`px-2.5 py-1 rounded-md text-[11px] font-medium transition-colors ${
                    selectedMode === mode.id
                      ? mode.color === 'blue'
                        ? 'bg-blue-100 text-blue-700 ring-1 ring-blue-300'
                        : mode.color === 'green'
                        ? 'bg-green-100 text-green-700 ring-1 ring-green-300'
                        : 'bg-purple-100 text-purple-700 ring-1 ring-purple-300'
                      : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                  }`}
                  title={mode.label}
                >
                  {mode.short}
                </button>
              ))}
            </div>
            <form onSubmit={handleSubmit} className="flex gap-3">
              <input
                type="text"
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="Ask about government data, forms, or programs..."
                className="flex-1 bg-slate-50 text-slate-900 rounded-xl px-4 py-3 placeholder-slate-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:bg-white transition-colors"
                disabled={isLoading}
              />
              <button
                type="submit"
                disabled={!input.trim() || isLoading}
                className="bg-blue-600 text-white px-5 py-3 rounded-xl font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
              >
                <span>Ask</span>
                <SendIcon className="w-4 h-4" />
              </button>
            </form>
          </div>
        </div>

        {/* Right Panel - Suggestions Only */}
        <div className="w-72 flex-shrink-0 flex flex-col gap-4">
          <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-4">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">Suggested Questions</h3>
            <div className="space-y-2">
              {suggestedQuestions.map((question, i) => (
                <button
                  key={i}
                  onClick={() => handleSuggestionClick(question)}
                  className="w-full text-left text-sm text-slate-600 hover:text-blue-600 hover:bg-blue-50 px-3 py-2 rounded-lg transition-colors"
                >
                  {question}
                </button>
              ))}
            </div>
          </div>

          <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-4">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">Available Topics</h3>
            <div className="space-y-3">
              {/* Agencies */}
              <div>
                <p className="text-xs text-slate-500 font-medium mb-1">Agencies</p>
                <div className="flex flex-wrap gap-1">
                  {displayAgencies.map((agency, i) => (
                    <button
                      key={i}
                      onClick={() => handleSuggestionClick(`What data do we have from ${agency}?`)}
                      className="px-2 py-0.5 bg-amber-100 text-amber-700 rounded text-xs hover:bg-amber-200"
                    >
                      {agency}
                    </button>
                  ))}
                </div>
              </div>

              {/* Forms */}
              <div>
                <p className="text-xs text-slate-500 font-medium mb-1">Forms</p>
                <div className="flex flex-wrap gap-1">
                  {displayForms.slice(0, 6).map((form, i) => (
                    <button
                      key={i}
                      onClick={() => handleSuggestionClick(`What is ${form}?`)}
                      className="px-2 py-0.5 bg-blue-100 text-blue-700 rounded text-xs hover:bg-blue-200"
                    >
                      {form}
                    </button>
                  ))}
                </div>
              </div>

              {/* Programs */}
              <div>
                <p className="text-xs text-slate-500 font-medium mb-1">Programs</p>
                <div className="flex flex-wrap gap-1">
                  {displayPrograms.slice(0, 6).map((prog, i) => (
                    <button
                      key={i}
                      onClick={() => handleSuggestionClick(`Tell me about ${prog}`)}
                      className="px-2 py-0.5 bg-green-100 text-green-700 rounded text-xs hover:bg-green-200"
                    >
                      {prog}
                    </button>
                  ))}
                </div>
              </div>

              {/* Topics (only if API returns them) */}
              {availableEntities?.topics && availableEntities.topics.length > 0 && (
                <div>
                  <p className="text-xs text-slate-500 font-medium mb-1">Topics</p>
                  <div className="flex flex-wrap gap-1">
                    {availableEntities.topics.slice(0, 8).map((topic, i) => (
                      <button
                        key={i}
                        onClick={() => handleSuggestionClick(`What data do we have about ${topic}?`)}
                        className="px-2 py-0.5 bg-purple-100 text-purple-700 rounded text-xs hover:bg-purple-200"
                      >
                        {topic}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>

        </div>
      </div>
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

function ClaudeIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor">
      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 17.93c-3.95-.49-7-3.85-7-7.93 0-.62.08-1.21.21-1.79L9 15v1c0 1.1.9 2 2 2v1.93zm6.9-2.54c-.26-.81-1-1.39-1.9-1.39h-1v-3c0-.55-.45-1-1-1H8v-2h2c.55 0 1-.45 1-1V7h2c1.1 0 2-.9 2-2v-.41c2.93 1.19 5 4.06 5 7.41 0 2.08-.8 3.97-2.1 5.39z"/>
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

function TokenIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="10" />
      <path d="M12 6v12" />
      <path d="M8 10h8" />
      <path d="M8 14h8" />
    </svg>
  );
}

function DataIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
      <path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
    </svg>
  );
}

function SearchIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="11" r="8" />
      <line x1="21" y1="21" x2="16.65" y2="16.65" />
    </svg>
  );
}

function WarningIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
      <line x1="12" y1="9" x2="12" y2="13" />
      <line x1="12" y1="17" x2="12.01" y2="17" />
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
