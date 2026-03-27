import { useState, useRef, useEffect, useCallback } from 'react';
import { searchQuery, type SearchResult, type DocumentReference } from '../api/client';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Choice {
  label: string;
  query: string;
  icon: string;
}

interface Stage {
  id: string;
  type: 'welcome' | 'choices' | 'input' | 'answer' | 'summary';
  title: string;
  subtitle?: string;
  choices?: Choice[];
  inputPlaceholder?: string;
  answer?: string;
  sources?: { title: string; agency: string; url?: string | null }[];
  isLoading?: boolean;
}

// ---------------------------------------------------------------------------
// Follow-up question templates
// ---------------------------------------------------------------------------

function getFollowUps(query: string, _answer: string): Choice[] {
  const q = query.toLowerCase();

  if (q.includes('visa') || q.includes('form')) {
    return [
      { label: 'How has this changed over time?', query: `${query} historical trends over time`, icon: '\u{1F4C8}' },
      { label: 'Which countries are most affected?', query: `${query} breakdown by country of origin`, icon: '\u{1F30D}' },
      { label: 'What is the current backlog?', query: `${query} current backlog and processing times`, icon: '\u{23F3}' },
    ];
  }

  if (q.includes('state') || q.includes('california') || q.includes('texas') || q.includes('new york') || q.includes('florida')) {
    const stateMatch = q.match(/(\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\b/i);
    const state = stateMatch ? stateMatch[1] : 'this state';
    return [
      { label: `How does ${state} compare nationally?`, query: `${state} immigration statistics compared to national average`, icon: '\u{1F4CA}' },
      { label: `Top immigration programs in ${state}`, query: `What are the top immigration programs in ${state}?`, icon: '\u{1F3C6}' },
      { label: `${state} immigration trends over 5 years`, query: `How has ${state} immigration changed over the past 5 years?`, icon: '\u{1F4C5}' },
    ];
  }

  if (q.includes('agenc')) {
    return [
      { label: 'What does this agency process most?', query: `${query} most common processing categories`, icon: '\u{1F4CB}' },
      { label: 'How efficient is the processing?', query: `${query} processing efficiency and timelines`, icon: '\u{26A1}' },
      { label: 'What are the recent policy changes?', query: `${query} recent policy updates and changes`, icon: '\u{1F4DC}' },
    ];
  }

  // Default / statistics follow-ups
  return [
    { label: 'Break this down by category', query: `${query} breakdown by category`, icon: '\u{1F4CA}' },
    { label: 'What trends do you see?', query: `${query} trends and patterns`, icon: '\u{1F4C8}' },
    { label: 'How does this compare to previous years?', query: `${query} comparison with previous years`, icon: '\u{1F504}' },
  ];
}

// ---------------------------------------------------------------------------
// Welcome stage
// ---------------------------------------------------------------------------

const WELCOME_STAGE: Stage = {
  id: 'welcome',
  type: 'welcome',
  title: 'Welcome to the Immigration Learning Guide',
  subtitle:
    'Explore immigration data through guided questions. We\u2019ll help you discover patterns, statistics, and context about immigration in the United States.',
  choices: [
    { label: 'How does immigration work in the US?', query: 'How does the US immigration system work?', icon: '\u{1F3DB}\uFE0F' },
    { label: 'What are the latest immigration statistics?', query: 'What are the key immigration statistics for FY2025?', icon: '\u{1F4CA}' },
    { label: 'How does immigration affect my state?', query: '__STATE_INPUT__', icon: '\u{1F5FA}\uFE0F' },
  ],
};

// ---------------------------------------------------------------------------
// Path-specific second-stage choices
// ---------------------------------------------------------------------------

const OVERVIEW_CHOICES: Choice[] = [
  { label: 'Tell me about visa categories', query: 'What are the different US visa categories and types?', icon: '\u{1F4C4}' },
  { label: 'What agencies handle immigration?', query: 'Which government agencies handle US immigration?', icon: '\u{1F3E2}' },
  { label: 'What forms are most common?', query: 'What are the most common USCIS immigration forms?', icon: '\u{1F4DD}' },
];

const STATISTICS_CHOICES: Choice[] = [
  { label: 'Backlogs and processing times', query: 'What are the current immigration backlogs and processing times?', icon: '\u{23F3}' },
  { label: 'Visa approvals and denials', query: 'What are the visa approval and denial rates?', icon: '\u{2705}' },
  { label: 'Refugee and asylum programs', query: 'What are the current refugee and asylum program statistics?', icon: '\u{1F91D}' },
];

// ---------------------------------------------------------------------------
// Markdown rendering (mirrors ChatbotPage patterns)
// ---------------------------------------------------------------------------

function FormattedText({ text }: { text: string }) {
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

function MarkdownContent({ content }: { content: string }) {
  const lines = content.split('\n');
  const elements: JSX.Element[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];

    // Table
    if (line.trim().startsWith('|')) {
      const tableLines: string[] = [];
      while (i < lines.length && lines[i].trim().startsWith('|')) {
        tableLines.push(lines[i]);
        i++;
      }
      if (tableLines.length >= 2) {
        const parseRow = (l: string) =>
          l.split('|').map((c) => c.trim()).filter((_, idx, arr) => idx > 0 && idx < arr.length - 1);
        const headers = parseRow(tableLines[0]);
        const rows = tableLines.slice(2).map(parseRow);
        elements.push(
          <div key={`table-${i}`} className="my-3 overflow-x-auto">
            <table className="min-w-full text-sm border-collapse border border-slate-200">
              <thead>
                <tr className="bg-slate-100">
                  {headers.map((h, idx) => (
                    <th key={idx} className="px-3 py-2 text-left font-semibold border border-slate-200 text-slate-700">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, rIdx) => (
                  <tr key={rIdx} className={rIdx % 2 === 0 ? 'bg-white' : 'bg-slate-50'}>
                    {row.map((cell, cIdx) => (
                      <td key={cIdx} className="px-3 py-1.5 border border-slate-200">
                        <FormattedText text={cell} />
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>,
        );
      }
      continue;
    }

    // Blockquote
    if (line.startsWith('>')) {
      elements.push(
        <div key={i} className="border-l-2 border-slate-300 pl-3 my-2 italic text-slate-600">
          <FormattedText text={line.slice(1).trim()} />
        </div>,
      );
      i++;
      continue;
    }

    // Headers
    if (line.startsWith('#')) {
      const level = line.match(/^#+/)?.[0].length || 1;
      const headerText = line.replace(/^#+\s*/, '');
      const cls = level === 1 ? 'text-lg font-bold' : level === 2 ? 'text-base font-semibold' : 'text-sm font-medium';
      elements.push(
        <div key={i} className={`${cls} mt-3 mb-1 text-slate-800`}>
          <FormattedText text={headerText} />
        </div>,
      );
      i++;
      continue;
    }

    // Bullet list
    if (line.match(/^\s*[-*+]\s/)) {
      const items: string[] = [];
      while (i < lines.length && lines[i].match(/^\s*[-*+]\s/)) {
        items.push(lines[i].replace(/^\s*[-*+]\s/, ''));
        i++;
      }
      elements.push(
        <ul key={`ul-${i}`} className="list-disc list-inside my-2 space-y-1 text-slate-700">
          {items.map((item, idx) => (
            <li key={idx}><FormattedText text={item} /></li>
          ))}
        </ul>,
      );
      continue;
    }

    // Numbered list
    if (line.match(/^\s*\d+\.\s/)) {
      const items: string[] = [];
      while (i < lines.length && lines[i].match(/^\s*\d+\.\s/)) {
        items.push(lines[i].replace(/^\s*\d+\.\s/, ''));
        i++;
      }
      elements.push(
        <ol key={`ol-${i}`} className="list-decimal list-inside my-2 space-y-1 text-slate-700">
          {items.map((item, idx) => (
            <li key={idx}><FormattedText text={item} /></li>
          ))}
        </ol>,
      );
      continue;
    }

    // Regular text
    if (line.trim()) {
      elements.push(
        <p key={i} className="my-1 text-slate-700 leading-relaxed">
          <FormattedText text={line} />
        </p>,
      );
    } else if (i > 0 && i < lines.length - 1) {
      elements.push(<div key={i} className="h-2" />);
    }
    i++;
  }

  return <>{elements}</>;
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function ProgressDots({ current, total }: { current: number; total: number }) {
  return (
    <div className="flex items-center gap-1.5">
      {Array.from({ length: total }).map((_, idx) => (
        <div
          key={idx}
          className={`h-2 rounded-full transition-all duration-300 ${
            idx < current ? 'w-2 bg-blue-500' : idx === current ? 'w-6 bg-blue-600' : 'w-2 bg-slate-300'
          }`}
        />
      ))}
      <span className="ml-2 text-xs text-slate-400 font-medium">
        Step {current + 1} of {total}
      </span>
    </div>
  );
}

function LoadingPulse() {
  return (
    <div className="flex flex-col items-center py-8 gap-4">
      <div className="flex gap-1.5">
        <div className="w-2.5 h-2.5 bg-blue-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
        <div className="w-2.5 h-2.5 bg-blue-500 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
        <div className="w-2.5 h-2.5 bg-blue-600 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
      </div>
      <p className="text-sm text-slate-500">Searching immigration data...</p>
    </div>
  );
}

function SourcePills({ sources }: { sources: Stage['sources'] }) {
  if (!sources || sources.length === 0) return null;
  return (
    <div className="mt-4 pt-3 border-t border-slate-100">
      <p className="text-xs text-slate-400 font-medium mb-2">Sources</p>
      <div className="flex flex-wrap gap-2">
        {sources.map((src, idx) => (
          <span
            key={idx}
            className="inline-flex items-center gap-1 px-2.5 py-1 text-xs bg-slate-100 text-slate-600 rounded-full border border-slate-200"
            title={src.title}
          >
            <span className="w-1.5 h-1.5 bg-blue-400 rounded-full flex-shrink-0" />
            <span className="truncate max-w-[200px]">{src.title}</span>
            <span className="text-slate-400">|</span>
            <span className="text-slate-500 truncate max-w-[120px]">{src.agency}</span>
          </span>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Stage Cards
// ---------------------------------------------------------------------------

function WelcomeCard({ stage, onChoice }: { stage: Stage; onChoice: (choice: Choice) => void }) {
  return (
    <div className="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-sm">
      {/* Gradient header */}
      <div className="bg-gradient-to-r from-[#1B2A4A] to-[#2563EB] px-8 py-10 text-center">
        <div className="text-4xl mb-4">{'\u{1F4DA}'}</div>
        <h1 className="text-2xl font-bold text-white mb-2">{stage.title}</h1>
        <p className="text-blue-200 text-sm max-w-lg mx-auto leading-relaxed">{stage.subtitle}</p>
      </div>

      {/* Choice cards */}
      <div className="p-6">
        <p className="text-sm text-slate-500 font-medium mb-4 text-center">Choose a starting topic</p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {stage.choices?.map((choice) => (
            <button
              key={choice.label}
              onClick={() => onChoice(choice)}
              className="group flex flex-col items-center gap-3 p-6 rounded-xl border-2 border-slate-200 bg-white hover:border-blue-400 hover:shadow-md transition-all duration-200 text-center"
            >
              <span className="text-3xl group-hover:scale-110 transition-transform duration-200">{choice.icon}</span>
              <span className="text-sm font-medium text-slate-700 group-hover:text-blue-700 transition-colors">{choice.label}</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function ChoicesCard({ stage, onChoice }: { stage: Stage; onChoice: (choice: Choice) => void }) {
  return (
    <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
      <h2 className="text-lg font-semibold text-slate-800 mb-1">{stage.title}</h2>
      {stage.subtitle && <p className="text-sm text-slate-500 mb-5">{stage.subtitle}</p>}
      <div className={`grid gap-3 ${(stage.choices?.length || 0) > 2 ? 'grid-cols-1 md:grid-cols-3' : 'grid-cols-1 md:grid-cols-2'}`}>
        {stage.choices?.map((choice) => (
          <button
            key={choice.label}
            onClick={() => onChoice(choice)}
            className="group flex items-center gap-3 p-4 rounded-lg border-2 border-slate-200 bg-white hover:border-blue-400 hover:shadow-md transition-all duration-200 text-left"
          >
            <span className="text-2xl flex-shrink-0 group-hover:scale-110 transition-transform">{choice.icon}</span>
            <span className="text-sm font-medium text-slate-700 group-hover:text-blue-700 transition-colors">{choice.label}</span>
          </button>
        ))}
      </div>
    </div>
  );
}

function InputCard({
  stage,
  onSubmit,
}: {
  stage: Stage;
  onSubmit: (value: string) => void;
}) {
  const [value, setValue] = useState('');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (value.trim()) onSubmit(value.trim());
  };

  return (
    <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
      <h2 className="text-lg font-semibold text-slate-800 mb-1">{stage.title}</h2>
      {stage.subtitle && <p className="text-sm text-slate-500 mb-5">{stage.subtitle}</p>}
      <form onSubmit={handleSubmit} className="flex gap-3">
        <input
          type="text"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          placeholder={stage.inputPlaceholder || 'Type here...'}
          className="flex-1 px-4 py-3 rounded-lg border-2 border-slate-200 text-sm focus:outline-none focus:border-blue-400 transition-colors"
        />
        <button
          type="submit"
          disabled={!value.trim()}
          className="px-6 py-3 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-colors"
        >
          Explore
        </button>
      </form>
    </div>
  );
}

function AnswerCard({ stage }: { stage: Stage }) {
  return (
    <div className="bg-white rounded-xl border border-slate-200 p-6 shadow-sm">
      <div className="flex items-start gap-3 mb-4">
        <div className="w-8 h-8 rounded-full bg-gradient-to-br from-blue-500 to-blue-700 flex items-center justify-center flex-shrink-0">
          <svg className="w-4 h-4 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z" />
            <path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z" />
          </svg>
        </div>
        <h3 className="text-base font-semibold text-slate-800 pt-1">{stage.title}</h3>
      </div>

      {stage.isLoading ? (
        <LoadingPulse />
      ) : stage.answer ? (
        <div className="pl-11">
          <div className="text-sm leading-relaxed">
            <MarkdownContent content={stage.answer} />
          </div>
          <SourcePills sources={stage.sources} />
        </div>
      ) : null}
    </div>
  );
}

function SummaryCard({ stage, onRestart }: { stage: Stage; onRestart: () => void }) {
  return (
    <div className="bg-white rounded-xl border-2 border-blue-200 overflow-hidden shadow-sm">
      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 px-6 py-5 border-b border-blue-100">
        <div className="flex items-center gap-3">
          <span className="text-2xl">{'\u{1F393}'}</span>
          <div>
            <h2 className="text-lg font-bold text-slate-800">{stage.title}</h2>
            {stage.subtitle && <p className="text-sm text-slate-500">{stage.subtitle}</p>}
          </div>
        </div>
      </div>
      <div className="p-6">
        {stage.answer && (
          <div className="text-sm leading-relaxed text-slate-700 mb-6">
            <MarkdownContent content={stage.answer} />
          </div>
        )}
        <button
          onClick={onRestart}
          className="inline-flex items-center gap-2 px-5 py-2.5 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors"
        >
          <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="1 4 1 10 7 10" />
            <path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10" />
          </svg>
          Start a New Exploration
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

const MAX_DEEP_DIVES = 4;

export default function LearnPage() {
  const [stages, setStages] = useState<Stage[]>([WELCOME_STAGE]);
  const [isLoading, setIsLoading] = useState(false);
  const [currentPath, setCurrentPath] = useState('');
  const [deepDiveCount, setDeepDiveCount] = useState(0);
  const bottomRef = useRef<HTMLDivElement>(null);

  // Scroll to bottom on new stages
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [stages]);

  const extractSources = (docs: DocumentReference[]): Stage['sources'] => {
    return docs.slice(0, 5).map((d) => ({
      title: d.document_title || d.asset_name,
      agency: d.agency_name,
      url: d.source_url,
    }));
  };

  const addStage = useCallback((stage: Stage) => {
    setStages((prev) => [...prev, stage]);
  }, []);

  const handleSearch = useCallback(
    async (query: string, answerTitle: string, nextChoices?: Choice[], path?: string) => {
      if (isLoading) return;
      setIsLoading(true);

      // Add loading answer stage
      const answerId = `answer-${Date.now()}`;
      addStage({
        id: answerId,
        type: 'answer',
        title: answerTitle,
        isLoading: true,
      });

      try {
        const result: SearchResult = await searchQuery(query, 'vg');
        const sources = extractSources(result.documents);

        // Update loading stage with answer
        setStages((prev) => {
          const copy = [...prev];
          const idx = copy.findIndex((s) => s.id === answerId);
          if (idx >= 0) {
            copy[idx] = { ...copy[idx], answer: result.answer, sources, isLoading: false };
          }
          return copy;
        });

        const newCount = deepDiveCount + 1;
        setDeepDiveCount(newCount);

        if (path) setCurrentPath(path);

        // Check if we should show summary
        if (newCount >= MAX_DEEP_DIVES) {
          // Build summary
          const answerStages = [...stages, { id: answerId, type: 'answer' as const, title: answerTitle, answer: result.answer }]
            .filter((s) => s.type === 'answer' && s.answer);
          const bulletPoints = answerStages.map((s) => `- **${s.title}**`).join('\n');

          addStage({
            id: `summary-${Date.now()}`,
            type: 'summary',
            title: 'Exploration Complete',
            subtitle: `You explored ${newCount} topics in this session.`,
            answer: `Here's a recap of what you explored:\n\n${bulletPoints}\n\nFeel free to start a new exploration to dive into different areas of immigration data.`,
          });
        } else if (nextChoices) {
          addStage({
            id: `choices-${Date.now()}`,
            type: 'choices',
            title: 'What would you like to explore next?',
            subtitle: 'Choose a topic to dive deeper.',
            choices: nextChoices,
          });
        } else {
          // Generate contextual follow-ups
          const followUps = getFollowUps(query, result.answer);
          addStage({
            id: `choices-${Date.now()}`,
            type: 'choices',
            title: 'Continue exploring',
            subtitle: 'Select a follow-up question or go deeper.',
            choices: followUps,
          });
        }
      } catch {
        setStages((prev) => {
          const copy = [...prev];
          const idx = copy.findIndex((s) => s.id === answerId);
          if (idx >= 0) {
            copy[idx] = {
              ...copy[idx],
              answer: 'We encountered an error while fetching data. Please make sure the backend is running and try again.',
              isLoading: false,
            };
          }
          return copy;
        });
      } finally {
        setIsLoading(false);
      }
    },
    [isLoading, addStage, deepDiveCount, stages],
  );

  const handleWelcomeChoice = (choice: Choice) => {
    if (choice.query === '__STATE_INPUT__') {
      // Show state input
      addStage({
        id: 'state-input',
        type: 'input',
        title: 'Explore immigration in your state',
        subtitle: 'Enter a US state to see immigration statistics and data specific to that area.',
        inputPlaceholder: 'e.g., California, Texas, New York',
      });
      setCurrentPath('state');
    } else if (choice.query.includes('system work')) {
      handleSearch(choice.query, 'How the US Immigration System Works', OVERVIEW_CHOICES, 'overview');
    } else {
      handleSearch(choice.query, 'Key Immigration Statistics', STATISTICS_CHOICES, 'statistics');
    }
  };

  const handleChoice = (choice: Choice) => {
    handleSearch(choice.query, choice.label);
  };

  const handleStateSubmit = (state: string) => {
    const query = `${state} immigration statistics and data`;
    const stateFollowUps: Choice[] = [
      { label: `How does ${state} compare nationally?`, query: `${state} immigration compared to national average`, icon: '\u{1F4CA}' },
      { label: `Top programs in ${state}`, query: `What are the top immigration programs in ${state}?`, icon: '\u{1F3C6}' },
      { label: `${state} trends over 5 years`, query: `How has ${state} immigration changed over the past 5 years?`, icon: '\u{1F4C5}' },
    ];
    handleSearch(query, `Immigration Data for ${state}`, stateFollowUps, 'state');
  };

  const handleRestart = () => {
    setStages([WELCOME_STAGE]);
    setDeepDiveCount(0);
    setCurrentPath('');
    setIsLoading(false);
  };

  // Calculate progress for the dots
  const totalSteps = MAX_DEEP_DIVES + 2; // welcome + dives + summary
  const currentStep = Math.min(deepDiveCount + 1, totalSteps);

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Header bar */}
      <div className="sticky top-0 z-10 bg-white/80 backdrop-blur-md border-b border-slate-200 px-6 py-3">
        <div className="max-w-3xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-[#1B2A4A] to-[#2563EB] flex items-center justify-center">
              <svg className="w-4 h-4 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z" />
                <path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z" />
              </svg>
            </div>
            <h1 className="text-base font-semibold text-slate-800">Immigration Learning Guide</h1>
            {currentPath && (
              <span className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded-full font-medium capitalize">
                {currentPath}
              </span>
            )}
          </div>
          <ProgressDots current={currentStep} total={totalSteps} />
        </div>
      </div>

      {/* Stages timeline */}
      <div className="max-w-3xl mx-auto px-6 py-8 space-y-6">
        {stages.map((stage) => {
          switch (stage.type) {
            case 'welcome':
              return <WelcomeCard key={stage.id} stage={stage} onChoice={handleWelcomeChoice} />;
            case 'choices':
              return <ChoicesCard key={stage.id} stage={stage} onChoice={handleChoice} />;
            case 'input':
              return <InputCard key={stage.id} stage={stage} onSubmit={handleStateSubmit} />;
            case 'answer':
              return <AnswerCard key={stage.id} stage={stage} />;
            case 'summary':
              return <SummaryCard key={stage.id} stage={stage} onRestart={handleRestart} />;
            default:
              return null;
          }
        })}

        {/* Scroll anchor */}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
