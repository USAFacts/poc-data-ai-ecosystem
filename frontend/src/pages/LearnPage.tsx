import { useState, useRef, useEffect, useCallback } from 'react';
import { searchQuery, type TraceStep, type AnswerMetrics } from '../api/client';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface SparkPoint {
  label: string;
  value: number;
}

interface Choice {
  label: string;
  subtitle: string;
  query: string;
  icon: string;
  spark?: SparkPoint[];
}

interface Slide {
  id: string;
  type: 'welcome' | 'answer' | 'input';
  title?: string;
  subtitle?: string;
  answer?: string;
  sources?: { title: string; agency: string; url?: string | null }[];
  choices?: Choice[];
  isLoading?: boolean;
  inputPlaceholder?: string;
  trace?: TraceStep[];
  metrics?: AnswerMetrics | null;
}

// ---------------------------------------------------------------------------
// SVG Icons (dashboard-style: stroke, currentColor, round caps)
// ---------------------------------------------------------------------------

function CardIcon({ name, className = 'w-5 h-5' }: { name: string; className?: string }) {
  const props = { className, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.75, strokeLinecap: 'round' as const, strokeLinejoin: 'round' as const };

  switch (name) {
    case 'globe':
      return <svg {...props}><circle cx="12" cy="12" r="10"/><path d="M2 12h20"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>;
    case 'building':
      return <svg {...props}><path d="M6 22V4a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v18Z"/><path d="M6 12H4a2 2 0 0 0-2 2v6a2 2 0 0 0 2 2h2"/><path d="M18 9h2a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-2"/><path d="M10 6h4"/><path d="M10 10h4"/><path d="M10 14h4"/><path d="M10 18h4"/></svg>;
    case 'trend-up':
      return <svg {...props}><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>;
    case 'clock':
      return <svg {...props}><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>;
    case 'map':
      return <svg {...props}><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>;
    case 'shield':
      return <svg {...props}><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>;
    case 'briefcase':
      return <svg {...props}><rect x="2" y="7" width="20" height="14" rx="2" ry="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/></svg>;
    case 'users':
      return <svg {...props}><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>;
    case 'bar-chart':
      return <svg {...props}><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>;
    case 'pie-chart':
      return <svg {...props}><path d="M21.21 15.89A10 10 0 1 1 8 2.83"/><path d="M22 12A10 10 0 0 0 12 2v10z"/></svg>;
    case 'calendar':
      return <svg {...props}><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>;
    case 'zap':
      return <svg {...props}><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>;
    case 'file-text':
      return <svg {...props}><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>;
    case 'refresh':
      return <svg {...props}><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10"/><path d="M20.49 15a9 9 0 0 1-14.85 3.36L1 14"/></svg>;
    case 'award':
      return <svg {...props}><circle cx="12" cy="8" r="7"/><polyline points="8.21 13.89 7 23 12 20 17 23 15.79 13.88"/></svg>;
    default:
      return <svg {...props}><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>;
  }
}

// ---------------------------------------------------------------------------
// Mini sparkline / bar chart for cards
// ---------------------------------------------------------------------------

function Sparkline({ data, className = '' }: { data: SparkPoint[]; className?: string }) {
  if (!data || data.length < 2) return null;
  const max = Math.max(...data.map(d => d.value));
  const min = Math.min(...data.map(d => d.value));
  const range = max - min || 1;
  const w = 80;
  const h = 28;
  const pad = 2;
  const points = data.map((d, i) => {
    const x = pad + (i / (data.length - 1)) * (w - 2 * pad);
    const y = h - pad - ((d.value - min) / range) * (h - 2 * pad);
    return `${x},${y}`;
  }).join(' ');

  return (
    <svg className={className} viewBox={`0 0 ${w} ${h}`} fill="none" preserveAspectRatio="none">
      <polyline points={points} stroke="#3B82F6" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" fill="none" />
      <polyline points={`${pad},${h} ${points} ${w - pad},${h}`} fill="#3B82F6" fillOpacity="0.08" stroke="none" />
    </svg>
  );
}

function SparkBars({ data, className = '' }: { data: SparkPoint[]; className?: string }) {
  if (!data || data.length === 0) return null;
  const max = Math.max(...data.map(d => d.value));
  const w = 80;
  const h = 28;
  const gap = 2;
  const barW = (w - gap * (data.length - 1)) / data.length;

  return (
    <svg className={className} viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none">
      {data.map((d, i) => {
        const barH = max > 0 ? (d.value / max) * (h - 4) : 0;
        return (
          <rect
            key={i}
            x={i * (barW + gap)}
            y={h - barH - 2}
            width={barW}
            height={barH}
            rx={1.5}
            fill="#3B82F6"
            fillOpacity={0.7 + (i / data.length) * 0.3}
          />
        );
      })}
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Follow-up generators
// ---------------------------------------------------------------------------

function getFollowUps(query: string): Choice[] {
  const q = query.toLowerCase();

  if (q.includes('visa') || q.includes('form')) {
    return [
      { label: 'Historical trends', subtitle: 'How has this changed over time?', query: `${query} historical trends`, icon: 'trend-up', spark: [{ label: '19', value: 620 }, { label: '20', value: 480 }, { label: '21', value: 540 }, { label: '22', value: 710 }, { label: '23', value: 780 }] },
      { label: 'By country of origin', subtitle: 'Which countries are most affected?', query: `${query} breakdown by country`, icon: 'globe' },
      { label: 'Current backlogs', subtitle: 'What are the processing delays?', query: `${query} current backlog processing times`, icon: 'clock', spark: [{ label: 'I-130', value: 24 }, { label: 'I-485', value: 18 }, { label: 'I-765', value: 8 }, { label: 'N-400', value: 12 }] },
    ];
  }

  if (q.includes('state') || /california|texas|new york|florida|illinois/i.test(q)) {
    const stateMatch = q.match(/\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\b/i);
    const state = stateMatch ? stateMatch[1] : 'this state';
    return [
      { label: 'National comparison', subtitle: `How does ${state} compare?`, query: `${state} immigration compared to national average`, icon: 'bar-chart' },
      { label: 'Top programs', subtitle: `Most used programs in ${state}`, query: `top immigration programs in ${state}`, icon: 'award' },
      { label: '5-year trends', subtitle: `How things have changed`, query: `${state} immigration trends past 5 years`, icon: 'trend-up', spark: [{ label: '19', value: 100 }, { label: '20', value: 85 }, { label: '21', value: 92 }, { label: '22', value: 110 }, { label: '23', value: 118 }] },
    ];
  }

  if (q.includes('agenc')) {
    return [
      { label: 'Processing volume', subtitle: 'What gets processed most?', query: `${query} most common processing`, icon: 'file-text' },
      { label: 'Efficiency metrics', subtitle: 'How fast is processing?', query: `${query} processing efficiency`, icon: 'zap' },
      { label: 'Policy changes', subtitle: 'What changed recently?', query: `${query} recent policy updates`, icon: 'file-text' },
    ];
  }

  return [
    { label: 'Category breakdown', subtitle: 'Break this down by type', query: `${query} breakdown by category`, icon: 'pie-chart' },
    { label: 'Trends & patterns', subtitle: 'What patterns emerge?', query: `${query} trends and patterns`, icon: 'trend-up' },
    { label: 'Year-over-year', subtitle: 'Compare to previous years', query: `${query} comparison previous years`, icon: 'refresh' },
  ];
}

// ---------------------------------------------------------------------------
// Starter choices (USAFacts-style suggestion cards)
// ---------------------------------------------------------------------------

const STARTER_CHOICES: Choice[] = [
  {
    label: 'Why do people immigrate?', subtitle: 'The main reasons people come to the US',
    query: 'What are the main reasons people immigrate to the United States?', icon: 'globe',
    spark: [{ label: 'Family', value: 65 }, { label: 'Employment', value: 14 }, { label: 'Refugee', value: 12 }, { label: 'Diversity', value: 5 }, { label: 'Other', value: 4 }],
  },
  {
    label: 'How the system works', subtitle: 'Visa types, green cards, and pathways',
    query: 'How does the US immigration system work? What are the main pathways?', icon: 'building',
  },
  {
    label: 'Immigration over time', subtitle: 'How has US immigration changed?',
    query: 'How has US immigration changed over the past 10 years?', icon: 'trend-up',
    spark: [{ label: '14', value: 1.02 }, { label: '15', value: 1.05 }, { label: '16', value: 1.18 }, { label: '17', value: 1.13 }, { label: '18', value: 1.1 }, { label: '19', value: 1.03 }, { label: '20', value: 0.71 }, { label: '21', value: 0.74 }, { label: '22', value: 1.01 }, { label: '23', value: 1.15 }],
  },
  {
    label: 'The backlog problem', subtitle: 'Which forms have the longest waits?',
    query: 'Which immigration forms have the largest backlogs and longest processing times?', icon: 'clock',
    spark: [{ label: 'I-130', value: 24 }, { label: 'I-485', value: 18 }, { label: 'I-140', value: 14 }, { label: 'N-400', value: 10 }, { label: 'I-765', value: 7 }],
  },
  {
    label: 'State by state', subtitle: 'How does immigration affect your state?',
    query: '__STATE_INPUT__', icon: 'map',
  },
  {
    label: 'Refugee & asylum', subtitle: 'Programs for those seeking protection',
    query: 'What are the current refugee and asylum statistics in the United States?', icon: 'users',
    spark: [{ label: '19', value: 30 }, { label: '20', value: 12 }, { label: '21', value: 11 }, { label: '22', value: 25 }, { label: '23', value: 60 }],
  },
  {
    label: 'Employment visas', subtitle: 'H-1B, H-2B, and work permits',
    query: 'What are the key statistics for H-1B and other employment-based visas?', icon: 'briefcase',
    spark: [{ label: 'H-1B', value: 386 }, { label: 'H-2A', value: 310 }, { label: 'H-2B', value: 130 }, { label: 'L-1', value: 72 }, { label: 'O-1', value: 18 }],
  },
  {
    label: 'DACA & TPS', subtitle: 'Temporary protection programs',
    query: 'What is the current status of DACA and TPS programs?', icon: 'shield',
    spark: [{ label: '18', value: 690 }, { label: '19', value: 650 }, { label: '20', value: 640 }, { label: '21', value: 616 }, { label: '22', value: 594 }, { label: '23', value: 580 }],
  },
];

// ---------------------------------------------------------------------------
// Markdown rendering
// ---------------------------------------------------------------------------

function FormattedText({ text }: { text: string }) {
  const parts = text.split(/(\*\*.*?\*\*|\*.*?\*|`.*?`)/g);
  return (
    <>
      {parts.map((part, i) => {
        if (part.startsWith('**') && part.endsWith('**')) return <strong key={i}>{part.slice(2, -2)}</strong>;
        if (part.startsWith('*') && part.endsWith('*') && !part.startsWith('**')) return <em key={i}>{part.slice(1, -1)}</em>;
        if (part.startsWith('`') && part.endsWith('`')) return <code key={i} className="px-1 py-0.5 bg-slate-200 text-slate-800 rounded text-xs font-mono">{part.slice(1, -1)}</code>;
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

    if (line.trim().startsWith('|')) {
      const tableLines: string[] = [];
      while (i < lines.length && lines[i].trim().startsWith('|')) { tableLines.push(lines[i]); i++; }
      if (tableLines.length >= 2) {
        const parseRow = (l: string) => l.split('|').map(c => c.trim()).filter((_, idx, arr) => idx > 0 && idx < arr.length - 1);
        const headers = parseRow(tableLines[0]);
        const rows = tableLines.slice(2).map(parseRow);
        elements.push(
          <div key={`t-${i}`} className="my-3 overflow-x-auto">
            <table className="min-w-full text-sm border-collapse border border-slate-200">
              <thead><tr className="bg-slate-100">{headers.map((h, idx) => <th key={idx} className="px-3 py-2 text-left font-semibold border border-slate-200">{h}</th>)}</tr></thead>
              <tbody>{rows.map((row, rIdx) => <tr key={rIdx} className={rIdx % 2 === 0 ? 'bg-white' : 'bg-slate-50'}>{row.map((cell, cIdx) => <td key={cIdx} className="px-3 py-1.5 border border-slate-200"><FormattedText text={cell} /></td>)}</tr>)}</tbody>
            </table>
          </div>
        );
      }
      continue;
    }
    if (line.startsWith('>')) { elements.push(<div key={i} className="border-l-2 border-slate-300 pl-3 my-2 italic text-slate-600"><FormattedText text={line.slice(1).trim()} /></div>); i++; continue; }
    if (line.startsWith('#')) {
      const level = line.match(/^#+/)?.[0].length || 1;
      const text = line.replace(/^#+\s*/, '');
      const cls = level === 1 ? 'text-lg font-bold' : level === 2 ? 'text-base font-semibold' : 'text-sm font-medium';
      elements.push(<div key={i} className={`${cls} mt-3 mb-1 text-slate-800`}><FormattedText text={text} /></div>);
      i++; continue;
    }
    if (line.match(/^\s*[-*+]\s/)) {
      const items: string[] = [];
      while (i < lines.length && lines[i].match(/^\s*[-*+]\s/)) { items.push(lines[i].replace(/^\s*[-*+]\s/, '')); i++; }
      elements.push(<ul key={`ul-${i}`} className="list-disc list-inside my-2 space-y-1 text-slate-700">{items.map((item, idx) => <li key={idx}><FormattedText text={item} /></li>)}</ul>);
      continue;
    }
    if (line.match(/^\s*\d+\.\s/)) {
      const items: string[] = [];
      while (i < lines.length && lines[i].match(/^\s*\d+\.\s/)) { items.push(lines[i].replace(/^\s*\d+\.\s/, '')); i++; }
      elements.push(<ol key={`ol-${i}`} className="list-decimal list-inside my-2 space-y-1 text-slate-700">{items.map((item, idx) => <li key={idx}><FormattedText text={item} /></li>)}</ol>);
      continue;
    }
    if (line.trim()) elements.push(<p key={i} className="my-1 text-slate-700 leading-relaxed"><FormattedText text={line} /></p>);
    else if (i > 0) elements.push(<div key={i} className="h-2" />);
    i++;
  }
  return <>{elements}</>;
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function LearnPage() {
  const [slides, setSlides] = useState<Slide[]>([
    { id: 'welcome', type: 'welcome', choices: STARTER_CHOICES },
  ]);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [stateInput, setStateInput] = useState('');
  const [liveTraceStep, setLiveTraceStep] = useState(-1);
  const containerRef = useRef<HTMLDivElement>(null);

  // Smooth transition when index changes
  useEffect(() => {
    if (containerRef.current) {
      // Each slide is (100 / slides.length)% of the container width.
      // To show slide N, shift by N * (100 / slides.length)%.
      const pct = (currentIndex * 100) / slides.length;
      containerRef.current.style.transform = `translateX(-${pct}%)`;
    }
  }, [currentIndex, slides.length]);

  const addSlideAndNavigate = useCallback((slide: Slide) => {
    setSlides(prev => {
      // Remove any slides after current index (branching)
      const trimmed = prev.slice(0, currentIndex + 1);
      return [...trimmed, slide];
    });
    setCurrentIndex(prev => prev + 1);
  }, [currentIndex]);

  const handleChoice = useCallback(async (choice: Choice) => {
    if (choice.query === '__STATE_INPUT__') {
      addSlideAndNavigate({
        id: `input-${Date.now()}`,
        type: 'input',
        title: 'Where do you live?',
        subtitle: 'Tell us your state to see local immigration data',
        inputPlaceholder: 'e.g., California, Texas, New York...',
      });
      return;
    }

    // Add loading slide
    const loadingSlide: Slide = {
      id: `answer-${Date.now()}`,
      type: 'answer',
      title: choice.label,
      isLoading: true,
    };
    addSlideAndNavigate(loadingSlide);
    setLiveTraceStep(0);

    // Animate through stages while waiting
    const stageTimers = [
      setTimeout(() => setLiveTraceStep(1), 1500),
      setTimeout(() => setLiveTraceStep(2), 3500),
      setTimeout(() => setLiveTraceStep(3), 5500),
    ];

    try {
      const result = await searchQuery(choice.query, 'vgw');
      stageTimers.forEach(clearTimeout);
      const followUps = getFollowUps(choice.query);

      setSlides(prev => prev.map(s =>
        s.id === loadingSlide.id
          ? {
              ...s,
              isLoading: false,
              answer: result.answer,
              sources: result.documents?.map(d => ({
                title: d.document_title || d.asset_name,
                agency: d.agency_name,
                url: d.source_url,
              })) || [],
              choices: followUps,
              trace: result.trace,
              metrics: result.metrics,
            }
          : s
      ));
    } catch {
      stageTimers.forEach(clearTimeout);
      setSlides(prev => prev.map(s =>
        s.id === loadingSlide.id
          ? { ...s, isLoading: false, answer: 'Sorry, I couldn\'t fetch that information right now. Please try again.' }
          : s
      ));
    } finally {
      setLiveTraceStep(-1);
    }
  }, [addSlideAndNavigate]);

  const handleStateSubmit = useCallback(async (state: string) => {
    if (!state.trim()) return;
    const query = `${state} immigration statistics and demographics`;
    handleChoice({ label: `Immigration in ${state}`, subtitle: '', query, icon: 'map' });
  }, [handleChoice]);

  const goBack = useCallback(() => {
    if (currentIndex > 0) setCurrentIndex(prev => prev - 1);
  }, [currentIndex]);

  const goHome = useCallback(() => {
    setSlides([{ id: 'welcome', type: 'welcome', choices: STARTER_CHOICES }]);
    setCurrentIndex(0);
  }, []);

  return (
    <div className="h-[calc(100vh-64px)] flex flex-col overflow-hidden">
      {/* Navigation bar */}
      {currentIndex > 0 && (
        <div className="flex items-center gap-3 px-6 py-3 border-b border-slate-200 bg-white flex-shrink-0">
          <button onClick={goBack} className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-800 transition-colors">
            <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="15 18 9 12 15 6" /></svg>
            Back
          </button>
          <div className="flex gap-1.5 ml-4">
            {slides.map((_, idx) => (
              <button
                key={idx}
                onClick={() => setCurrentIndex(idx)}
                className={`w-2 h-2 rounded-full transition-colors ${idx === currentIndex ? 'bg-[#0A3161]' : idx < currentIndex ? 'bg-[#0A3161]/40' : 'bg-slate-200'}`}
              />
            ))}
          </div>
          <button onClick={goHome} className="ml-auto text-xs text-slate-400 hover:text-slate-600 transition-colors">
            Start over
          </button>
        </div>
      )}

      {/* Horizontal slide container */}
      <div className="flex-1 overflow-hidden relative">
        <div
          ref={containerRef}
          className="flex h-full transition-transform duration-500 ease-in-out"
          style={{ width: `${slides.length * 100}%` }}
        >
          {slides.map((slide) => (
            <div
              key={slide.id}
              className="h-full overflow-y-auto"
              style={{ width: `${100 / slides.length}%` }}
            >
              <div className="max-w-3xl mx-auto px-6 py-8">
                {slide.type === 'welcome' && (
                  <WelcomeSlide
                    choices={slide.choices || []}
                    onChoice={handleChoice}
                  />
                )}
                {slide.type === 'input' && (
                  <InputSlide
                    title={slide.title || ''}
                    subtitle={slide.subtitle || ''}
                    placeholder={slide.inputPlaceholder || ''}
                    value={stateInput}
                    onChange={setStateInput}
                    onSubmit={() => { handleStateSubmit(stateInput); setStateInput(''); }}
                  />
                )}
                {slide.type === 'answer' && (
                  <AnswerSlide
                    slide={slide}
                    onChoice={handleChoice}
                    liveTraceStep={slide.isLoading ? liveTraceStep : -1}
                  />
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Slide components
// ---------------------------------------------------------------------------

function WelcomeSlide({ choices, onChoice }: { choices: Choice[]; onChoice: (c: Choice) => void }) {
  return (
    <div className="flex flex-col items-center pt-8">
      <h1 className="text-4xl font-bold text-[#4A7C59] mb-1">Learn</h1>
      <h2 className="text-2xl font-semibold text-slate-800 mb-2">Explore immigration in the US</h2>
      <p className="text-sm text-slate-500 mb-10">Choose a topic to start your guided learning journey</p>

      {/* Topic cards — 2x4 grid */}
      <div className="grid grid-cols-2 gap-4 w-full max-w-2xl">
        {choices.map((choice) => (
          <button
            key={choice.label}
            onClick={() => onChoice(choice)}
            className="text-left p-4 rounded-xl bg-white border border-slate-200 hover:border-slate-300 hover:shadow-sm transition-all group"
          >
            <div className="flex items-start gap-3">
              <div className="w-8 h-8 rounded-lg bg-slate-100 group-hover:bg-[#0A3161]/10 flex items-center justify-center flex-shrink-0 transition-colors text-slate-500 group-hover:text-[#0A3161]">
                <CardIcon name={choice.icon} className="w-4 h-4" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-semibold text-slate-800">{choice.label}</p>
                <p className="text-xs text-slate-500 mt-0.5 leading-relaxed">{choice.subtitle}</p>
              </div>
            </div>
            {choice.spark && (
              <div className="mt-2.5 ml-11">
                {choice.icon === 'clock' || choice.icon === 'briefcase' || (choice.icon === 'globe' && choice.spark.length <= 6)
                  ? <SparkBars data={choice.spark} className="w-full h-5 opacity-60 group-hover:opacity-100 transition-opacity" />
                  : <Sparkline data={choice.spark} className="w-full h-5 opacity-60 group-hover:opacity-100 transition-opacity" />
                }
                <div className="flex justify-between mt-0.5">
                  <span className="text-[9px] text-slate-400">{choice.spark[0].label}</span>
                  <span className="text-[9px] text-slate-400">{choice.spark[choice.spark.length - 1].label}</span>
                </div>
              </div>
            )}
          </button>
        ))}
      </div>
    </div>
  );
}

function InputSlide({
  title, subtitle, placeholder, value, onChange, onSubmit,
}: {
  title: string; subtitle: string; placeholder: string;
  value: string; onChange: (v: string) => void; onSubmit: () => void;
}) {
  return (
    <div className="flex flex-col items-center pt-8">
      <h1 className="text-4xl font-bold text-[#4A7C59] mb-1">Learn</h1>
      <h2 className="text-2xl font-semibold text-slate-800 mb-2">{title}</h2>
      <p className="text-sm text-slate-500 mb-8">{subtitle}</p>

      <form onSubmit={(e) => { e.preventDefault(); onSubmit(); }} className="w-full max-w-md">
        <div className="flex items-center bg-white rounded-full border border-slate-200 shadow-sm px-5 py-3 focus-within:ring-2 focus-within:ring-[#4A7C59]/30 transition-all">
          <input
            type="text"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            className="flex-1 text-sm text-slate-800 placeholder-slate-400 bg-transparent outline-none"
            autoFocus
          />
          <button
            type="submit"
            disabled={!value.trim()}
            className="w-8 h-8 rounded-full bg-[#4A7C59] text-white flex items-center justify-center hover:bg-[#3D6B4A] disabled:opacity-30 transition-colors flex-shrink-0 ml-2"
          >
            <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="9 18 15 12 9 6" /></svg>
          </button>
        </div>
      </form>
    </div>
  );
}

function AnswerSlide({ slide, onChoice, liveTraceStep }: { slide: Slide; onChoice: (c: Choice) => void; liveTraceStep: number }) {
  if (slide.isLoading) {
    return (
      <div className="pt-8">
        {slide.title && (
          <h2 className="text-lg font-semibold text-slate-800 mb-5">{slide.title}</h2>
        )}
        <LearnLiveTrace currentStep={liveTraceStep} />
      </div>
    );
  }

  return (
    <div>
      {/* Completed reasoning trace */}
      {slide.trace && slide.trace.length > 0 && (
        <LearnCompletedTrace steps={slide.trace} />
      )}

      {/* Answer card */}
      <div className="bg-white rounded-2xl border border-slate-200 p-6 mb-4">
        {slide.title && (
          <h2 className="text-lg font-semibold text-slate-800 mb-4">{slide.title}</h2>
        )}

        {slide.answer && (
          <div className="text-sm leading-relaxed">
            <MarkdownContent content={slide.answer} />
          </div>
        )}

        {/* Sources */}
        {slide.sources && slide.sources.length > 0 && (
          <div className="mt-4 pt-3 border-t border-slate-100">
            <p className="text-[10px] uppercase tracking-wider text-slate-400 font-medium mb-2">Sources</p>
            <div className="flex flex-wrap gap-2">
              {slide.sources.map((src, i) => (
                <span key={i} className="inline-flex items-center gap-1 px-2.5 py-1 bg-slate-50 text-slate-600 rounded-full text-[11px] border border-slate-200">
                  {src.url ? (
                    <a href={src.url} target="_blank" rel="noopener noreferrer" className="hover:text-blue-600">{src.title || src.agency}</a>
                  ) : (
                    <>{src.title || src.agency}</>
                  )}
                  <span className="text-slate-400">· {src.agency}</span>
                </span>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Quality metrics */}
      {slide.metrics && <LearnMetricsPanel metrics={slide.metrics} />}

      {/* Follow-up choices */}
      {slide.choices && slide.choices.length > 0 && (
        <div className="mt-6">
          <p className="text-sm text-slate-500 mb-3">Continue exploring:</p>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {slide.choices.map((choice) => (
              <button
                key={choice.label}
                onClick={() => onChoice(choice)}
                className="text-left p-3.5 rounded-xl bg-white border border-slate-200 hover:border-slate-300 hover:shadow-sm transition-all group"
              >
                <div className="flex items-start gap-2.5">
                  <div className="w-7 h-7 rounded-lg bg-slate-100 group-hover:bg-[#0A3161]/10 flex items-center justify-center flex-shrink-0 transition-colors text-slate-500 group-hover:text-[#0A3161]">
                    <CardIcon name={choice.icon} className="w-3.5 h-3.5" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-slate-800">{choice.label}</p>
                    <p className="text-[11px] text-slate-500 mt-0.5">{choice.subtitle}</p>
                  </div>
                </div>
                {choice.spark && (
                  <div className="mt-2 ml-9">
                    {choice.icon === 'clock' || choice.icon === 'bar-chart'
                      ? <SparkBars data={choice.spark} className="w-full h-4 opacity-50 group-hover:opacity-100 transition-opacity" />
                      : <Sparkline data={choice.spark} className="w-full h-4 opacity-50 group-hover:opacity-100 transition-opacity" />
                    }
                  </div>
                )}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Learn-specific Reasoning Trace (adapted stages for guided learning flow)
// ---------------------------------------------------------------------------

const LEARN_LIVE_STAGES = [
  { stage: 'Topic Analysis', description: 'Understanding your topic and formulating search queries...' },
  { stage: 'Document Retrieval', description: 'Searching all sources: knowledge base, graph, and web...' },
  { stage: 'Context Assembly', description: 'Loading full documents and extracting relevant sections...' },
  { stage: 'Answer Synthesis', description: 'Claude is composing an educational answer from sources...' },
];

function LearnLiveTrace({ currentStep }: { currentStep: number }) {
  return (
    <div className="mb-4">
      <div className="space-y-0">
        {LEARN_LIVE_STAGES.map((stage, i) => {
          const isActive = i === currentStep;
          const isDone = i < currentStep;
          const isPending = i > currentStep;

          return (
            <div key={i} className="flex items-start gap-2.5 py-1.5">
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
                {i < LEARN_LIVE_STAGES.length - 1 && (
                  <div className={`w-0.5 h-5 ${isDone ? 'bg-green-300' : 'bg-slate-200'}`} />
                )}
              </div>
              <div className={`flex-1 ${isPending ? 'opacity-40' : ''}`}>
                <p className={`text-xs font-medium ${isActive ? 'text-blue-700' : isDone ? 'text-green-700' : 'text-slate-400'}`}>
                  {stage.stage}
                  {i === 0 && <span className="font-normal text-slate-400 ml-1">(All Sources)</span>}
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

function LearnCompletedTrace({ steps }: { steps: TraceStep[] }) {
  const [expanded, setExpanded] = useState(false);
  const totalMs = steps.reduce((sum, s) => sum + s.duration_ms, 0);
  const totalStr = totalMs < 1000 ? `${totalMs}ms` : `${(totalMs / 1000).toFixed(1)}s`;

  return (
    <div className="mb-3">
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
        <div className="mt-2 space-y-0 ml-1">
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

// ---------------------------------------------------------------------------
// Answer Quality Metrics (STS, NVS, HDS, CSCS)
// ---------------------------------------------------------------------------

function LearnMetricsPanel({ metrics }: { metrics: AnswerMetrics }) {
  const items = [
    { label: 'Source Traceability', abbr: 'STS', value: metrics.sts, threshold: 1.0, format: 'pct' as const, higherBetter: true },
    { label: 'Numerical Verification', abbr: 'NVS', value: metrics.nvs, threshold: 0.95, format: 'pct' as const, higherBetter: true },
    { label: 'Hallucination Flags', abbr: 'HDS', value: metrics.hds, threshold: 0, format: 'int' as const, higherBetter: false },
    { label: 'Cross-Store Consistency', abbr: 'CSCS', value: metrics.cscs, threshold: 0.95, format: 'pct' as const, higherBetter: true },
  ];

  return (
    <div className="flex flex-wrap gap-2 mb-4">
      {items.map((item) => {
        const passes = item.higherBetter
          ? item.value >= item.threshold
          : item.value <= item.threshold;
        const color = passes ? 'text-green-600' : item.value > (item.higherBetter ? 0.7 : 2) ? 'text-amber-500' : 'text-red-500';
        const displayValue = item.format === 'pct'
          ? `${(item.value * 100).toFixed(0)}%`
          : item.value.toString();

        return (
          <div
            key={item.abbr}
            className="flex items-center gap-1.5 px-2.5 py-1.5 bg-slate-50 rounded-lg border border-slate-200"
            title={`${item.label}: ${displayValue} (threshold: ${item.format === 'pct' ? (item.threshold * 100) + '%' : item.threshold})`}
          >
            <span className="text-[10px] font-bold text-slate-400 uppercase">{item.abbr}</span>
            <span className={`text-xs font-semibold ${color}`}>{displayValue}</span>
            <span className={`w-1.5 h-1.5 rounded-full ${passes ? 'bg-green-500' : 'bg-amber-500'}`} />
          </div>
        );
      })}
    </div>
  );
}
