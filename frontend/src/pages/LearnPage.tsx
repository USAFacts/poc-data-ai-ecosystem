import { useState, useRef, useEffect, useCallback } from 'react';
import { searchQuery } from '../api/client';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Choice {
  label: string;
  subtitle: string;
  query: string;
  icon: string;
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
}

// ---------------------------------------------------------------------------
// Follow-up generators
// ---------------------------------------------------------------------------

function getFollowUps(query: string): Choice[] {
  const q = query.toLowerCase();

  if (q.includes('visa') || q.includes('form')) {
    return [
      { label: 'Historical trends', subtitle: 'How has this changed over time?', query: `${query} historical trends`, icon: '📈' },
      { label: 'By country of origin', subtitle: 'Which countries are most affected?', query: `${query} breakdown by country`, icon: '🌍' },
      { label: 'Current backlogs', subtitle: 'What are the processing delays?', query: `${query} current backlog processing times`, icon: '⏳' },
    ];
  }

  if (q.includes('state') || /california|texas|new york|florida|illinois/i.test(q)) {
    const stateMatch = q.match(/\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\b/i);
    const state = stateMatch ? stateMatch[1] : 'this state';
    return [
      { label: 'National comparison', subtitle: `How does ${state} compare?`, query: `${state} immigration compared to national average`, icon: '📊' },
      { label: 'Top programs', subtitle: `Most used programs in ${state}`, query: `top immigration programs in ${state}`, icon: '🏆' },
      { label: '5-year trends', subtitle: `How things have changed`, query: `${state} immigration trends past 5 years`, icon: '📅' },
    ];
  }

  if (q.includes('agenc')) {
    return [
      { label: 'Processing volume', subtitle: 'What gets processed most?', query: `${query} most common processing`, icon: '📋' },
      { label: 'Efficiency metrics', subtitle: 'How fast is processing?', query: `${query} processing efficiency`, icon: '⚡' },
      { label: 'Policy changes', subtitle: 'What changed recently?', query: `${query} recent policy updates`, icon: '📜' },
    ];
  }

  return [
    { label: 'Category breakdown', subtitle: 'Break this down by type', query: `${query} breakdown by category`, icon: '📊' },
    { label: 'Trends & patterns', subtitle: 'What patterns emerge?', query: `${query} trends and patterns`, icon: '📈' },
    { label: 'Year-over-year', subtitle: 'Compare to previous years', query: `${query} comparison previous years`, icon: '🔄' },
  ];
}

// ---------------------------------------------------------------------------
// Starter choices (USAFacts-style suggestion cards)
// ---------------------------------------------------------------------------

const STARTER_CHOICES: Choice[] = [
  { label: 'Think broadly', subtitle: 'What is the main reason people immigrate to the US?', query: 'What are the main reasons people immigrate to the United States?', icon: '🌎' },
  { label: 'Explain it to me', subtitle: 'How has US immigration changed over time?', query: 'How has US immigration changed over time?', icon: '📊' },
  { label: 'Data behind the forms', subtitle: 'Which immigration forms have the largest backlogs?', query: 'Which immigration forms have the largest backlogs?', icon: '📋' },
  { label: 'State by state', subtitle: 'How does immigration differ across US states?', query: '__STATE_INPUT__', icon: '🗺️' },
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
  const [searchInput, setSearchInput] = useState('');
  const containerRef = useRef<HTMLDivElement>(null);

  // Smooth transition when index changes
  useEffect(() => {
    if (containerRef.current) {
      containerRef.current.style.transform = `translateX(-${currentIndex * 100}%)`;
    }
  }, [currentIndex]);

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

    try {
      const result = await searchQuery(choice.query, 'vg');
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
            }
          : s
      ));
    } catch {
      setSlides(prev => prev.map(s =>
        s.id === loadingSlide.id
          ? { ...s, isLoading: false, answer: 'Sorry, I couldn\'t fetch that information right now. Please try again.' }
          : s
      ));
    }
  }, [addSlideAndNavigate]);

  const handleStateSubmit = useCallback(async (state: string) => {
    if (!state.trim()) return;
    const query = `${state} immigration statistics and demographics`;
    handleChoice({ label: `Immigration in ${state}`, subtitle: '', query, icon: '🗺️' });
  }, [handleChoice]);

  const handleSearchSubmit = useCallback(async (q: string) => {
    if (!q.trim()) return;
    handleChoice({ label: q, subtitle: '', query: q, icon: '🔍' });
    setSearchInput('');
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
                    searchInput={searchInput}
                    onSearchChange={setSearchInput}
                    onSearchSubmit={handleSearchSubmit}
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

function WelcomeSlide({
  choices, onChoice, searchInput, onSearchChange, onSearchSubmit,
}: {
  choices: Choice[];
  onChoice: (c: Choice) => void;
  searchInput: string;
  onSearchChange: (v: string) => void;
  onSearchSubmit: (q: string) => void;
}) {
  return (
    <div className="flex flex-col items-center pt-12">
      {/* Greeting */}
      <h1 className="text-4xl font-bold text-[#4A7C59] mb-1">Hello</h1>
      <h2 className="text-3xl font-semibold text-slate-800 mb-8">What would you like to learn?</h2>

      {/* Search bar */}
      <form
        onSubmit={(e) => { e.preventDefault(); onSearchSubmit(searchInput); }}
        className="w-full max-w-xl mb-12"
      >
        <div className="flex items-center bg-white rounded-full border border-slate-200 shadow-sm px-5 py-3 focus-within:ring-2 focus-within:ring-[#4A7C59]/30 focus-within:border-[#4A7C59]/50 transition-all">
          <input
            type="text"
            value={searchInput}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder="Ask your question about immigration..."
            className="flex-1 text-sm text-slate-800 placeholder-slate-400 bg-transparent outline-none"
          />
          <button
            type="submit"
            disabled={!searchInput.trim()}
            className="w-8 h-8 rounded-full bg-[#4A7C59] text-white flex items-center justify-center hover:bg-[#3D6B4A] disabled:opacity-30 transition-colors flex-shrink-0 ml-2"
          >
            <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="22" y1="2" x2="11" y2="13" /><polygon points="22 2 15 22 11 13 2 9 22 2" /></svg>
          </button>
        </div>
      </form>

      {/* Suggestion cards — 2x2 grid */}
      <div className="grid grid-cols-2 gap-4 w-full max-w-xl">
        {choices.map((choice) => (
          <button
            key={choice.label}
            onClick={() => onChoice(choice)}
            className="text-left p-4 rounded-2xl bg-[#F0F5F1] border border-[#E0E8E2] hover:bg-[#E5EDE7] hover:border-[#C8D5CB] transition-all group"
          >
            <div className="flex items-start justify-between">
              <div className="flex-1">
                <p className="text-sm font-semibold text-slate-800">{choice.label}</p>
                <p className="text-xs text-slate-500 mt-1 leading-relaxed">{choice.subtitle}</p>
              </div>
              <span className="text-xl ml-3 opacity-60 group-hover:opacity-100 transition-opacity">{choice.icon}</span>
            </div>
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
    <div className="flex flex-col items-center pt-16">
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

function AnswerSlide({ slide, onChoice }: { slide: Slide; onChoice: (c: Choice) => void }) {
  if (slide.isLoading) {
    return (
      <div className="flex flex-col items-center pt-20">
        <div className="flex space-x-2 mb-4">
          <div className="w-3 h-3 rounded-full bg-[#4A7C59] animate-bounce" style={{ animationDelay: '0ms' }} />
          <div className="w-3 h-3 rounded-full bg-[#4A7C59] animate-bounce" style={{ animationDelay: '150ms' }} />
          <div className="w-3 h-3 rounded-full bg-[#4A7C59] animate-bounce" style={{ animationDelay: '300ms' }} />
        </div>
        <p className="text-sm text-slate-500">Searching documents and building your answer...</p>
      </div>
    );
  }

  return (
    <div>
      {/* Answer card */}
      <div className="bg-white rounded-2xl border border-slate-200 p-6 mb-6">
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

      {/* Follow-up choices */}
      {slide.choices && slide.choices.length > 0 && (
        <div>
          <p className="text-sm text-slate-500 mb-3">Continue exploring:</p>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {slide.choices.map((choice) => (
              <button
                key={choice.label}
                onClick={() => onChoice(choice)}
                className="text-left p-4 rounded-2xl bg-[#F0F5F1] border border-[#E0E8E2] hover:bg-[#E5EDE7] hover:border-[#C8D5CB] transition-all group"
              >
                <div className="flex items-start gap-2">
                  <span className="text-lg opacity-60 group-hover:opacity-100 transition-opacity">{choice.icon}</span>
                  <div>
                    <p className="text-sm font-medium text-slate-800">{choice.label}</p>
                    <p className="text-[11px] text-slate-500 mt-0.5">{choice.subtitle}</p>
                  </div>
                </div>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
