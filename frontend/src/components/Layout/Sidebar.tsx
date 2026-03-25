import { useState } from 'react';
import { NavLink } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { DashboardIcon, AgencyIcon, AssetIcon, ChevronIcon } from '../Icons';
import { fetchDashboardStats } from '../../api/client';

const navItems = [
  { path: '/', label: 'Executive Dashboard', Icon: DashboardIcon },
  { path: '/agencies', label: 'Agencies', Icon: AgencyIcon },
  { path: '/assets', label: 'Assets Report', Icon: AssetIcon },
  { path: '/architecture', label: 'Architecture', Icon: ArchitectureIcon },
  { path: '/weaviate', label: 'Weaviate', Icon: WeaviateIcon },
  { path: '/neo4j', label: 'Neo4j Graph', Icon: Neo4jIcon },
];

export default function Sidebar() {
  const [isIntelligenceExpanded, setIsIntelligenceExpanded] = useState(true);
  const [isInteractiveExpanded, setIsInteractiveExpanded] = useState(false);
  const [isLabExpanded, setIsLabExpanded] = useState(false);

  const { data: stats } = useQuery({
    queryKey: ['dashboardStats'],
    queryFn: fetchDashboardStats,
  });

  return (
    <aside className="w-60 bg-slate-900 text-white flex flex-col min-h-screen">
      <div className="p-6">
        <h1 className="text-lg font-bold">Gov Data Studio</h1>
      </div>

      {/* Collapsible Intelligence Platform Section */}
      <div className="mt-2">
        <button
          onClick={() => setIsIntelligenceExpanded(!isIntelligenceExpanded)}
          className="w-full flex items-center justify-between px-6 py-3 text-sm font-medium text-slate-300 hover:bg-slate-800 hover:text-white transition-colors"
        >
          <div className="flex items-center gap-2">
            <PlatformIcon className="w-[18px] h-[18px]" />
            <span>Intelligence Platform</span>
          </div>
          <ChevronIcon
            className={`w-4 h-4 transition-transform duration-200 ${isIntelligenceExpanded ? 'rotate-180' : ''}`}
          />
        </button>

        {/* Collapsible Content */}
        <div
          className={`overflow-hidden transition-all duration-300 ease-in-out ${
            isIntelligenceExpanded ? 'max-h-[600px] opacity-100' : 'max-h-0 opacity-0'
          }`}
        >
          {/* Pages */}
          <nav className="mt-1">
            {navItems.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-slate-800 hover:text-white transition-colors ${
                    isActive ? 'bg-blue-600 text-white' : ''
                  }`
                }
              >
                <item.Icon className="w-[16px] h-[16px]" />
                {item.label}
              </NavLink>
            ))}
          </nav>

          {/* Quick Stats */}
          {stats && (
            <div className="mt-4 px-6 pl-10 pb-2">
              <div className="mb-2">
                <span className="text-[10px] uppercase tracking-wider text-slate-500">Quick Stats</span>
              </div>
              <div className="text-sm space-y-2">
                <QuickStat
                  label="DIS Score"
                  value={stats.summary.overall_dis.toFixed(1)}
                  trend={stats.summary.dis_trend}
                />
                <QuickStat
                  label="Total Assets"
                  value={stats.summary.total_assets.toString()}
                />
                <QuickStat
                  label="Onboarded"
                  value={stats.summary.workflows_executed.toString()}
                />
                <QuickStat
                  label="Executed"
                  value={stats.summary.workflows_executed.toString()}
                  color="text-blue-400"
                />
                <QuickStat
                  label="Successful"
                  value={stats.summary.workflows_successful.toString()}
                  color="text-green-400"
                />
                <QuickStat
                  label="Success Rate"
                  value={`${stats.summary.success_rate.toFixed(0)}%`}
                  color="text-green-400"
                />
                <QuickStat
                  label="Eligible Coverage"
                  value={`${stats.summary.eligible_coverage.toFixed(0)}%`}
                  color="text-purple-400"
                />
                <QuickStat
                  label="Avg Quality"
                  value={stats.summary.avg_quality.toFixed(1)}
                  color="text-green-400"
                />
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Collapsible Interactive Platform Section */}
      <div className="mt-2">
        <button
          onClick={() => setIsInteractiveExpanded(!isInteractiveExpanded)}
          className="w-full flex items-center justify-between px-6 py-3 text-sm font-medium text-slate-300 hover:bg-slate-800 hover:text-white transition-colors"
        >
          <div className="flex items-center gap-2">
            <ChatIcon className="w-[18px] h-[18px]" />
            <span>Interactive Platform</span>
          </div>
          <ChevronIcon
            className={`w-4 h-4 transition-transform duration-200 ${isInteractiveExpanded ? 'rotate-180' : ''}`}
          />
        </button>

        {/* Collapsible Content */}
        <div
          className={`overflow-hidden transition-all duration-300 ease-in-out ${
            isInteractiveExpanded ? 'max-h-[200px] opacity-100' : 'max-h-0 opacity-0'
          }`}
        >
          <nav className="mt-1">
            <NavLink
              to="/chat"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-slate-800 hover:text-white transition-colors ${
                  isActive ? 'bg-blue-600 text-white' : ''
                }`
              }
            >
              <ChatBubbleIcon className="w-[16px] h-[16px]" />
              Q&A
            </NavLink>
          </nav>
        </div>
      </div>

      {/* Collapsible Lab Platform Section */}
      <div className="mt-2">
        <button
          onClick={() => setIsLabExpanded(!isLabExpanded)}
          className="w-full flex items-center justify-between px-6 py-3 text-sm font-medium text-slate-300 hover:bg-slate-800 hover:text-white transition-colors"
        >
          <div className="flex items-center gap-2">
            <LabIcon className="w-[18px] h-[18px]" />
            <span>Lab Platform</span>
          </div>
          <ChevronIcon
            className={`w-4 h-4 transition-transform duration-200 ${isLabExpanded ? 'rotate-180' : ''}`}
          />
        </button>

        {/* Collapsible Content */}
        <div
          className={`overflow-hidden transition-all duration-300 ease-in-out ${
            isLabExpanded ? 'max-h-[200px] opacity-100' : 'max-h-0 opacity-0'
          }`}
        >
          <nav className="mt-1">
            <NavLink
              to="/experiments"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-slate-800 hover:text-white transition-colors ${
                  isActive ? 'bg-blue-600 text-white' : ''
                }`
              }
            >
              <ExperimentIcon className="w-[16px] h-[16px]" />
              Experiment Tracker
            </NavLink>
          </nav>
        </div>
      </div>

      <div className="mt-auto p-6 border-t border-slate-800">
        <p className="text-slate-500 text-xs">Pipeline Version 0.1.0</p>
      </div>
    </aside>
  );
}

// Platform icon (grid/dashboard style)
function PlatformIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="7" height="7" rx="1" />
      <rect x="14" y="3" width="7" height="7" rx="1" />
      <rect x="3" y="14" width="7" height="7" rx="1" />
      <rect x="14" y="14" width="7" height="7" rx="1" />
    </svg>
  );
}

// Chat icon for Interactive Platform section header
function ChatIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
    </svg>
  );
}

// Chat bubble icon for Pipeline Assistant nav item
function ChatBubbleIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 11.5a8.38 8.38 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.38 8.38 0 0 1-3.8-.9L3 21l1.9-5.7a8.38 8.38 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.38 8.38 0 0 1 3.8-.9h.5a8.48 8.48 0 0 1 8 8v.5z" />
    </svg>
  );
}


// Architecture icon (flow/diagram)
function ArchitectureIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="6" height="4" rx="1" />
      <rect x="15" y="3" width="6" height="4" rx="1" />
      <rect x="9" y="17" width="6" height="4" rx="1" />
      <path d="M6 7v3a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2V7" />
      <path d="M12 12v5" />
    </svg>
  );
}

// Weaviate icon (database cylinder)
function WeaviateIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5" />
      <path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3" />
    </svg>
  );
}

// Neo4j icon (connected nodes/graph)
function Neo4jIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="6" cy="6" r="3" />
      <circle cx="18" cy="6" r="3" />
      <circle cx="6" cy="18" r="3" />
      <circle cx="18" cy="18" r="3" />
      <line x1="8.5" y1="7.5" x2="15.5" y2="16.5" />
      <line x1="15.5" y1="7.5" x2="8.5" y2="16.5" />
      <line x1="6" y1="9" x2="6" y2="15" />
      <line x1="18" y1="9" x2="18" y2="15" />
    </svg>
  );
}

// Lab icon (beaker/flask)
function LabIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M9 3h6" />
      <path d="M10 3v7.4a2 2 0 0 1-.5 1.3L4 18.6a1 1 0 0 0 .7 1.7h14.6a1 1 0 0 0 .7-1.7l-5.5-6.9a2 2 0 0 1-.5-1.3V3" />
    </svg>
  );
}

// Experiment icon (test tube)
function ExperimentIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14.5 2v6.5a2 2 0 0 0 .5 1.3l4.5 5.6a2 2 0 0 1-1.6 3.1H6.1a2 2 0 0 1-1.6-3.1l4.5-5.6a2 2 0 0 0 .5-1.3V2" />
      <path d="M8.5 2h7" />
      <path d="M7 16.5h10" />
    </svg>
  );
}

interface QuickStatProps {
  label: string;
  value: string;
  color?: string;
  trend?: number;
}

function QuickStat({ label, value, color, trend }: QuickStatProps) {
  return (
    <div className="flex justify-between">
      <span className="text-slate-500">{label}</span>
      <span className={`font-semibold ${color || 'text-white'}`}>
        {value}
        {trend !== undefined && Math.abs(trend) >= 0.1 && (
          <span className={`ml-1 text-xs ${trend > 0 ? 'text-green-400' : 'text-red-400'}`}>
            {trend > 0 ? '↑' : '↓'} {trend > 0 ? '+' : ''}{trend.toFixed(1)}
          </span>
        )}
      </span>
    </div>
  );
}
