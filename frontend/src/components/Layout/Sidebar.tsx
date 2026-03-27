import { useState } from 'react';
import { NavLink } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { DashboardIcon, AgencyIcon, AssetIcon, ChevronIcon } from '../Icons';
import { fetchDashboardStats } from '../../api/client';

interface SidebarProps {
  isCollapsed: boolean;
  onToggleCollapse: () => void;
}

const navItems = [
  { path: '/', label: 'Executive Dashboard', Icon: DashboardIcon },
  { path: '/agencies', label: 'Agencies', Icon: AgencyIcon },
  { path: '/assets', label: 'Assets Report', Icon: AssetIcon },
  { path: '/architecture', label: 'Architecture', Icon: ArchitectureIcon },
  { path: '/weaviate', label: 'Weaviate', Icon: WeaviateIcon },
  { path: '/neo4j', label: 'Neo4j Graph', Icon: Neo4jIcon },
];

export default function Sidebar({ isCollapsed, onToggleCollapse }: SidebarProps) {
  const [isIntelligenceExpanded, setIsIntelligenceExpanded] = useState(true);
  const [isInteractiveExpanded, setIsInteractiveExpanded] = useState(false);
  const [isLabExpanded, setIsLabExpanded] = useState(false);
  const [isConnectorsExpanded, setIsConnectorsExpanded] = useState(false);

  const { data: stats } = useQuery({
    queryKey: ['dashboardStats'],
    queryFn: fetchDashboardStats,
  });

  return (
    <aside
      className={`${isCollapsed ? 'w-[60px]' : 'w-60'} bg-[#1B2A4A] text-white flex flex-col min-h-screen transition-all duration-300 ease-in-out overflow-hidden`}
    >
      {/* Header with toggle */}
      <div className="flex items-center justify-between p-4">
        <div className={`flex items-center gap-2 ${isCollapsed ? 'hidden' : ''}`}>
          <UsaFactsLogo className="w-6 h-6 flex-shrink-0" />
          <h1 className="text-lg font-bold whitespace-nowrap">Studio</h1>
        </div>
        {isCollapsed && <UsaFactsLogo className="w-5 h-5 flex-shrink-0 mx-auto" />}
        <button
          onClick={onToggleCollapse}
          className="p-1.5 rounded-md hover:bg-white/[0.08] transition-colors flex-shrink-0"
          title={isCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          <svg
            className={`w-4 h-4 text-slate-300 transition-transform duration-300 ${isCollapsed ? 'rotate-180' : ''}`}
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <polyline points="15 18 9 12 15 6" />
          </svg>
        </button>
      </div>

      {/* Collapsible Intelligence Platform Section */}
      <div className="mt-2">
        <button
          onClick={() => !isCollapsed && setIsIntelligenceExpanded(!isIntelligenceExpanded)}
          className={`w-full flex items-center ${isCollapsed ? 'justify-center px-0' : 'justify-between px-6'} py-3 text-sm font-medium text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors`}
          title={isCollapsed ? 'Intelligence Platform' : undefined}
        >
          <div className={`flex items-center ${isCollapsed ? '' : 'gap-2'}`}>
            <PlatformIcon className="w-[18px] h-[18px] flex-shrink-0" />
            {!isCollapsed && <span className="whitespace-nowrap">Intelligence Platform</span>}
          </div>
          {!isCollapsed && (
            <ChevronIcon
              className={`w-4 h-4 transition-transform duration-200 ${isIntelligenceExpanded ? 'rotate-180' : ''}`}
            />
          )}
        </button>

        {/* Collapsible Content */}
        <div
          className={`overflow-hidden transition-all duration-300 ease-in-out ${
            isIntelligenceExpanded && !isCollapsed ? 'max-h-[600px] opacity-100' : 'max-h-0 opacity-0'
          }`}
        >
          {/* Pages */}
          <nav className="mt-1">
            {navItems.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                className={({ isActive }) =>
                  `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                    isActive ? 'bg-[#0A3161] text-white' : ''
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

        {/* Collapsed nav icons for Intelligence section */}
        {isCollapsed && (
          <nav className="flex flex-col items-center">
            {navItems.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                title={item.label}
                className={({ isActive }) =>
                  `flex items-center justify-center w-full py-2.5 text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                    isActive ? 'bg-[#0A3161] text-white' : ''
                  }`
                }
              >
                <item.Icon className="w-[16px] h-[16px]" />
              </NavLink>
            ))}
          </nav>
        )}
      </div>

      {/* Collapsible Interactive Platform Section */}
      <div className="mt-2">
        <button
          onClick={() => !isCollapsed && setIsInteractiveExpanded(!isInteractiveExpanded)}
          className={`w-full flex items-center ${isCollapsed ? 'justify-center px-0' : 'justify-between px-6'} py-3 text-sm font-medium text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors`}
          title={isCollapsed ? 'Interactive Platform' : undefined}
        >
          <div className={`flex items-center ${isCollapsed ? '' : 'gap-2'}`}>
            <ChatIcon className="w-[18px] h-[18px] flex-shrink-0" />
            {!isCollapsed && <span className="whitespace-nowrap">Interactive Platform</span>}
          </div>
          {!isCollapsed && (
            <ChevronIcon
              className={`w-4 h-4 transition-transform duration-200 ${isInteractiveExpanded ? 'rotate-180' : ''}`}
            />
          )}
        </button>

        {/* Expanded content */}
        <div
          className={`overflow-hidden transition-all duration-300 ease-in-out ${
            isInteractiveExpanded && !isCollapsed ? 'max-h-[200px] opacity-100' : 'max-h-0 opacity-0'
          }`}
        >
          <nav className="mt-1">
            <NavLink
              to="/chat"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <ChatBubbleIcon className="w-[16px] h-[16px]" />
              Q&A
            </NavLink>
            <NavLink
              to="/catalog"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <CatalogIcon className="w-[16px] h-[16px]" />
              Catalog
            </NavLink>
            <NavLink
              to="/learn"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <LearnIcon className="w-[16px] h-[16px]" />
              Learn
            </NavLink>
          </nav>
        </div>

        {/* Collapsed icons for Interactive section */}
        {isCollapsed && (
          <nav className="flex flex-col items-center">
            <NavLink
              to="/chat"
              title="Q&A"
              className={({ isActive }) =>
                `flex items-center justify-center w-full py-2.5 text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <ChatBubbleIcon className="w-[16px] h-[16px]" />
            </NavLink>
            <NavLink
              to="/catalog"
              title="Catalog"
              className={({ isActive }) =>
                `flex items-center justify-center w-full py-2.5 text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <CatalogIcon className="w-[16px] h-[16px]" />
            </NavLink>
            <NavLink
              to="/learn"
              title="Learn"
              className={({ isActive }) =>
                `flex items-center justify-center w-full py-2.5 text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <LearnIcon className="w-[16px] h-[16px]" />
            </NavLink>
          </nav>
        )}
      </div>

      {/* Collapsible Lab Platform Section */}
      <div className="mt-2">
        <button
          onClick={() => !isCollapsed && setIsLabExpanded(!isLabExpanded)}
          className={`w-full flex items-center ${isCollapsed ? 'justify-center px-0' : 'justify-between px-6'} py-3 text-sm font-medium text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors`}
          title={isCollapsed ? 'Lab Platform' : undefined}
        >
          <div className={`flex items-center ${isCollapsed ? '' : 'gap-2'}`}>
            <LabIcon className="w-[18px] h-[18px] flex-shrink-0" />
            {!isCollapsed && <span className="whitespace-nowrap">Lab Platform</span>}
          </div>
          {!isCollapsed && (
            <ChevronIcon
              className={`w-4 h-4 transition-transform duration-200 ${isLabExpanded ? 'rotate-180' : ''}`}
            />
          )}
        </button>

        {/* Expanded content */}
        <div
          className={`overflow-hidden transition-all duration-300 ease-in-out ${
            isLabExpanded && !isCollapsed ? 'max-h-[200px] opacity-100' : 'max-h-0 opacity-0'
          }`}
        >
          <nav className="mt-1">
            <NavLink
              to="/experiments"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <ExperimentIcon className="w-[16px] h-[16px]" />
              Experiment Tracker
            </NavLink>
          </nav>
        </div>

        {/* Collapsed icons for Lab section */}
        {isCollapsed && (
          <nav className="flex flex-col items-center">
            <NavLink
              to="/experiments"
              title="Experiment Tracker"
              className={({ isActive }) =>
                `flex items-center justify-center w-full py-2.5 text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <ExperimentIcon className="w-[16px] h-[16px]" />
            </NavLink>
          </nav>
        )}
      </div>

      {/* Collapsible Connectors Section */}
      <div className="mt-2">
        <button
          onClick={() => !isCollapsed && setIsConnectorsExpanded(!isConnectorsExpanded)}
          className={`w-full flex items-center ${isCollapsed ? 'justify-center px-0' : 'justify-between px-6'} py-3 text-sm font-medium text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors`}
          title={isCollapsed ? 'Connectors' : undefined}
        >
          <div className={`flex items-center ${isCollapsed ? '' : 'gap-2'}`}>
            <ConnectorIcon className="w-[18px] h-[18px] flex-shrink-0" />
            {!isCollapsed && <span className="whitespace-nowrap">Connectors</span>}
          </div>
          {!isCollapsed && (
            <ChevronIcon
              className={`w-4 h-4 transition-transform duration-200 ${isConnectorsExpanded ? 'rotate-180' : ''}`}
            />
          )}
        </button>

        {/* Expanded content */}
        <div
          className={`overflow-hidden transition-all duration-300 ease-in-out ${
            isConnectorsExpanded && !isCollapsed ? 'max-h-[300px] opacity-100' : 'max-h-0 opacity-0'
          }`}
        >
          <nav className="mt-1">
            <NavLink
              to="/swagger"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <SwaggerIcon className="w-[16px] h-[16px]" />
              Swagger UI
            </NavLink>
            <NavLink
              to="/redoc"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <DocIcon className="w-[16px] h-[16px]" />
              ReDoc
            </NavLink>
            <NavLink
              to="/openapi"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <ApiIcon className="w-[16px] h-[16px]" />
              OpenAPI Spec
            </NavLink>
            <NavLink
              to="/mcp"
              className={({ isActive }) =>
                `flex items-center gap-3 px-6 pl-10 py-2.5 text-sm text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <McpIcon className="w-[16px] h-[16px]" />
              MCP Inspector
            </NavLink>
          </nav>
        </div>

        {/* Collapsed icons for Connectors section */}
        {isCollapsed && (
          <nav className="flex flex-col items-center">
            <NavLink
              to="/swagger"
              title="Swagger UI"
              className={({ isActive }) =>
                `flex items-center justify-center w-full py-2.5 text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <SwaggerIcon className="w-[16px] h-[16px]" />
            </NavLink>
            <NavLink
              to="/redoc"
              title="ReDoc"
              className={({ isActive }) =>
                `flex items-center justify-center w-full py-2.5 text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <DocIcon className="w-[16px] h-[16px]" />
            </NavLink>
            <NavLink
              to="/openapi"
              title="OpenAPI Spec"
              className={({ isActive }) =>
                `flex items-center justify-center w-full py-2.5 text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <ApiIcon className="w-[16px] h-[16px]" />
            </NavLink>
            <NavLink
              to="/mcp"
              title="MCP Inspector"
              className={({ isActive }) =>
                `flex items-center justify-center w-full py-2.5 text-slate-300 hover:bg-white/[0.08] hover:text-white transition-colors ${
                  isActive ? 'bg-[#0A3161] text-white' : ''
                }`
              }
            >
              <McpIcon className="w-[16px] h-[16px]" />
            </NavLink>
          </nav>
        )}
      </div>

      <div className={`mt-auto p-4 border-t border-[#142038] ${isCollapsed ? 'text-center' : ''}`}>
        {!isCollapsed && <p className="text-slate-500 text-xs">Pipeline Version 0.1.0</p>}
        {isCollapsed && <p className="text-slate-500 text-[10px]">v0.1</p>}
      </div>
    </aside>
  );
}

// USAFacts logo — wavy lines forming US map silhouette
function UsaFactsLogo({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 32 32" fill="none">
      <path d="M4 8 C8 6, 12 7, 16 8 C20 9, 24 7, 28 8" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" />
      <path d="M3 13 C7 11, 11 13, 16 13 C21 13, 25 11, 29 13" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" />
      <path d="M5 18 C9 16, 13 18, 17 18 C21 18, 25 16, 28 18" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" />
      <path d="M6 23 C10 21, 14 23, 18 23 C22 23, 25 21, 27 23" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" />
    </svg>
  );
}

// Catalog icon (search/magnifying glass over document)
function CatalogIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="11" r="8" />
      <line x1="21" y1="21" x2="16.65" y2="16.65" />
      <line x1="8" y1="8" x2="14" y2="8" />
      <line x1="8" y1="11" x2="14" y2="11" />
    </svg>
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


// Learn icon (open book)
function LearnIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z" />
      <path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z" />
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

// Connector icon (plug)
function ConnectorIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 2v6" /><path d="M6 2v6" /><path d="M18 2v6" />
      <path d="M6 8h12a2 2 0 0 1 2 2v2a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2v-2a2 2 0 0 1 2-2z" />
      <path d="M12 14v8" />
    </svg>
  );
}

function SwaggerIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M4 6h16" /><path d="M4 12h16" /><path d="M4 18h10" />
      <circle cx="19" cy="18" r="2" />
    </svg>
  );
}

function DocIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <polyline points="14 2 14 8 20 8" />
      <line x1="16" y1="13" x2="8" y2="13" /><line x1="16" y1="17" x2="8" y2="17" />
    </svg>
  );
}

function ApiIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="16 18 22 12 16 6" /><polyline points="8 6 2 12 8 18" />
      <line x1="14" y1="4" x2="10" y2="20" />
    </svg>
  );
}

function McpIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="2" y="3" width="20" height="14" rx="2" />
      <line x1="8" y1="21" x2="16" y2="21" /><line x1="12" y1="17" x2="12" y2="21" />
      <path d="M7 9l3 3-3 3" /><line x1="13" y1="15" x2="17" y2="15" />
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
            {trend > 0 ? '\u2191' : '\u2193'} {trend > 0 ? '+' : ''}{trend.toFixed(1)}
          </span>
        )}
      </span>
    </div>
  );
}
