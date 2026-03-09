import { useState } from 'react';
import Sidebar from './components/Sidebar';
import DashboardContent from './components/DashboardContent';
import AgentPanel from './components/AgentPanel';
import MetricDefDetailModal from './components/MetricDefDetailModal';
import { useDashboardStore } from './dashboardStore';
import { PanelRightOpen, PanelLeftOpen, PanelLeftClose } from 'lucide-react';
import type { MetricDef } from './types';

export default function App() {
  const activeDashboardId = useDashboardStore(s => s.activeDashboardId);
  const [agentOpen, setAgentOpen] = useState(true);
  const [dashboardOpen, setDashboardOpen] = useState(true);
  const [viewingMetric, setViewingMetric] = useState<MetricDef | null>(null);

  return (
    <div className="h-screen flex bg-slate-50 overflow-hidden">
      <Sidebar onViewMetric={setViewingMetric} />

      {/* Dashboard 区域 */}
      {activeDashboardId && dashboardOpen && (
        <main className="flex-1 min-w-0 border-r border-slate-200 relative">
          <DashboardContent />
          {/* 收起 Dashboard 按钮 */}
          <button
            onClick={() => setDashboardOpen(false)}
            className="absolute left-3 top-3 flex items-center gap-1.5 px-3 py-2 text-xs font-medium text-slate-500 bg-white border border-slate-200 rounded-lg shadow-sm hover:bg-slate-50 transition-colors cursor-pointer z-10"
            title="收起 Dashboard"
          >
            <PanelLeftClose className="w-3.5 h-3.5" />
          </button>
          {/* 收起 Agent 状态下的展开按钮 */}
          {!agentOpen && (
            <button
              onClick={() => setAgentOpen(true)}
              className="absolute right-3 top-3 flex items-center gap-1.5 px-3 py-2 text-xs font-medium text-indigo-600 bg-white border border-slate-200 rounded-lg shadow-sm hover:bg-indigo-50 transition-colors cursor-pointer z-10"
            >
              <PanelRightOpen className="w-3.5 h-3.5" />
              ETL Agent
            </button>
          )}
        </main>
      )}

      {/* Dashboard 未选中时的占位 */}
      {!activeDashboardId && (
        <main className="flex-1 min-w-0 border-r border-slate-200 relative">
          <DashboardContent />
        </main>
      )}

      {/* Dashboard 收起时的展开按钮 */}
      {activeDashboardId && !dashboardOpen && (
        <div className="flex-shrink-0 flex items-start pt-3 px-1 border-r border-slate-200 bg-slate-50">
          <button
            onClick={() => setDashboardOpen(true)}
            className="p-2 text-slate-400 hover:text-indigo-600 hover:bg-white rounded-lg transition-colors cursor-pointer"
            title="展开 Dashboard"
          >
            <PanelLeftOpen className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Agent 面板 - Dashboard 收起时变宽 */}
      {activeDashboardId && agentOpen && (
        <aside className={`flex-shrink-0 bg-white transition-all ${dashboardOpen ? 'w-[560px]' : 'flex-1 min-w-0'}`}>
          <AgentPanel onCollapse={() => setAgentOpen(false)} />
        </aside>
      )}

      {viewingMetric && (
        <MetricDefDetailModal def={viewingMetric} onClose={() => setViewingMetric(null)} />
      )}
    </div>
  );
}
