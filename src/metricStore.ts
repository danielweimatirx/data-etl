import { create } from 'zustand';
import type { Metric, ChartType } from './types';
import { fetchMetricGenerate, fetchMetricQuery, fetchMetrics, upsertMetricApi, deleteMetricApi, clearMetricsByDashboardApi } from './api';

interface MetricState {
  metrics: Metric[];
  generating: boolean;
  error: string | null;

  loadFromServer: () => Promise<void>;
  getByDashboard: (dashboardId: string) => Metric[];
  generateMetric: (opts: {
    dashboardId: string;
    name: string;
    description: string;
    metricDefs: {
      name: string;
      definition: string;
      tables: string[];
      aggregation: string;
      measureField: string;
    }[];
    connectionString: string;
  }) => Promise<{ sql: string; chartType: ChartType; explanation: string; derivedMetricDef?: { name: string; definition: string; tables: string[]; aggregation: string; measureField: string } | null }>;
  confirmMetric: (opts: {
    dashboardId: string;
    name: string;
    description: string;
    tables: string[];
    sql: string;
    chartType: ChartType;
    connectionString: string;
  }) => Promise<void>;
  deleteMetric: (id: string) => void;
  refreshMetric: (id: string, connectionString: string) => Promise<void>;
  clearByDashboard: (dashboardId: string) => void;
  updateMetric: (id: string, updates: Partial<Pick<Metric, 'sql' | 'chartType' | 'data' | 'definition'>>) => void;
  findByName: (dashboardId: string, name: string) => Metric | undefined;
  reorderMetrics: (dashboardId: string, fromId: string, toId: string) => void;
}

export const useMetricStore = create<MetricState>((set, get) => ({
  metrics: [],
  generating: false,
  error: null,

  loadFromServer: async () => {
    try {
      const metrics = await fetchMetrics();
      set({ metrics });
    } catch (e) {
      console.error('[MetricStore] loadFromServer failed:', e);
    }
  },

  getByDashboard: (dashboardId) => get().metrics.filter(m => m.dashboardId === dashboardId),

  generateMetric: async ({ dashboardId, name, description, metricDefs, connectionString }) => {
    set({ generating: true, error: null });
    try {
      const result = await fetchMetricGenerate({ metricName: name, description, metricDefs, connectionString });
      set({ generating: false });
      return result;
    } catch (e) {
      const msg = e instanceof Error ? e.message : '生成失败';
      set({ generating: false, error: msg });
      throw e;
    }
  },

  confirmMetric: async ({ dashboardId, name, description, tables, sql, chartType, connectionString }) => {
    set({ generating: true, error: null });
    try {
      const queryResult = await fetchMetricQuery({ sql, connectionString });
      const metric: Metric = {
        id: `m-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
        dashboardId, name, definition: description, tables, sql, chartType,
        data: queryResult.rows, createdAt: Date.now(),
      };
      const updated = [metric, ...get().metrics];
      set({ metrics: updated, generating: false });
      upsertMetricApi(metric).catch(console.error);
    } catch (e) {
      const msg = e instanceof Error ? e.message : '查询失败';
      set({ generating: false, error: msg });
      throw e;
    }
  },

  deleteMetric: (id) => {
    set({ metrics: get().metrics.filter(m => m.id !== id) });
    deleteMetricApi(id).catch(console.error);
  },

  refreshMetric: async (id, connectionString) => {
    const metric = get().metrics.find(m => m.id === id);
    if (!metric) return;
    try {
      const result = await fetchMetricQuery({ sql: metric.sql, connectionString });
      const updated = get().metrics.map(m => m.id === id ? { ...m, data: result.rows } : m);
      set({ metrics: updated });
      const refreshed = updated.find(m => m.id === id);
      if (refreshed) upsertMetricApi(refreshed).catch(console.error);
    } catch { /* silent */ }
  },

  clearByDashboard: (dashboardId) => {
    set({ metrics: get().metrics.filter(m => m.dashboardId !== dashboardId) });
    clearMetricsByDashboardApi(dashboardId).catch(console.error);
  },

  updateMetric: (id, updates) => {
    const updated = get().metrics.map(m => m.id === id ? { ...m, ...updates } : m);
    set({ metrics: updated });
    const m = updated.find(m => m.id === id);
    if (m) upsertMetricApi(m).catch(console.error);
  },

  findByName: (dashboardId, name) =>
    get().metrics.find(m => m.dashboardId === dashboardId && m.name === name),

  reorderMetrics: (dashboardId, fromId, toId) => {
    if (fromId === toId) return;
    const all = get().metrics;
    const dashMetrics = all.filter(m => m.dashboardId === dashboardId);
    const others = all.filter(m => m.dashboardId !== dashboardId);
    const fromIdx = dashMetrics.findIndex(m => m.id === fromId);
    const toIdx = dashMetrics.findIndex(m => m.id === toId);
    if (fromIdx < 0 || toIdx < 0) return;
    const [moved] = dashMetrics.splice(fromIdx, 1);
    dashMetrics.splice(toIdx, 0, moved);
    const updated = [...dashMetrics, ...others];
    set({ metrics: updated });
    dashMetrics.forEach((m, i) => upsertMetricApi({ ...m, sortOrder: i } as any).catch(console.error));
  },
}));
