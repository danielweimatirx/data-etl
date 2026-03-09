import { create } from 'zustand';
import type { MetricDef } from './types';
import { fetchMetricDefs, upsertMetricDefApi, deleteMetricDefApi } from './api';

interface MetricDefState {
  defs: MetricDef[];
  loadFromServer: () => Promise<void>;
  add: (def: Omit<MetricDef, 'id' | 'createdAt'>) => MetricDef;
  remove: (id: string) => void;
  getAll: () => MetricDef[];
  getByDashboard: (dashboardId: string) => MetricDef[];
}

export const useMetricDefStore = create<MetricDefState>((set, get) => ({
  defs: [],

  loadFromServer: async () => {
    try {
      const defs = await fetchMetricDefs();
      set({ defs });
    } catch (e) {
      console.error('[MetricDefStore] loadFromServer failed:', e);
    }
  },

  add: (entry) => {
    const def: MetricDef = {
      ...entry,
      id: `md-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      createdAt: Date.now(),
    };
    set({ defs: [def, ...get().defs] });
    upsertMetricDefApi(def).catch(console.error);
    return def;
  },

  remove: (id) => {
    set({ defs: get().defs.filter(d => d.id !== id) });
    deleteMetricDefApi(id).catch(console.error);
  },

  getAll: () => get().defs,
  getByDashboard: (dashboardId) => get().defs.filter(d => d.dashboardId === dashboardId),
}));
