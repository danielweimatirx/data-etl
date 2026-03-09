import { create } from 'zustand';
import type { Dashboard } from './types';
import { useProcessedTableStore } from './processedTableStore';
import {
  fetchDashboards, createDashboardApi, updateDashboardApi, deleteDashboardApi,
} from './api';

interface DashboardState {
  dashboards: Dashboard[];
  activeDashboardId: string | null;
  loaded: boolean;
  error: string | null;

  loadFromServer: () => Promise<void>;
  createDashboard: (name: string, description: string) => string;
  deleteDashboard: (id: string) => void;
  renameDashboard: (id: string, name: string) => void;
  openDashboard: (id: string) => void;
  goHome: () => void;
}

export const useDashboardStore = create<DashboardState>((set, get) => ({
  dashboards: [],
  activeDashboardId: null,
  loaded: false,
  error: null,

  loadFromServer: async () => {
    try {
      const dashboards = await fetchDashboards();
      set({ dashboards, loaded: true, error: null });
    } catch (e) {
      set({ loaded: true, error: e instanceof Error ? e.message : '无法连接服务器' });
    }
  },

  createDashboard: (name, description) => {
    const id = `db-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const now = Date.now();
    const dashboard: Dashboard = { id, name, description, createdAt: now, updatedAt: now };
    const updated = [dashboard, ...get().dashboards];
    set({ dashboards: updated, activeDashboardId: id });
    createDashboardApi(dashboard).catch(console.error);
    return id;
  },

  deleteDashboard: (id) => {
    const updated = get().dashboards.filter(d => d.id !== id);
    set({ dashboards: updated });
    deleteDashboardApi(id).catch(console.error);
    useProcessedTableStore.getState().clearByDashboard(id);
  },

  renameDashboard: (id, name) => {
    const updated = get().dashboards.map(d =>
      d.id === id ? { ...d, name, updatedAt: Date.now() } : d
    );
    set({ dashboards: updated });
    updateDashboardApi(id, { name }).catch(console.error);
  },

  openDashboard: (id) => {
    set({ activeDashboardId: id });
  },

  goHome: () => {
    set({ activeDashboardId: null });
  },
}));
