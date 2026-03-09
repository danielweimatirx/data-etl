import { create } from 'zustand';
import type { ProcessedTable } from './types';
import { fetchProcessedTables, upsertProcessedTableApi, deleteProcessedTableApi, clearProcessedTablesByDashboardApi } from './api';

interface ProcessedTableState {
  tables: ProcessedTable[];
  loadFromServer: () => Promise<void>;
  getByDashboard: (dashboardId: string) => ProcessedTable[];
  addOrUpdate: (entry: Omit<ProcessedTable, 'id'>) => void;
  remove: (id: string) => void;
  clearByDashboard: (dashboardId: string) => void;
}

export const useProcessedTableStore = create<ProcessedTableState>((set, get) => ({
  tables: [],

  loadFromServer: async () => {
    try {
      const tables = await fetchProcessedTables();
      set({ tables });
    } catch (e) {
      console.error('[ProcessedTableStore] loadFromServer failed:', e);
    }
  },

  getByDashboard: (dashboardId) =>
    get().tables.filter(t => t.dashboardId === dashboardId),

  addOrUpdate: (entry) => {
    const id = `${entry.database}.${entry.table}`;
    const existing = get().tables;
    const idx = existing.findIndex(t => t.id === id && t.dashboardId === entry.dashboardId);
    const record = { ...entry, id, processedAt: Date.now() } as ProcessedTable;
    let updated: ProcessedTable[];
    if (idx >= 0) {
      updated = [...existing];
      updated[idx] = { ...updated[idx], ...record };
    } else {
      updated = [record, ...existing];
    }
    set({ tables: updated });
    upsertProcessedTableApi(record).catch(console.error);
  },

  remove: (id) => {
    set({ tables: get().tables.filter(t => t.id !== id) });
    deleteProcessedTableApi(id).catch(console.error);
  },

  clearByDashboard: (dashboardId) => {
    set({ tables: get().tables.filter(t => t.dashboardId !== dashboardId) });
    clearProcessedTablesByDashboardApi(dashboardId).catch(console.error);
  },
}));
