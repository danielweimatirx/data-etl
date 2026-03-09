export interface MappingFromModel {
  targetField: string;
  source: string;
  logic: string;
  sql: string;
}

export interface ConversationTurn {
  role: 'user' | 'assistant';
  content: string;
}

export interface ChatContext {
  connectionString?: string | null;
  /** 当前 ETL 步骤 1～6，后端仅执行本步操作 */
  currentStep?: number;
  /** 用户选中的库表列表（格式 db.table） */
  selectedTables?: string[];
}

import type { ChartType, Dashboard, Metric, MetricDef, ProcessedTable, ChatMessage } from './types';

/** 指标操作联合类型，由后端 /api/chat 在检测到指标意图时返回 */
export type MetricAction =
  | { type: 'preview'; name: string; definition: string; tables: string[]; sql: string; chartType: ChartType; explanation: string }
  | { type: 'create'; name: string; definition: string; tables: string[]; sql: string; chartType: ChartType; data: Record<string, unknown>[] }
  | { type: 'update'; targetMetricName: string; sql?: string; chartType?: ChartType; data?: Record<string, unknown>[] }
  | { type: 'refresh'; targetMetricName: string; data: Record<string, unknown>[] };

export interface ChatApiResponse {
  reply: string;
  connectionReceived?: boolean;
  /** 本轮是否有一次连接测试且为成功 */
  connectionTestOk?: boolean;
  /** 模型判断的当前步骤 1～6 */
  currentStep?: number;
  /** 指标操作指令，当后端检测到指标意图时返回 */
  metricAction?: MetricAction;
}

export async function fetchChatWithModel(
  conversation: ConversationTurn[],
  context: ChatContext,
): Promise<ChatApiResponse> {
  const res = await fetch('/api/chat', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ conversation, context }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}

export interface MappingApiRequest {
  /** 当前轮用户输入（保留兼容） */
  message: string;
  /** 完整对话历史，供 LLM 结合上下文理解与转换 */
  conversation?: ConversationTurn[];
  targetTableName: string;
  targetFields: { name: string; type: string; comment: string }[];
  existingMappings: { targetField: string; source: string; logic: string; sql: string; status: string }[];
}

export interface MappingApiResponse {
  mappings: MappingFromModel[];
}

export async function fetchMappingsFromModel(
  payload: MappingApiRequest,
): Promise<MappingApiResponse> {
  const res = await fetch('/api/mapping', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}

export interface DmlApiRequest {
  targetTableFullName: string;
  mappings: { targetField: string; source: string; logic: string; sql: string }[];
}

export interface DmlApiResponse {
  dml: string;
}

export async function fetchDmlFromModel(payload: DmlApiRequest): Promise<DmlApiResponse> {
  const res = await fetch('/api/dml', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}

/** 根据当前 SQL 让模型优化（如子查询改 JOIN） */
export async function fetchOptimizeDml(currentDml: string): Promise<DmlApiResponse> {
  const res = await fetch('/api/dml/optimize', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ dml: currentDml }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}


// ────────── 指标相关 API ──────────

export interface DatabaseTree {
  database: string;
  tables: string[];
}

export async function fetchTableList(connectionString: string): Promise<{ databases: DatabaseTree[] }> {
  const res = await fetch('/api/tables', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ connectionString }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}

export interface MetricMatchRequest {
  description: string;
  metricDefs: {
    name: string;
    definition: string;
    tables: string[];
    aggregation: string;
    measureField: string;
  }[];
}

export interface MetricMatchResponse {
  matches: { name: string; reason: string }[];
  suggestion: string;
}

export async function fetchMetricMatch(payload: MetricMatchRequest): Promise<MetricMatchResponse> {
  const res = await fetch('/api/metric/match', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}

export interface MetricGenerateRequest {
  metricName: string;
  description: string;
  metricDefs: {
    name: string;
    definition: string;
    tables: string[];
    aggregation: string;
    measureField: string;
  }[];
  connectionString: string;
}

export interface MetricGenerateResponse {
  sql: string;
  chartType: 'number' | 'bar' | 'line' | 'pie' | 'table';
  explanation: string;
  derivedMetricDef?: {
    name: string;
    definition: string;
    tables: string[];
    aggregation: string;
    measureField: string;
  } | null;
}

export async function fetchMetricGenerate(payload: MetricGenerateRequest): Promise<MetricGenerateResponse> {
  const res = await fetch('/api/metric/generate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}

export interface MetricQueryRequest {
  sql: string;
  connectionString: string;
}

export interface MetricQueryResponse {
  rows: Record<string, unknown>[];
  rowCount: number;
}

export async function fetchMetricQuery(payload: MetricQueryRequest): Promise<MetricQueryResponse> {
  const res = await fetch('/api/metric/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}

// ────────── 血缘分析 API ──────────

export interface MetricLineageTable {
  name: string;
  role: string;
  fields: string[];
}

export interface MetricLineageLayer {
  level: 'source' | 'processed' | 'metric';
  label: string;
  tables: MetricLineageTable[];
}

export interface MetricLineageEdge {
  from: { table: string; field: string };
  to: { table: string; field: string };
  transform: string;
}

export interface MetricLineageResponse {
  layers: MetricLineageLayer[];
  edges: MetricLineageEdge[];
  summary: string;
}

export async function fetchMetricLineage(payload: {
  metricDef: { name: string; definition: string; tables: string[]; aggregation: string; measureField: string };
  processedTables: { database: string; table: string; sourceTables: string[]; fieldMappings: { targetField: string; sourceTable: string; sourceExpr: string; transform: string }[]; insertSql: string }[];
  connectionString: string;
}): Promise<MetricLineageResponse> {
  const res = await fetch('/api/metric-lineage', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}

export interface LineageSourceTable {
  name: string;
  alias: string;
  role: '基表' | '维表' | '关联表';
  joinType: string;
  joinCondition: string;
}

export interface LineageFieldMapping {
  targetField: string;
  sourceTable: string;
  sourceField: string;
  transform: string;
  expression: string;
}

export interface LineageJoinRelation {
  leftTable: string;
  rightTable: string;
  joinType: string;
  condition: string;
}

export interface LineageResponse {
  targetTable: string;
  sourceTables: LineageSourceTable[];
  fieldMappings: LineageFieldMapping[];
  joinRelations: LineageJoinRelation[];
  groupBy: string;
  filters: string;
}

export async function fetchLineage(payload: {
  sql: string;
  connectionString: string;
  targetTable: string;
}): Promise<LineageResponse> {
  const res = await fetch('/api/lineage', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error((err as { error?: string }).error || `HTTP ${res.status}`);
  }
  return res.json();
}

// ────────── 应用数据持久化 API ──────────

export async function fetchAppStatus(): Promise<{ enabled: boolean }> {
  const res = await fetch('/api/app/status');
  return res.json();
}

// Dashboard

export async function fetchDashboards(): Promise<Dashboard[]> {
  const res = await fetch('/api/app/dashboards');
  if (!res.ok) throw new Error('获取 dashboards 失败');
  return res.json();
}

export async function createDashboardApi(dashboard: Dashboard): Promise<Dashboard> {
  const res = await fetch('/api/app/dashboards', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(dashboard),
  });
  if (!res.ok) throw new Error('创建 dashboard 失败');
  return dashboard;
}

export async function updateDashboardApi(id: string, updates: Partial<Dashboard>): Promise<void> {
  await fetch(`/api/app/dashboards/${id}`, {
    method: 'PUT', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(updates),
  });
}

export async function deleteDashboardApi(id: string): Promise<void> {
  await fetch(`/api/app/dashboards/${id}`, { method: 'DELETE' });
}

// Metrics
export async function fetchMetrics(dashboardId?: string): Promise<Metric[]> {
  const url = dashboardId ? `/api/app/metrics?dashboardId=${dashboardId}` : '/api/app/metrics';
  const res = await fetch(url);
  if (!res.ok) throw new Error('获取 metrics 失败');
  return res.json();
}

export async function upsertMetricApi(metric: Metric): Promise<void> {
  await fetch('/api/app/metrics', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(metric),
  });
}

export async function deleteMetricApi(id: string): Promise<void> {
  await fetch(`/api/app/metrics/${id}`, { method: 'DELETE' });
}

export async function clearMetricsByDashboardApi(dashboardId: string): Promise<void> {
  await fetch(`/api/app/metrics/by-dashboard/${dashboardId}`, { method: 'DELETE' });
}

// MetricDefs
export async function fetchMetricDefs(dashboardId?: string): Promise<MetricDef[]> {
  const url = dashboardId ? `/api/app/metric-defs?dashboardId=${dashboardId}` : '/api/app/metric-defs';
  const res = await fetch(url);
  if (!res.ok) throw new Error('获取 metric-defs 失败');
  return res.json();
}

export async function upsertMetricDefApi(def: MetricDef): Promise<void> {
  await fetch('/api/app/metric-defs', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(def),
  });
}

export async function deleteMetricDefApi(id: string): Promise<void> {
  await fetch(`/api/app/metric-defs/${id}`, { method: 'DELETE' });
}

// ProcessedTables
export async function fetchProcessedTables(dashboardId?: string): Promise<ProcessedTable[]> {
  const url = dashboardId ? `/api/app/processed-tables?dashboardId=${dashboardId}` : '/api/app/processed-tables';
  const res = await fetch(url);
  if (!res.ok) throw new Error('获取 processed-tables 失败');
  return res.json();
}

export async function upsertProcessedTableApi(table: ProcessedTable): Promise<void> {
  await fetch('/api/app/processed-tables', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(table),
  });
}

export async function deleteProcessedTableApi(id: string): Promise<void> {
  await fetch(`/api/app/processed-tables/${id}`, { method: 'DELETE' });
}

export async function clearProcessedTablesByDashboardApi(dashboardId: string): Promise<void> {
  await fetch(`/api/app/processed-tables/by-dashboard/${dashboardId}`, { method: 'DELETE' });
}

// Chat Messages
export interface ChatState {
  step: number;
  connectionString: string | null;
  messages: ChatMessage[];
}

export async function fetchChatState(dashboardId: string, chatType: 'etl' | 'metric'): Promise<ChatState | null> {
  const res = await fetch(`/api/app/chat/${dashboardId}/${chatType}`);
  if (!res.ok) return null;
  const data = await res.json();
  return data;
}

export async function saveChatStateApi(dashboardId: string, chatType: 'etl' | 'metric', data: ChatState): Promise<void> {
  await fetch(`/api/app/chat/${dashboardId}/${chatType}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
}

export async function deleteChatStateApi(dashboardId: string, chatType?: 'etl' | 'metric'): Promise<void> {
  const url = chatType ? `/api/app/chat/${dashboardId}/${chatType}` : `/api/app/chat/${dashboardId}`;
  await fetch(url, { method: 'DELETE' });
}

// Schema Selections
export async function fetchSchemaSelection(dashboardId: string): Promise<string[] | null> {
  const res = await fetch(`/api/app/schema-selection/${dashboardId}`);
  if (!res.ok) return null;
  return res.json();
}

export async function saveSchemaSelectionApi(dashboardId: string, selectedTables: string[]): Promise<void> {
  await fetch(`/api/app/schema-selection/${dashboardId}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ selectedTables }),
  });
}
