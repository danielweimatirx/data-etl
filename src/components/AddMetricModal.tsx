import { useState } from 'react';
import { Loader2, Sparkles, Check, X, BarChart3, ChevronDown, Plus } from 'lucide-react';
import { useMetricStore } from '../metricStore';
import { useMetricDefStore } from '../metricDefStore';
import { useStore } from '../store';
import type { ChartType, MetricDef } from '../types';

interface Props {
  dashboardId: string;
  onClose: () => void;
}

type Step = 'form' | 'preview' | 'done';

const CHART_OPTIONS: { value: ChartType; label: string }[] = [
  { value: 'number', label: '数值' },
  { value: 'bar', label: '柱状图' },
  { value: 'line', label: '折线图' },
  { value: 'pie', label: '饼图' },
  { value: 'table', label: '表格' },
];

function MetricDefItem({ def }: { def: MetricDef }) {
  return (
    <div className="flex items-center gap-2 px-3 py-2">
      <BarChart3 className="w-3 h-3 flex-shrink-0 text-emerald-500" />
      <div className="flex-1 min-w-0">
        <p className="text-xs font-medium text-slate-700 truncate">{def.name}</p>
        <p className="text-[10px] text-slate-400 truncate">
          {def.aggregation}({def.measureField}) · {def.definition}
        </p>
      </div>
    </div>
  );
}

export default function AddMetricModal({ dashboardId, onClose }: Props) {
  const connectionString = useStore(s => s.connectionString);
  const { generating, generateMetric, confirmMetric } = useMetricStore();
  const allDefs = useMetricDefStore(s => s.defs);
  const metricDefs = allDefs.filter(d => d.dashboardId === dashboardId);

  const [step, setStep] = useState<Step>('form');
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [sql, setSql] = useState('');
  const [chartType, setChartType] = useState<ChartType>('bar');
  const [explanation, setExplanation] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [derivedDef, setDerivedDef] = useState<{ name: string; definition: string; tables: string[]; aggregation: string; measureField: string } | null>(null);
  const [createDerived, setCreateDerived] = useState(true);
  const addMetricDef = useMetricDefStore(s => s.add);

  const canGenerate = name.trim() && description.trim() && connectionString && metricDefs.length > 0;

  const handleGenerate = async () => {
    if (!canGenerate) return;
    setError(null);
    try {
      const result = await generateMetric({
        dashboardId,
        name: name.trim(),
        description: description.trim(),
        metricDefs: metricDefs.map(d => ({
          name: d.name, definition: d.definition, tables: d.tables,
          aggregation: d.aggregation, measureField: d.measureField,
        })),
        connectionString: connectionString!,
      });
      setSql(result.sql);
      setChartType(result.chartType);
      setExplanation(result.explanation);
      if (result.derivedMetricDef) {
        setDerivedDef(result.derivedMetricDef);
        setCreateDerived(true);
      } else {
        setDerivedDef(null);
      }
      // 如果 explanation 中包含验证失败信息，提取为 error 提示
      if (result.explanation && result.explanation.includes('⚠️ SQL 验证失败')) {
        const errPart = result.explanation.split('⚠️ SQL 验证失败')[1] || '';
        setError('SQL 自动验证失败' + errPart + '，请手动修改 SQL 后再执行');
      }
      setStep('preview');
    } catch (e) {
      setError(e instanceof Error ? e.message : '生成失败');
    }
  };

  const handleConfirm = async () => {
    if (!connectionString) return;
    setError(null);
    try {
      const tables = [...new Set(metricDefs.flatMap(d => d.tables))];
      await confirmMetric({ dashboardId, name: name.trim(), description: description.trim(), tables, sql, chartType, connectionString });
      // 同时创建派生指标定义
      if (createDerived && derivedDef) {
        addMetricDef({
          dashboardId,
          name: derivedDef.name,
          definition: derivedDef.definition,
          tables: derivedDef.tables || [],
          aggregation: derivedDef.aggregation || 'SUM',
          measureField: derivedDef.measureField || '',
        });
      }
      setStep('done');
    } catch (e) {
      setError(e instanceof Error ? e.message : '查询失败');
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 backdrop-blur-sm">
      <div className="bg-white rounded-2xl shadow-2xl w-[520px] max-h-[85vh] flex flex-col overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3.5 border-b border-slate-100">
          <h2 className="text-sm font-semibold text-slate-800">
            {step === 'form' && '添加监控数据'}
            {step === 'preview' && '预览 SQL'}
            {step === 'done' && '完成'}
          </h2>
          <button onClick={onClose} className="p-1 rounded-md hover:bg-slate-100 transition-colors cursor-pointer">
            <X className="w-4 h-4 text-slate-400" />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">
          {step === 'form' && (<>
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">数据名称</label>
              <input value={name} onChange={e => setName(e.target.value)} placeholder="如：月度收入趋势、各品类订单量"
                className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500/30 focus:border-indigo-400" />
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">数据描述（自然语言）</label>
              <textarea value={description} onChange={e => setDescription(e.target.value)} rows={3}
                placeholder="用自然语言描述你想统计的数据，如：按月统计各行业的总收入，最近12个月"
                className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500/30 focus:border-indigo-400 resize-none" />
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">可用指标（{metricDefs.length}）</label>
              {metricDefs.length === 0 ? (
                <div className="text-xs text-slate-400 bg-slate-50 rounded-lg px-3 py-3 text-center">
                  当前 Dashboard 还没有定义指标，请先通过 Agent 对话「添加指标」模式创建指标定义
                </div>
              ) : (
                <div className="border border-slate-200 rounded-lg divide-y divide-slate-100 max-h-40 overflow-y-auto">
                  {metricDefs.map(d => <MetricDefItem key={d.id} def={d} />)}
                </div>
              )}
              <p className="text-[10px] text-slate-400 mt-1">系统会根据描述自动匹配合适的指标生成 SQL</p>
            </div>
            {!connectionString && (
              <p className="text-xs text-amber-600 bg-amber-50 rounded-lg px-3 py-2">请先在 ETL Agent 对话中连接数据库</p>
            )}
            {error && <p className="text-xs text-red-500">{error}</p>}
          </>)}
          {step === 'preview' && (<>
            {explanation && <div className="bg-indigo-50 rounded-lg px-3 py-2"><p className="text-xs text-indigo-700">{explanation}</p></div>}
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">SQL（可编辑）</label>
              <textarea value={sql} onChange={e => setSql(e.target.value)} rows={6}
                className="w-full px-3 py-2 text-xs font-mono border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500/30 focus:border-indigo-400 resize-none bg-slate-50" />
            </div>
            <div>
              <label className="block text-xs font-medium text-slate-600 mb-1">图表类型</label>
              <div className="relative">
                <select value={chartType} onChange={e => setChartType(e.target.value as ChartType)}
                  className="w-full appearance-none px-3 py-2 text-sm border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500/30 focus:border-indigo-400 bg-white pr-8">
                  {CHART_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                </select>
                <ChevronDown className="w-3.5 h-3.5 text-slate-400 absolute right-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
              </div>
            </div>
            {/* 派生指标建议 */}
            {derivedDef && (
              <div className="bg-amber-50 border border-amber-200 rounded-lg p-3">
                <label className="flex items-start gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={createDerived}
                    onChange={e => setCreateDerived(e.target.checked)}
                    className="mt-0.5 rounded border-amber-300 text-amber-600 focus:ring-amber-500/30"
                  />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-1.5">
                      <Plus className="w-3 h-3 text-amber-600 flex-shrink-0" />
                      <span className="text-xs font-medium text-amber-800">同时创建派生指标</span>
                    </div>
                    <p className="text-[11px] text-amber-700 mt-1">
                      <span className="font-medium">{derivedDef.name}</span>
                      <span className="text-amber-600 ml-1">
                        {derivedDef.aggregation}({derivedDef.measureField})
                      </span>
                    </p>
                    <p className="text-[10px] text-amber-600 mt-0.5">{derivedDef.definition}</p>
                  </div>
                </label>
              </div>
            )}
            {error && <p className="text-xs text-red-500">{error}</p>}
          </>)}
          {step === 'done' && (
            <div className="text-center py-6">
              <div className="w-12 h-12 rounded-full bg-emerald-100 flex items-center justify-center mx-auto mb-3">
                <Check className="w-6 h-6 text-emerald-600" />
              </div>
              <p className="text-sm font-medium text-slate-800">监控数据已添加</p>
              <p className="text-xs text-slate-400 mt-1">「{name}」已保存到当前 Dashboard</p>
              {createDerived && derivedDef && (
                <p className="text-xs text-amber-600 mt-2">
                  派生指标「{derivedDef.name}」已同步创建
                </p>
              )}
            </div>
          )}
        </div>
        <div className="flex items-center justify-end gap-2 px-5 py-3 border-t border-slate-100">
          {step === 'form' && (<>
            <button onClick={onClose} className="px-3 py-1.5 text-xs text-slate-500 hover:text-slate-700 cursor-pointer">取消</button>
            <button onClick={handleGenerate} disabled={!canGenerate || generating}
              className="flex items-center gap-1.5 px-4 py-1.5 text-xs font-medium text-white bg-indigo-600 rounded-lg hover:bg-indigo-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer">
              {generating ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Sparkles className="w-3.5 h-3.5" />}
              生成 SQL
            </button>
          </>)}
          {step === 'preview' && (<>
            <button onClick={() => { setStep('form'); setError(null); }} className="px-3 py-1.5 text-xs text-slate-500 hover:text-slate-700 cursor-pointer">返回修改</button>
            <button onClick={handleConfirm} disabled={generating || !sql.trim()}
              className="flex items-center gap-1.5 px-4 py-1.5 text-xs font-medium text-white bg-emerald-600 rounded-lg hover:bg-emerald-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors cursor-pointer">
              {generating ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Check className="w-3.5 h-3.5" />}
              确认并执行
            </button>
          </>)}
          {step === 'done' && (
            <button onClick={onClose} className="px-4 py-1.5 text-xs font-medium text-white bg-indigo-600 rounded-lg hover:bg-indigo-700 transition-colors cursor-pointer">关闭</button>
          )}
        </div>
      </div>
    </div>
  );
}
