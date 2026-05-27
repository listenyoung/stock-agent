import type { EvalDashboardSummary, EvalRunSummary } from '../../types'

interface EvalPanelProps {
  summary?: EvalDashboardSummary
  running: boolean
  onRunEval: () => void
}

function pct(value?: number) {
  return `${Math.round((value || 0) * 100)}%`
}

function score(value?: number) {
  return (value || 0).toFixed(2)
}

function samples(summary?: EvalRunSummary) {
  if (!summary) return []
  return [
    ...summary.failed_samples.map((item) => ({ ...item, label: 'failed' })),
    ...summary.tool_misuse_samples.map((item) => ({ ...item, label: 'tool' })),
    ...summary.low_score_answers.map((item) => ({ ...item, label: 'low' })),
  ].slice(0, 5)
}

export function EvalPanel({ summary, running, onRunEval }: EvalPanelProps) {
  const latest = summary?.latest?.summary
  const items = samples(latest)

  return (
    <section className="eval-panel">
      <div className="panel-heading">
        <h2>Eval</h2>
        <button disabled={running} onClick={onRunEval}>
          {running ? 'Running' : 'Run'}
        </button>
      </div>

      <div className="eval-metrics">
        <div>
          <strong>{pct(latest?.pass_rate)}</strong>
          <span>通过率</span>
        </div>
        <div>
          <strong>{score(latest?.average_score)}</strong>
          <span>平均分</span>
        </div>
        <div>
          <strong>{latest?.total || 0}</strong>
          <span>样本</span>
        </div>
      </div>

      {latest && (
        <div className="eval-grid">
          <span>任务完成度</span>
          <strong>{score(latest.metrics.task_completion)}</strong>
          <span>工具正确率</span>
          <strong>{score(latest.metrics.tool_call_accuracy)}</strong>
          <span>事实准确性</span>
          <strong>{score(latest.metrics.factual_accuracy)}</strong>
          <span>上下文利用率</span>
          <strong>{score(latest.metrics.context_utilization)}</strong>
          <span>风险提示</span>
          <strong>{score(latest.metrics.risk_disclosure)}</strong>
          <span>幻觉率</span>
          <strong>{score(latest.metrics.hallucination_rate)}</strong>
          <span>审批命中率</span>
          <strong>{score(latest.metrics.approval_hit_rate)}</strong>
          <span>平均工具轮数</span>
          <strong>{score(latest.metrics.avg_tool_rounds)}</strong>
          <span>平均 latency</span>
          <strong>{Math.round(latest.metrics.avg_latency_ms || 0)}ms</strong>
        </div>
      )}

      <div className="eval-samples">
        {items.map((item, index) => (
          <article key={`${item.run_id}-${item.label}-${index}`}>
            <div>
              <strong>{item.label}</strong>
              <span>{score(item.overall)}</span>
            </div>
            <p>{item.question || 'No question'}</p>
            {item.issues.length > 0 && <small>{item.issues.join(' / ')}</small>}
          </article>
        ))}
        {!latest && <p className="muted">还没有评估结果</p>}
      </div>
    </section>
  )
}
