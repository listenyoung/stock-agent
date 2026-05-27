interface FeedbackBarProps {
  runId?: string
  disabled?: boolean
  onRate: (rating: -1 | 0 | 1) => void
  onExport: () => void
  exportPreview: string
}

export function FeedbackBar({ runId, disabled, onRate, onExport, exportPreview }: FeedbackBarProps) {
  return (
    <section className="feedback-bar">
      <div className="feedback-actions">
        <button type="button" disabled={!runId || disabled} onClick={() => onRate(1)}>
          好
        </button>
        <button type="button" disabled={!runId || disabled} onClick={() => onRate(0)}>
          一般
        </button>
        <button type="button" disabled={!runId || disabled} onClick={() => onRate(-1)}>
          差
        </button>
        <button type="button" disabled={disabled} onClick={onExport}>
          导出 JSONL
        </button>
      </div>
      {exportPreview ? <pre>{exportPreview}</pre> : null}
    </section>
  )
}
