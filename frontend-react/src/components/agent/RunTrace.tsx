import type { AgentRunEvent } from '../../types'

interface RunTraceProps {
  events: AgentRunEvent[]
}

export function RunTrace({ events }: RunTraceProps) {
  return (
    <aside className="trace-panel">
      <div className="panel-heading">
        <h2>Trace</h2>
        <span>{events.length}</span>
      </div>
      <div className="trace-list">
        {events.map((event) => (
          <div className="trace-item" key={`${event.run_id}-${event.sequence}`}>
            <span>{event.sequence}</span>
            <strong>{event.event}</strong>
          </div>
        ))}
      </div>
    </aside>
  )
}
