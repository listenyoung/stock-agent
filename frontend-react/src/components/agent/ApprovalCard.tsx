import type { ApprovalRequest } from '../../types'

interface ApprovalCardProps {
  approval?: ApprovalRequest
  onApprove: (approvalId: string) => void
  onReject: (approvalId: string) => void
}

export function ApprovalCard({ approval, onApprove, onReject }: ApprovalCardProps) {
  if (!approval) return null

  return (
    <section className="approval-card">
      <div className="panel-heading">
        <h2>Approval</h2>
        <span>pending</span>
      </div>
      <strong>{approval.name}</strong>
      <p>{approval.reason}</p>
      <pre>{JSON.stringify(approval.arguments, null, 2)}</pre>
      <div className="approval-actions">
        <button type="button" onClick={() => onApprove(approval.approval_id)}>
          同意执行
        </button>
        <button type="button" onClick={() => onReject(approval.approval_id)}>
          拒绝
        </button>
      </div>
    </section>
  )
}
