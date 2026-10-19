import type { CommitPublication } from '../../../session/Session.ts'
import type { Op } from '../../../storage/schema/Document.ts'
import type { ConversationId, TaskId } from '../../../storage/schema/Ids.ts'
import type { AgentEvent } from '../../AgentEvent.ts'
import type { ConversationView } from '../../ViewState.ts'
import { RUN_KIND } from '../run.ts'
import { appended, EMPTY_USAGE, parts, progress, queued, toolEnds, writtenTo } from './events.ts'
import type { Step } from './events.ts'

/** Compaction ends, then task failures. */
const failures = (step: Step): ReadonlyArray<AgentEvent> => {
  const events: Array<AgentEvent> = []
  const now = step.now.live.compactions ?? []
  const failure = step.now.live.compactionFailure
  for (const { taskId, reason } of step.was.live.compactions ?? []) {
    if (now.some((status) => status.taskId === taskId)) continue
    const error = failure?.taskId === taskId ? failure.error : undefined
    events.push({
      type: 'compaction_end',
      taskId,
      reason,
      ...(error === undefined ? {} : { error }),
    })
  }
  for (const task of step.written.tasks.values()) {
    if (task.state.status !== 'terminal') continue
    const { outcome } = task.state
    if (outcome.status !== 'faulted' && outcome.status !== 'orphaned') continue
    const message = outcome.status === 'faulted' ? outcome.error.message : outcome.reason
    events.push({ type: 'task_failed', taskId: task.id, kind: task.kind, message })
  }
  return events
}

/**
 * Whether a generation's turn ended: its outcome committed, at a `completing` hold or at
 * terminal, whichever came first. `held` keeps the generations whose hold ended their turn.
 */
const turnEnded = (step: Step, held: Set<TaskId>): boolean => {
  let ended = false
  for (const task of step.written.tasks.values()) {
    if (task.kind !== RUN_KIND) continue
    if (task.state.status === 'completing' && !held.has(task.id)) {
      held.add(task.id)
      ended = true
    }
    if (task.state.status === 'terminal' && !held.delete(task.id)) ended = true
  }
  return ended
}

/** Submissions and document state, then what began. */
const began = (step: Step): ReadonlyArray<AgentEvent> => {
  const { was, now } = step
  const events: Array<AgentEvent> = []
  for (const record of step.written.submissions) events.push({ type: 'submission', record })
  if (now.docs['pi.inbox'] !== was.docs['pi.inbox'])
    events.push({ type: 'inbox_update', items: queued(now.inbox) })
  // A retired document reads as its initial value, as in a snapshot.
  if (now.docs['pi.agent'] !== was.docs['pi.agent'])
    events.push({ type: 'agent_changed', agent: now.agent ?? {} })
  if (now.docs['pi.usage'] !== was.docs['pi.usage'])
    events.push({ type: 'usage_changed', usage: now.usage ?? EMPTY_USAGE })
  const before = was.live.compactions ?? []
  for (const { taskId, reason, blocking } of now.live.compactions ?? []) {
    if (!before.some((status) => status.taskId === taskId))
      events.push({ type: 'compaction_start', taskId, reason, blocking })
  }
  const run = now.live.run
  if (run !== undefined && run.inputs[0] !== was.live.run?.inputs[0])
    events.push({ type: 'run_start', inputs: run.inputs })
  if (
    run !== undefined &&
    run.taskId !== was.live.run?.taskId &&
    step.written.tasks.get(run.taskId)?.kind === RUN_KIND
  )
    events.push({ type: 'turn_start' })
  return events
}

/** Every event one publication causes, in the order of spec §9.4. */
export const translate = (
  id: ConversationId,
  before: ConversationView,
  after: ConversationView,
  ops: ReadonlyArray<Op>,
  publication: CommitPublication,
  held: Set<TaskId>,
): ReadonlyArray<AgentEvent> => {
  const written = writtenTo(id, publication)
  const touched = written.entries.length + written.tasks.size + written.submissions.length
  if (ops.length === 0 && touched === 0) return []
  const step: Step = { was: parts(before), now: parts(after), ops, written }
  const ends = toolEnds(step)
  const events = [...progress(step), ...appended(step, ends), ...failures(step)]
  if (turnEnded(step, held)) events.push({ type: 'turn_end' })
  const runBefore = step.was.live.run
  if (runBefore !== undefined && step.now.live.run?.inputs[0] !== runBefore.inputs[0])
    events.push({ type: 'run_end', inputs: runBefore.inputs })
  return [...events, ...began(step)]
}
