/**
 * Run control in `$live` (spec §6, §8): a run is the task holding `$live.run`, which settles
 * the run's inputs when it ends. The built-in generation task drives it through these helpers.
 */
import { Effect } from 'effect'
import type { Draft } from '../../delta/Tracker.ts'
import type { AssistantMessage } from '../../model/Message.ts'
import type { SubmissionSettlement, TaskKind, Tx } from '../../session/Session.ts'
import type { ConversationId, EntryId, SubmissionId, TaskId } from '../../storage/schema/Ids.ts'
import type { TaskRecord } from '../../storage/schema/Task.ts'
import type { SchedulerOutcome } from '../../task/Scheduler.ts'
import * as ToolTask from '../../tool/ToolTask.ts'
import * as Live from '../Live.ts'
import * as Usage from '../Usage.ts'
import { COMPACTION_KIND, failStatus } from './compaction/status.ts'
import type { UserInput } from '../../protocol/Submission.ts'
import { appendInput, applyBoundary } from './inbox.ts'
import type { Boundary } from './inbox.ts'
import { strictJson } from '../../internal/strictJson.ts'

export const ASSISTANT_KIND = '$assistant'

/** The task kind that takes `$live.run`: the built-in generation task. */
export const RUN_KIND = '$generation'

/** What `Tx.createTask()` needs of the run task: the generation's first checkpoint. */
export const runTask: TaskKind<Record<string, never>> = {
  name: RUN_KIND,
  version: 1,
  initial: () => ({ phase: 'prepare', attempt: 1 }),
}

/** A run task owned by its conversation. */
const createRunTask = (tx: Tx, conversationId: ConversationId) =>
  tx.createTask(runTask, {}, { ownership: { kind: 'conversation' }, conversationId })

/** Start a run for placed input submissions: a new run task takes `$live.run`. */
export const startRun = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  live: Draft<Live.State>,
  inputs: ReadonlyArray<SubmissionId>,
) {
  live.run = { taskId: yield* createRunTask(tx, conversationId), inputs: [...inputs] }
})

/**
 * End the run `taskId` owns: settle each of its inputs and remove `run`. Always removes
 * `generation` and `tools`, whose presentation belongs to the ending run.
 */
export const endRun = Effect.fnUntraced(function* (
  tx: Tx,
  live: Draft<Live.State>,
  taskId: TaskId,
  settlement: SubmissionSettlement,
) {
  delete live.generation
  delete live.tools
  const run = live.run
  if (run?.taskId !== taskId) return
  for (const id of run.inputs) yield* tx.settleSubmission(id, settlement)
  delete live.run
})

/**
 * Append a model response as a `$assistant` entry, and add its usage to `$usage`.
 * REMINDER: every built-in writer of assistant entries goes through here, so the ledger stays
 * complete.
 */
export const appendAssistant = Effect.fnUntraced(function* (
  tx: Tx,
  conversationId: ConversationId,
  message: AssistantMessage,
) {
  const key = `${message.provider}/${message.model}`
  yield* Usage.record(tx, conversationId, 'models', key, message.usage)
  return yield* tx.appendEntry(conversationId, {
    kind: ASSISTANT_KIND,
    model: [strictJson({ ...message })],
  })
})

/**
 * Append the committed partial an interrupted, aborted, faulted or orphaned attempt left, as an
 * aborted assistant entry, so the transcript keeps what the model produced. The caller replaces
 * or removes `generation`.
 */
export const convertPartial = Effect.fnUntraced(function* (
  tx: Tx,
  live: Draft<Live.State>,
  conversationId: ConversationId,
) {
  const partial = live.generation?.message
  if (partial === undefined) return
  // A plain copy: the draft's handles read empty once the commit settles.
  const message: AssistantMessage = strictJson({ ...partial })
  yield* appendAssistant(tx, conversationId, { ...message, stopReason: 'aborted' })
})

/** The slot of tool task `taskId` in the current round. */
const slotOf = (live: Draft<Live.State>, taskId: TaskId) =>
  live.tools?.find((slot) => slot.taskId === taskId)

/** The tool task's lifecycle: its slot runs when the call starts, and is done with its result. */
export const toolLifecycle: ToolTask.Lifecycle = {
  started: (tx, call) =>
    Effect.map(tx.doc(Live.document, call.conversationId), (live) => {
      const slot = slotOf(live, call.taskId)
      if (slot !== undefined) slot.status = 'running'
    }),
  settled: (tx, call, entry) =>
    Effect.map(tx.doc(Live.document, call.conversationId), (live) => {
      const slot = slotOf(live, call.taskId)
      if (slot === undefined) return
      slot.status = 'done'
      slot.entry = entry
    }),
}

/** Hand run control from `from` to `to`; the run's inputs move with it. */
export const handOver = (live: Draft<Live.State>, from: TaskId, to: TaskId): void => {
  if (live.run?.taskId === from) live.run.taskId = to
}

/**
 * A final answer (spec §6): the final boundary places queued items, the run's inputs settle
 * `done` with `answer`, and the user items it placed start the next run. A `continuation` from
 * `onYield` instead appends user input and hands the run to a fresh generation, unless the
 * boundary placed user items or a reset. The caller prepares the boundary at the start of its
 * commit, before it appends the answer.
 */
export const finishRun = Effect.fnUntraced(function* (
  tx: Tx,
  boundary: Boundary,
  taskId: TaskId,
  answer: EntryId,
  continuation?: UserInput,
) {
  const { conversationId } = boundary
  const live = yield* tx.doc(Live.document, conversationId)
  const { users, reset } = yield* applyBoundary(tx, boundary, 'final')
  if (continuation !== undefined && users.length === 0 && !reset && live.run?.taskId === taskId) {
    yield* appendInput(tx, conversationId, continuation)
    handOver(live, taskId, yield* createRunTask(tx, conversationId))
    delete live.generation
    return
  }
  yield* endRun(tx, live, taskId, { status: 'done', answer })
  if (users.length > 0) yield* startRun(tx, conversationId, live, users)
})

/**
 * The `postTools` boundary after a tool round (spec §8.5): placed steers join the run, which a
 * fresh run task takes over. A queued reset cuts the run's context instead: the run ends
 * `unanswered` with reason `reset`, and the user items placed after it start the next run.
 * Returns the task that holds the run now, if any.
 */
export const continueRun = Effect.fnUntraced(function* (
  tx: Tx,
  boundary: Boundary,
  taskId: TaskId,
) {
  const { conversationId } = boundary
  const live = yield* tx.doc(Live.document, conversationId)
  const { users, reset } = yield* applyBoundary(tx, boundary, 'postTools')
  if (reset) {
    yield* endRun(tx, live, taskId, { status: 'unanswered', reason: 'reset' })
    if (users.length > 0) yield* startRun(tx, conversationId, live, users)
    return live.run?.taskId
  }
  if (live.run?.taskId !== taskId) return live.run?.taskId
  delete live.tools
  live.run.inputs.push(...users)
  const next = yield* createRunTask(tx, conversationId)
  handOver(live, taskId, next)
  return next
})

/**
 * Cleanup for an outcome the scheduler writes itself (spec §5.4): a faulted or orphaned run task
 * ends its run `unanswered`, keeping its partial as an aborted entry; a tool task's slot is done
 * without an entry, and context derivation synthesizes the missing result; a compaction's status
 * is removed and its failure recorded. Other kinds are left alone, so it never creates `$live` elsewhere.
 */
export const settleOutcome = Effect.fnUntraced(function* (
  tx: Tx,
  record: TaskRecord,
  outcome: SchedulerOutcome,
) {
  if (record.kind === ToolTask.name) {
    const slot = slotOf(yield* tx.doc(Live.document, record.conversationId), record.id)
    if (slot !== undefined) slot.status = 'done'
    return
  }
  if (record.kind === COMPACTION_KIND) {
    const error = outcome.status === 'faulted' ? outcome.error.message : outcome.reason
    failStatus(yield* tx.doc(Live.document, record.conversationId), record.id, error)
    return
  }
  if (record.kind !== RUN_KIND) return
  const live = yield* tx.doc(Live.document, record.conversationId)
  if (live.run?.taskId !== record.id) return
  yield* convertPartial(tx, live, record.conversationId)
  const settlement: SubmissionSettlement =
    outcome.status === 'faulted'
      ? { status: 'unanswered', reason: 'faulted', detail: outcome.error.message }
      : { status: 'unanswered', reason: outcome.reason }
  yield* endRun(tx, live, record.id, settlement)
})
