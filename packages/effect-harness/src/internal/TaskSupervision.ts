/** Native ownership decisions. These functions stage only through the supplied Transaction. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Agent from '../Agent.js'
import type * as Record from '../Record.js'
import * as TaskPolicy from '../TaskPolicy.js'
import * as Transaction from '../Transaction.js'
import type * as TaskExecution from '../TaskExecution.js'
import type * as Scheduler from '../TaskScheduler.js'
import * as AgentState from './AgentState.js'
import * as Tree from './TaskTree.js'

export { validateWait } from './TaskTree.js'

export const graph = (tree: Tree.Tree): Scheduler.Graph => ({
  conversations: [...tree.conversations.values()],
  tasks: [...tree.tasks.values()]
    .filter((record) => record.state.status !== 'terminal')
    .map((record) => {
      const phase = record.state.checkpoint
      const checkpoint = Schema.is(Schema.Struct({ phase: Schema.String }))(phase)
        ? phase
        : undefined
      return {
        id: record.id,
        conversationId: record.conversationId,
        kind: record.kind,
        version: record.version,
        ...(record.owner === undefined ? {} : { owner: record.owner }),
        background: record.background,
        abortRequested: record.abortRequested,
        status: record.state.status,
        ...(checkpoint === undefined ? {} : { phase: checkpoint.phase }),
        on: [...(record.state.on ?? [])],
        ownedConversations: [...tree.conversations.values()]
          .filter((conversation) => conversation.owner?.taskId === record.id)
          .map((conversation) => conversation.id),
      }
    }),
})

export const ownedLive = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  id: Record.TaskId,
  includeBackground = false,
) {
  return Tree.liveOwned(yield* Tree.load(tx), id, includeBackground).length > 0
})

export const hold = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  record: Record.Task,
  outcome: Record.TaskOutcome,
) {
  yield* Transaction.putTask(
    tx,
    TaskPolicy.withState(record, {
      status: (yield* ownedLive(tx, record.id)) ? 'completing' : 'terminal',
      outcome,
    }),
  )
})

/** Repeat on the staged view: draining one owner may release the next owner in this commit. */
export const reconcile = Effect.fnUntraced(function* (tx: Transaction.Transaction) {
  for (;;) {
    const tree = yield* Tree.load(tx)
    let changed = false
    for (const record of tree.tasks.values()) {
      if (record.state.status === 'terminal') continue
      const owner = Tree.cancellingOwner(tree, record)
      if (owner !== undefined) {
        const next = Tree.mark(
          record,
          TaskPolicy.abortIntent(owner)?._tag === 'restart' && !Tree.failedOutcome(owner),
        )
        if (next !== record) {
          yield* Transaction.putTask(tx, next)
          changed = true
        }
      }
      if (
        record.state.status === 'waiting' &&
        !record.abortRequested &&
        record.state.policy === 'failFast' &&
        record.state.on!.some((id) => {
          const member = tree.tasks.get(id)
          return member !== undefined && Tree.failedOutcome(member)
        })
      ) {
        for (const id of record.state.on!) {
          const member = tree.tasks.get(id)
          if (member === undefined || Tree.failedOutcome(member)) continue
          const next = Tree.mark(member)
          if (next !== member) {
            yield* Transaction.putTask(tx, next)
            changed = true
          }
        }
      }
    }
    // Work in a task-owned conversation uses that owner's cancellation intent too.
    const submissions = yield* Transaction.scanSubmissions(tx, { status: 'queued' }).pipe(
      Stream.runCollect,
    )
    for (const submission of submissions) {
      if (submission._tag !== 'InputQueued') continue
      const ownerId = tree.conversations.get(submission.conversationId)?.owner?.taskId
      const owner = ownerId === undefined ? undefined : tree.tasks.get(ownerId)
      if (owner === undefined) continue
      if (
        (owner.state.status !== 'terminal' &&
          (owner.abortRequested || Tree.failedOutcome(owner))) ||
        Tree.cancellingOwner(tree, owner) !== undefined
      ) {
        yield* Transaction.putSubmission(tx, {
          ...submission,
          _tag: 'InputUnanswered',
          status: 'unanswered',
          reason: 'aborted',
        })
        changed = true
      }
    }
    if (changed) continue
    for (const record of tree.tasks.values()) {
      if (record.state.status === 'completing' && Tree.liveOwned(tree, record.id).length === 0) {
        yield* Transaction.putTask(
          tx,
          TaskPolicy.withState(record, { status: 'terminal', outcome: record.state.outcome! }),
        )
        changed = true
      } else if (
        record.state.status === 'waiting' &&
        !record.abortRequested &&
        record.state.on!.every((id) => tree.tasks.get(id)?.state.status === 'terminal')
      ) {
        yield* Transaction.putTask(tx, {
          ...record,
          state: { status: 'pending', checkpoint: record.state.checkpoint! },
        })
        changed = true
      }
    }
    if (!changed) return
  }
})

export const createConversation = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  owner: Record.Task,
  options: TaskExecution.ConversationOptions = {},
) {
  const conversation =
    options.fork === undefined
      ? yield* Transaction.createConversation(tx, { ownership: { _tag: 'task', taskId: owner.id } })
      : yield* Transaction.forkConversation(tx, options.fork.conversationId, options.fork.at, {
          ownership: { _tag: 'task', taskId: owner.id },
        })
  const inherited = yield* Transaction.snapshot(
    tx,
    AgentState.document,
    AgentState.target(options.fork === undefined ? owner.conversationId : conversation.id),
  )
  const state: Agent.State = Option.isSome(inherited) ? inherited.value.value : {}
  const value = { ...state }
  if (options.agent !== undefined) {
    const change = yield* Schema.decodeEffect(Agent.ChangeSchema)(options.agent)
    for (const key of [
      'model',
      'extensions',
      'tools',
      'modelTools',
      'toolkits',
      'instructions',
    ] as const) {
      if (change[key] === null) delete value[key]
      else if (change[key] !== undefined) Object.assign(value, { [key]: change[key] })
    }
  }
  yield* Transaction.ensureDocument(tx, AgentState.document, AgentState.target(conversation.id))
  yield* Transaction.setDocument(tx, AgentState.document, AgentState.target(conversation.id), value)
  return conversation
})
