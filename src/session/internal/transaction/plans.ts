import { Effect } from 'effect'
import type * as Tracker from '../../../delta/Tracker.ts'
import type { DocumentCreate, DocumentRecord } from '../../../storage/schema/Document.ts'
import type { ConversationId, Seq, TaskId } from '../../../storage/schema/Ids.ts'
import type { JsonObject } from '../../../storage/schema/Json.ts'
import type { StorageWrite } from '../../../storage/schema/Write.ts'
import type { DocumentCommitChange } from '../../Session.ts'
import type { Cache, Loaded } from '../cache.ts'
import { addressId } from '../documents.ts'
import type { AnyDefinition } from '../documents.ts'
import { scanDocuments } from './conversations.ts'
import { currentTask, rejected } from './state.ts'
import type { DocumentEntry, TxState } from './state.ts'

/**
 * What each staged document incarnation writes and publishes, ported from pi's
 * `session/transaction.ts`. Plans are decided before Storage sees the batch, so adoption after
 * the commit only applies them.
 */

type ContentWrite = Extract<
  StorageWrite,
  { readonly type: 'document.create' | 'document.copy' | 'document.change' }
>

/** The prepared change of a tracked incarnation. */
interface PlannedChange {
  readonly tracker: Tracker.Tracker<JsonObject>
  readonly prepared: Tracker.Prepared<JsonObject>
  readonly version: number
  /** The cached incarnation this change updates; absent when it creates one. */
  readonly loaded?: Loaded
  readonly definition?: AnyDefinition
}

export interface Plan {
  readonly addressId: string
  /** A committed incarnation's record, or the create record of a new one. */
  readonly record: DocumentCreate | DocumentRecord
  retire: boolean
  /** Creation, copy, or change content; absent when only retirement is written. */
  content?: ContentWrite
  /** Absent for fork copies and retirement only. */
  readonly change?: PlannedChange
  /** The conversation the publication names, resolved before the commit. */
  conversationId?: ConversationId
}

const isCommitted = (record: DocumentCreate | DocumentRecord): record is DocumentRecord =>
  'createdAt' in record

/** The content a loaded incarnation writes: a base for a new version, else a delta for a change. */
const loadedContent = (
  loaded: Loaded,
  prepared: Tracker.Prepared<JsonObject>,
  version: number,
): ContentWrite | undefined => {
  const id = loaded.record.id
  if (loaded.storedVersion < version) {
    return {
      type: 'document.change',
      id,
      content: { version, kind: 'base', value: prepared.value },
    }
  }
  if (prepared.ops.length === 0) return undefined
  return { type: 'document.change', id, content: { version, kind: 'delta', ops: prepared.ops } }
}

/** The plan of one staged incarnation; retirement of terminal tasks is decided later. */
export const planDocument = (entry: DocumentEntry): Plan | undefined => {
  const { target, prepared, definition } = entry
  const plan = { addressId: entry.addressId, retire: entry.retireOnCommit }
  if (target === undefined) return undefined
  if (target.kind === 'fork-copy') {
    const { record, source } = target
    return { ...plan, record, content: { type: 'document.copy', record, source } }
  }
  if (target.kind === 'retire-only') return { ...plan, record: target.record }
  if (prepared === undefined) return undefined
  if (target.kind === 'created') {
    const { record, version, tracker } = target
    const content = { version, kind: 'base' as const, value: prepared.value }
    const change = { tracker, prepared, version }
    return { ...plan, record, content: { type: 'document.create', record, content }, change }
  }
  if (definition === undefined) return undefined
  const loaded = target.document
  const version = definition.version
  const change = { tracker: loaded.tracker, prepared, version, loaded, definition }
  const content = loadedContent(loaded, prepared, version)
  return { ...plan, record: loaded.record, change, ...(content === undefined ? {} : { content }) }
}

/**
 * Whether adoption publishes the plan: every creation, copy, and retirement, and a loaded
 * incarnation that writes content, a migration-only base included.
 */
export const publishes = (plan: Plan): boolean =>
  plan.retire || plan.change?.loaded === undefined || plan.content !== undefined

/** A fork transaction may not change the documents it copies from. */
export const rejectForkSourceWrites = (state: TxState, plans: ReadonlyArray<Plan>) => {
  for (const { content, retire, record } of plans) {
    if (content === undefined && !retire) continue
    if (state.forkSourceDocuments.has(record.id)) {
      return rejected(`Cannot change fork source document ${record.id} in the fork transaction`)
    }
    if (
      'history' in record &&
      record.fork === 'current' &&
      state.forkSourceConversations.has(record.scope.conversationId)
    ) {
      const conversationId = record.scope.conversationId
      return rejected(
        `Cannot fork conversation ${conversationId} while changing its current-policy documents`,
      )
    }
  }
  return Effect.void
}

/** Terminal settlement retires every document of the task, those this transaction created too. */
export const retireTerminalTasks = Effect.fnUntraced(function* (
  state: TxState,
  plans: Array<Plan>,
) {
  const terminal = new Set<TaskId>()
  for (const task of state.tasks.values()) {
    if (task.write?.record.state.status === 'terminal') terminal.add(task.write.record.id)
  }
  if (terminal.size === 0) return
  const retiring = new Set<DocumentCreate['id']>()
  for (const plan of plans) {
    const scope = plan.record.scope
    if (scope.kind !== 'task' || !terminal.has(scope.taskId)) continue
    plan.retire = true
    retiring.add(plan.record.id)
  }
  for (const taskId of terminal) {
    // A task created in this transaction has no committed documents.
    if (state.tasks.get(taskId)?.write?.kind === 'create') continue
    for (const record of yield* scanDocuments(state, { kind: 'task', taskId }, 'current')) {
      if (retiring.has(record.id)) continue
      plans.push({ addressId: addressId(record), record, retire: true })
      retiring.add(record.id)
    }
  }
})

/** Resolve the conversation each publication names; a task document takes its task's. */
export const resolvePublicationOwners = (state: TxState, plans: ReadonlyArray<Plan>) =>
  Effect.forEach(
    plans,
    (plan) => {
      const scope = plan.record.scope
      if (!publishes(plan) || scope.kind === 'session') return Effect.void
      if (scope.kind === 'conversation') {
        plan.conversationId = scope.conversationId
        return Effect.void
      }
      return Effect.flatMap(currentTask(state, scope.taskId), (task) =>
        Effect.sync(() => {
          if (task !== undefined) plan.conversationId = task.conversationId
        }),
      )
    },
    { discard: true },
  )

/** Checkpoint predicates run last, after every validation: a true one turns a delta into a base. */
export const checkpoint = (plan: Plan): Effect.Effect<void> => {
  const { content, change } = plan
  if (content?.type !== 'document.change' || content.content.kind !== 'delta') return Effect.void
  const definition = change?.definition
  if (change?.loaded === undefined || definition?.checkpointWhen === undefined) return Effect.void
  const { prepared, version, loaded } = change
  const info = { deltasSinceBase: loaded.deltasSinceBase }
  return Effect.flatMap(definition.checkpointWhen(prepared.value, prepared.ops, info), (base) =>
    Effect.sync(() => {
      if (base)
        plan.content = { ...content, content: { version, kind: 'base', value: prepared.value } }
    }),
  )
}

/** The adopted record of a plan committed as `seq`. */
const adoptedRecord = (plan: Plan, seq: Seq): DocumentRecord => {
  const record = isCommitted(plan.record) ? plan.record : { ...plan.record, createdAt: seq }
  return plan.retire ? { ...record, retiredAt: seq } : record
}

/** Adopt a planned change after Storage committed it, and keep the cache in step. */
const adoptChange = Effect.fnUntraced(function* (
  cache: Cache,
  plan: Plan,
  change: PlannedChange,
  record: DocumentRecord,
) {
  const { tracker, prepared, loaded, version } = change
  // A new incarnation is adopted unless it retires at once; a loaded one only when it changed.
  const adopt = loaded === undefined ? !plan.retire : prepared.ops.length > 0
  if (!adopt) yield* prepared.abort
  // Adoption only fails for a change Storage never saw; dropping the tracker reloads it.
  const adopted = adopt
    ? yield* Effect.as(tracker.adopt(prepared), true).pipe(Effect.orElseSucceed(() => false))
    : true
  if (!adopted) yield* cache.evict(plan.addressId, record.id)
  if (loaded !== undefined) {
    if (loaded.storedVersion < version) loaded.storedVersion = version
    if (plan.content?.type === 'document.change') {
      loaded.deltasSinceBase = plan.content.content.kind === 'base' ? 0 : loaded.deltasSinceBase + 1
    }
    return
  }
  if (plan.retire || !adopted) return
  const valueVersion = version
  const installed = { addressId: plan.addressId, record, storedVersion: version, valueVersion }
  yield* cache.install({ ...installed, deltasSinceBase: 0, tracker })
})

/** The publication of one plan, if it publishes. */
const planPublication = (plan: Plan, record: DocumentRecord): DocumentCommitChange | undefined => {
  const { conversationId, change, content } = plan
  if (plan.retire) {
    return { type: 'document', record, conversationId, version: undefined, value: null, ops: [] }
  }
  if (content?.type === 'document.copy') {
    const scope = content.record.scope
    if (scope.kind !== 'conversation') return undefined
    const { source } = content
    return { type: 'document.copy', record, conversationId: scope.conversationId, source }
  }
  if (change === undefined || !publishes(plan)) return undefined
  const ops = change.loaded === undefined ? [] : change.prepared.ops
  const { version, prepared } = change
  return { type: 'document', record, conversationId, version, value: prepared.value, ops }
}

/** Adopt every plan after Storage committed it as `seq`, and describe what each published. */
export const adopt = Effect.fnUntraced(function* (
  cache: Cache,
  plans: ReadonlyArray<Plan>,
  seq: Seq,
) {
  const publications: Array<DocumentCommitChange> = []
  for (const plan of plans) {
    const record = adoptedRecord(plan, seq)
    if (plan.change !== undefined) yield* adoptChange(cache, plan, plan.change, record)
    if (plan.retire && isCommitted(plan.record)) yield* cache.evict(plan.addressId, record.id)
    const publication = planPublication(plan, record)
    if (publication !== undefined) publications.push(publication)
  }
  return publications
})
