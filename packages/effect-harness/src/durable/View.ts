/**
 * Shared committed conversation mounts and bounded subscriber projections.
 */
import * as Order from 'effect/Order'
import { dual } from 'effect/Function'
import * as handle from './internal/handle.ts'
const ProjectionWatchProto = handle.prototype('@effect-harness/durable/View/ProjectionWatch')
const StateProto = handle.prototype('@effect-harness/durable/View/State')
import type * as Pipeable from 'effect/Pipeable'
import type * as Inspectable from 'effect/Inspectable'
import { identity } from 'effect/Function'
import type * as Types from 'effect/Types'
import * as Predicate from 'effect/Predicate'
import * as Option from 'effect/Option'
import { cursor as journalCursor } from './storage/internal/state.ts'
import * as Outcome from './workflow/Outcome.ts'
// Committed mounts adapted from pi-durable (MIT), pinned 636703a0.
import * as Cause from 'effect/Cause'
import * as Arr from 'effect/Array'
import * as Channel from 'effect/Channel'
import * as PubSub from 'effect/PubSub'
import * as Queue from 'effect/Queue'
import * as RcMap from 'effect/RcMap'
import * as HashMap from 'effect/HashMap'
import * as Ref from 'effect/Ref'
import * as Agent from 'effect-harness/Agent'
import * as Usage from 'effect-harness/Usage'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Layer from 'effect/Layer'
import * as Scope from 'effect/Scope'
import * as Semaphore from 'effect/Semaphore'
import * as Stream from 'effect/Stream'
import * as Conversation from './Conversation.ts'
import * as Document from './Document.ts'
import * as Inbox from './Inbox.ts'
import type * as Observation from './Observation.ts'
import * as Record from './Record.ts'
import { rejected, type StorageError, NotFound, Corrupt, Closed } from './StorageError.ts'
import * as Store from './Store.ts'
import { UsageDoc } from './Usage.ts'
import { findDocument, materialize, visibleEntries } from './storage/internal/state.ts'

/**
 * Schema-derived singleton document values retain their mounted reference identity.
 *
 * @category schemas
 */
export const Documents = Schema.Struct({
  'harness.agent': Schema.optionalKey(Agent.State),
  'harness.live': Schema.optionalKey(Inbox.LiveState),
  'harness.inbox': Schema.optionalKey(Inbox.State),
  'harness.provider': Schema.optionalKey(Conversation.ProviderState),
  'harness.usage': Schema.optionalKey(Usage.State),
})
/**
 * Built-in document values included in a committed conversation view.
 *
 * @category models
 */
export type Documents = typeof Documents.Type
/**
 * Schema for conversation, visible entries and built-in documents from one committed
 * snapshot.
 *
 * @category schemas
 */
export const Value = Schema.Struct({
  conversation: Record.Conversation,
  entries: Schema.Array(Record.Entry),
  docs: Documents,
})
/**
 * Conversation, visible entries and built-in documents from one committed snapshot.
 *
 * @category models
 */
export type Value = View.Value
/**
 * Schema for address of an entry or document field within a conversation view.
 *
 * @category schemas
 */
export const Path = Schema.Array(Schema.Union([Schema.String, Schema.Finite]))
/**
 * Address of an entry or document field within a conversation view.
 *
 * @category models
 */
export type Path = View.Path
/**
 * Schema for structural mutation applied to a committed conversation view.
 *
 * @category schemas
 */
export const Op = Schema.Union([
  Schema.Tuple([Schema.Literal('replace'), Value]),
  Schema.Tuple([Schema.Literal('set'), Path, Schema.Unknown]),
  Schema.Tuple([
    Schema.Literal('delete'),
    Schema.NonEmptyArray(Schema.Union([Schema.String, Schema.Finite])),
  ]),
  Schema.Tuple([
    Schema.Literal('splice'),
    Path,
    Schema.Int,
    Schema.Int,
    Schema.Array(Record.Entry),
  ]),
])
/**
 * Structural mutation applied to a committed conversation view.
 *
 * @category models
 */
export type Op = View.Op
/**
 * Schema for before/after view values and structural operations for a committed update.
 *
 * @category schemas
 */
export const Change = Schema.Struct({
  seq: Record.JournalCursor,
  before: Value,
  value: Value,
  ops: Schema.Array(Op),
  publication: Schema.optionalKey(Record.Frame),
  reset: Schema.Boolean,
  rebased: Schema.optionalKey(Schema.Boolean),
})
/**
 * Before/after view values and structural operations for a committed update.
 *
 * @category models
 */
export type Change = View.Change
/**
 * Structural set values are opaque decoded field values; the JSON client codec validates their wire representation without changing mounted references.
 *
 * @category schemas
 */
export const ChangeJson = Schema.toCodecJson(Change)
const ProjectionWatchTypeId = '~@effect-harness/durable/View/ProjectionWatch'
/**
 * Scoped initial projection and stream of subsequent committed values.
 *
 * @category models
 */
export type ProjectionWatch<A> = View.ProjectionWatch<A>
/**
 * Scoped conversation snapshot and stream of structural changes.
 *
 * @category models
 */
export type Watch = View.Watch
const ProjectionTypeId = '~@effect-harness/durable/View/Projection'
/**
 * Initial, incremental and reset functions for a custom committed view.
 *
 * @category models
 */
export type Projection<A> = View.Projection<A>
/**
 * Live committed view with its latest journal cursor and closing result.
 *
 * @category models
 */
export type State = View.State
/**
 * Shared conversation mounts and scoped committed projections.
 *
 * @category models
 */
export type Service = View.Service
/**
 * Service for coherent committed conversation views and scoped projections.
 *
 * **Details**
 *
 * Views combine visible entries and built-in documents from saved commit frames. Projections
 * for a conversation share a mount and storage driver.
 *
 * **Gotchas**
 *
 * Provider token streams are invisible until committed. Each watch holds at most 100 pending
 * batches; overflow resets to the newest coherent snapshot.
 *
 * @category services
 */
export class View extends Context.Service<View, Service>()('@effect-harness/durable/View') {}

interface MountedDocument {
  readonly id: Record.DocumentId
  readonly version: number
}
interface MountedState {
  readonly value: Value
  readonly seq: Record.Seq | 0
  readonly tasks: HashMap.HashMap<Record.TaskId, Record.Task>
  readonly documents: HashMap.HashMap<string, MountedDocument>
}
interface Envelope {
  readonly type: 'change' | 'resync'
  readonly change: Change
  readonly tasks: ReadonlyArray<Record.Task>
}
interface Mount {
  readonly state: Ref.Ref<MountedState>
  readonly events: PubSub.PubSub<Envelope>
  readonly closed: Deferred.Deferred<Observation.End>
}
const descriptor = <K extends keyof Documents>(
  kind: K,
  token: Document.Document<NonNullable<Documents[K]>>,
) => ({ kind, load: (snapshot: Document.Snapshot) => Document.typed(token, snapshot) })
const mounted = [
  descriptor('harness.agent', Conversation.AgentDoc),
  descriptor('harness.live', Inbox.LiveDoc),
  descriptor('harness.inbox', Inbox.InboxDoc),
  descriptor('harness.provider', Conversation.ProviderDoc),
  descriptor('harness.usage', UsageDoc),
]
const mountedByKind: ReadonlyMap<string, (typeof mounted)[number]> = new Map(
  mounted.map((item) => [item.kind, item]),
)
const own = (object: object, key: string | number, value: unknown) =>
  Object.defineProperty(object, key, {
    value,
    enumerable: true,
    configurable: true,
    writable: true,
  })

/**
 * Replays structural deltas while preserving all unchanged branches and safe own-property keys.
 *
 * @category schemas
 */
function applyUnsafeImpl(self: Value, ops: ReadonlyArray<Op>): Value {
  const value = self
  if (!Schema.is(Schema.Array(Op))(ops)) throw new TypeError('Invalid view operation')
  let result: unknown = value
  const edit = (node: unknown, path: Path, update: (leaf: unknown) => unknown): unknown => {
    if (Arr.isReadonlyArrayEmpty(path)) return update(node)
    if (!Predicate.isObjectOrArray(node)) throw new TypeError('Invalid view operation path')
    const key = path[0]
    if (key === undefined) throw new TypeError('Missing view operation path')
    const copy: object = Array.isArray(node) ? [...node] : { ...node }
    const previous = Object.hasOwn(node, key) ? Reflect.get(node, key) : undefined
    own(copy, key, edit(previous, path.slice(1), update))
    return copy
  }
  for (const op of ops) {
    if (op[0] === 'replace') result = op[1]
    else if (op[0] === 'set') result = edit(result, op[1], () => op[2])
    else if (op[0] === 'splice')
      result = edit(result, op[1], (leaf) => {
        if (!Array.isArray(leaf)) throw new TypeError('View splice requires an array')
        const copy = [...leaf]
        copy.splice(op[2], op[3], ...op[4])
        return copy
      })
    else {
      const key = Arr.lastNonEmpty(op[1])
      result = edit(result, op[1].slice(0, -1), (parent) => {
        if (!Predicate.isObjectOrArray(parent)) throw new TypeError('Invalid view deletion')
        const copy: object = Array.isArray(parent) ? [...parent] : { ...parent }
        if (Array.isArray(copy) && Predicate.isNumber(key)) copy.splice(key, 1)
        else Reflect.deleteProperty(copy, key)
        return copy
      })
    }
  }
  // Every delta is produced from a validated mount and preserves its structural shape.
  return result as Value
}
/**
 * Failure reporting an invalid structural view operation.
 *
 * @category errors
 */
export class ViewOperationError extends Schema.TaggedError<ViewOperationError>(
  '@effect-harness/durable/View/ViewOperationError',
)('ViewOperationError', { message: Schema.String, cause: Schema.Defect() }) {}
const applyImpl = (self: Value, ops: ReadonlyArray<Op>): Result.Result<Value, ViewOperationError> =>
  Result.try({
    try: () => applyUnsafe(self, ops),
    catch: (cause) => new ViewOperationError({ message: 'Invalid view operation', cause }),
  })

const hydrate = Effect.fnUntraced(function* (state: Record.State, id: Record.ConversationId) {
  const conversationOption = Arr.findFirst(state.conversations, (item) => item.id === id)
  if (Option.isNone(conversationOption))
    return yield* rejected('Conversation does not exist', NotFound)
  const conversation = conversationOption.value
  const visible = (yield* visibleEntries(state, id)).toReversed()
  const head = Arr.findLast(visible, (entry) => entry.head !== undefined)
  const cutoff = head.pipe(Option.flatMap((entry) => Option.fromUndefinedOr(entry.head)))
  const range = Arr.filter(visible, (entry) =>
    Option.match(cutoff, { onNone: () => true, onSome: (id) => entry.id >= id }),
  )
  const entries = Option.match(head, {
    onNone: () => range,
    onSome: (head) => [head, ...Arr.filter(range, (entry) => entry.head === undefined)],
  })
  const docs: Documents = {}
  const documents = new Map<string, MountedDocument>()
  for (const item of mounted) {
    const persisted = findDocument(
      state,
      {
        kind: item.kind,
        scope: { _tag: 'conversation', kind: 'conversation', conversationId: id },
      },
      'current',
    )
    if (Option.isNone(persisted)) continue
    const snapshot = yield* materialize(persisted.value, 'current')
    if (Option.isNone(snapshot)) continue
    const converted = yield* item.load(snapshot.value)
    own(docs, item.kind, converted.value)
    documents.set(item.kind, { id: converted.record.id, version: converted.version })
  }
  return {
    value: { conversation, entries, docs },
    documents: HashMap.fromIterable(documents),
    tasks: HashMap.fromIterable(
      Arr.filter(state.tasks, (task) => task.conversationId === id).map((task) => [task.id, task]),
    ),
    seq: yield* journalCursor(state.nextSeq),
  } satisfies MountedState
})
const endingTask = (task: Record.Task) => {
  const generation =
    task.kind === '@effect-harness/durable/Generation/v1' ||
    task.kind === 'harness.generation' ||
    task.kind === 'pi.generation'
  if (generation && task.state.status === 'completing') return true
  if (task.state.status !== 'terminal') return false
  if (generation) return true
  const status = Outcome.classifyTask(task)?.directStatus
  return status === 'failed' || status === 'faulted' || status === 'orphaned'
}
const structuralTouch = (frame: Record.Frame, id: Record.ConversationId) =>
  frame.writes.some((write) => write.type === 'entry' && write.value.conversationId === id) ||
  frame.documents.some(
    (publication) =>
      publication.record.scope.kind === 'conversation' &&
      publication.record.scope.conversationId === id &&
      mountedByKind.has(publication.record.kind) &&
      publication.record.key === undefined &&
      (Arr.isReadonlyArrayNonEmpty(publication.ops) || publication.value === null),
  )
const touches = (frame: Record.Frame, id: Record.ConversationId) =>
  structuralTouch(frame, id) ||
  frame.writes.some(
    (write) =>
      (write.type === 'submission' && write.value.conversationId === id) ||
      (write.type === 'task' && write.value.conversationId === id && endingTask(write.value)),
  )

const advance = Effect.fnUntraced(function* (
  previous: MountedState,
  frame: Record.Frame,
  rebase?: Set<Record.DocumentId>,
) {
  const mount = {
    ...previous,
    documents: new Map(previous.documents),
    tasks: new Map(previous.tasks),
  }
  const id = mount.value.conversation.id
  const docOps: Array<Op> = []
  let rebased = false
  const entryOps: Array<Op> = []
  let entries = mount.value.entries
  for (const publication of frame.documents) {
    if (
      publication.record.scope.kind !== 'conversation' ||
      publication.record.scope.conversationId !== id ||
      publication.record.key !== undefined
    )
      continue
    const item = mountedByKind.get(publication.record.kind)
    if (item === undefined) continue
    const current = mount.documents.get(item.kind)
    const path: Arr.NonEmptyReadonlyArray<string | number> = ['docs', item.kind]
    if (publication.value === null) {
      if (current?.id !== publication.record.id) continue
      mount.documents.delete(item.kind)
      docOps.push(['delete', path])
    } else {
      if (publication.version === undefined)
        return yield* rejected('Publication has no document version', Corrupt)
      const converted = yield* item.load(
        Document.makeSnapshot({
          record: publication.record,
          version: publication.version,
          value: publication.value,
          deltasSinceBase: 0,
        }),
      )
      const replace = rebase?.delete(publication.record.id) === true
      rebased ||= replace
      if (
        !replace &&
        current?.id === publication.record.id &&
        current.version === publication.version
      ) {
        for (const op of publication.ops) {
          if (op[0] === 'replace') docOps.push(['set', path, converted.value])
          else if (op[0] === 'set') docOps.push(['set', [...path, ...op[1]], op[2]])
          else docOps.push(['delete', Arr.appendAll(path, op[1])])
        }
      } else docOps.push(['set', path, converted.value])
      mount.documents.set(item.kind, { id: publication.record.id, version: converted.version })
    }
  }
  const writes = Arr.sortWith(
    Arr.filter(
      frame.writes,
      (write): write is Extract<Record.Write, { type: 'entry' }> =>
        write.type === 'entry' && write.value.conversationId === id,
    ),
    (item: Extract<Record.Write, { type: 'entry' }>) => item.value.id,
    Order.Number,
  )
  for (const { value: entry } of writes) {
    if (entry.head === undefined) {
      entryOps.push(['splice', ['entries'], entries.length, 0, [entry]])
      entries = [...entries, entry]
    } else {
      const found = Arr.findFirstIndex(
        entries,
        (candidate) => candidate.head === undefined && candidate.id >= entry.head!,
      ).pipe(Option.getOrElse(() => -1))
      const kept = found < 0 ? entries.length : found
      entryOps.push(['splice', ['entries'], 0, kept, [entry]])
      entries = [entry, ...entries.slice(kept)]
    }
  }
  for (const write of frame.writes)
    if (write.type === 'task' && write.value.conversationId === id)
      mount.tasks.set(write.value.id, write.value)
  const before = mount.value
  const ops = [...docOps, ...entryOps]
  mount.value = yield* Effect.fromResult(apply(before, ops)).pipe(
    Effect.mapError((error) => rejected(error.message, Corrupt, error.cause)),
  )
  const change: Change = {
    seq: frame.seq,
    before,
    value: mount.value,
    ops,
    publication: frame,
    reset: false,
    rebased,
  }
  return {
    state: {
      ...mount,
      documents: HashMap.fromIterable(mount.documents),
      tasks: HashMap.fromIterable(mount.tasks),
      seq: frame.seq,
    },
    change,
    tasks: [...mount.tasks.values()],
  }
})

/**
 * Scoped committed view service acquisition.
 *
 * @category constructors
 */
export const make: Effect.Effect<Service, never, Store.Store | Scope.Scope> = Effect.gen(
  function* () {
    const store = yield* Store.Store
    const semaphore = yield* Semaphore.make(1)
    const authoritative = yield* Ref.make(Record.emptyState())
    const after = yield* Ref.make<Record.Seq | 0>(0)
    const closed = yield* Ref.make<Observation.End | undefined>(undefined)
    const mounts = yield* RcMap.make({
      lookup: Effect.fnUntraced(function* (
        id: Record.ConversationId,
      ): Effect.fn.Return<Mount, StorageError, Scope.Scope> {
        // Lookup runs inside serialized acquisition/refresh and uses its exact snapshot.
        const state = yield* hydrate(yield* Ref.get(authoritative), id)
        const events = yield* Effect.acquireRelease(PubSub.unbounded<Envelope>(), PubSub.shutdown)
        const terminal = yield* Deferred.make<Observation.End>()
        yield* Effect.addFinalizer(() =>
          Ref.get(closed).pipe(
            Effect.flatMap((reason) => Deferred.succeed(terminal, reason ?? 'cancelled')),
          ),
        )
        return { state: yield* Ref.make<MountedState>(state), events, closed: terminal }
      }),
    })
    const close = Effect.fnUntraced(function* (reason: Observation.End) {
      if ((yield* Ref.getAndSet(closed, reason)) !== undefined) return
      // RcMap exposes a live key iterable; scoped leases can evict entries while closing.
      const ids = [...(yield* RcMap.keys(mounts))]
      for (const id of ids) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const mount = yield* RcMap.get(mounts, id)
            yield* Deferred.succeed(mount.closed, reason)
          }),
        ).pipe(Effect.ignore)
      }
    })
    const refreshUnlocked = Effect.gen(function* () {
      if ((yield* Ref.get(closed)) !== undefined)
        return yield* rejected('View service is closed', Closed)
      const journal = yield* store.journal(yield* Ref.get(after))
      yield* Ref.set(authoritative, journal.state)
      let previousSeq = yield* Ref.get(after)
      const gapped = journal.frames.some((frame) => {
        const gap = frame.seq > previousSeq + 1
        previousSeq = frame.seq
        return gap
      })
      const ids = [...(yield* RcMap.keys(mounts))]
      for (const id of ids) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const mount = yield* RcMap.get(mounts, id)
            let current = yield* Ref.get(mount.state)
            const relevant = Arr.filter(
              journal.frames,
              (frame) => frame.seq > current.seq && touches(frame, id),
            )
            const rebasing = (journal.reset || gapped) && relevant.length > 100
            const rebase = rebasing
              ? new Set(
                  Arr.flatMap(relevant, (frame) =>
                    frame.documents.map((publication) => publication.record.id),
                  ),
                )
              : undefined
            for (const frame of journal.frames) {
              if (frame.seq <= current.seq) continue
              if (touches(frame, id)) {
                const next = yield* advance(current, frame, rebase)
                current = next.state
                yield* Ref.set(mount.state, current)
                yield* PubSub.publish(mount.events, {
                  type: 'change',
                  change: next.change,
                  tasks: next.tasks,
                })
              } else {
                const tasks = new Map(current.tasks)
                for (const write of frame.writes)
                  if (write.type === 'task' && write.value.conversationId === id)
                    tasks.set(write.value.id, write.value)
                current = { ...current, seq: frame.seq, tasks: HashMap.fromIterable(tasks) }
              }
            }
            if (rebasing) {
              const hydrated = yield* hydrate(journal.state, id)
              const change: Change = {
                seq: hydrated.seq,
                before: current.value,
                value: hydrated.value,
                ops: [['replace', hydrated.value]],
                reset: true,
              }
              current = hydrated
              yield* PubSub.publish(mount.events, {
                type: 'resync',
                change,
                tasks: Arr.sortWith(
                  [...HashMap.values(hydrated.tasks)],
                  (item) => item.id,
                  Order.Number,
                ),
              })
            }
            // New subscribers receive the final authoritative sequence and task baseline.
            current = {
              ...current,
              seq: yield* journalCursor(journal.state.nextSeq),
              tasks: HashMap.fromIterable(
                Arr.filter(journal.state.tasks, (task) => task.conversationId === id).map(
                  (task) => [task.id, task],
                ),
              ),
            }
            yield* Ref.set(mount.state, current)
          }),
        )
      }
      yield* Ref.set(after, yield* journalCursor(journal.state.nextSeq))
    })
    yield* Effect.addFinalizer(() => close('cancelled'))
    yield* Effect.forkScoped(
      Effect.forever(
        semaphore.withPermit(refreshUnlocked).pipe(Effect.andThen(Effect.sleep('20 millis'))),
      ).pipe(
        Effect.catch((error) =>
          close(error.reason._tag === 'Closed' ? 'session_closed' : 'listener_error'),
        ),
      ),
    )
    const observe: Service['observe'] = Effect.fnUntraced(function* (id, projection) {
      const scope = yield* Effect.acquireRelease(Scope.make(), (owned, exit) =>
        Scope.close(owned, exit),
      )
      return yield* semaphore
        .withPermit(
          Effect.gen(function* () {
            yield* refreshUnlocked
            const mount = yield* RcMap.get(mounts, id)
            const baseline = yield* Ref.get(mount.state)
            const terminal = yield* Deferred.make<Observation.End>()
            const queue = yield* Queue.make<
              {
                readonly value: Effect.Success<ReturnType<typeof projection.initial>>
                /**
                 * Rebuilds the projection after incremental history is unavailable.
                 */
                readonly reset: boolean
              },
              Cause.Done
            >({ capacity: 100 })
            const delivery = yield* Semaphore.make(1)
            const value = yield* Ref.make(
              yield* projection.initial(
                baseline.value,
                Arr.sortWith([...HashMap.values(baseline.tasks)], (item) => item.id, Order.Number),
              ),
            )
            const ended = yield* Ref.make(false)
            const started = yield* Ref.make(false)
            const finish = Effect.fnUntraced(function* (reason: Observation.End) {
              if (yield* Ref.getAndSet(ended, true)) return
              // Ending discards history immediately; a stopped consumer cannot drain stale values.
              yield* Queue.clear(queue)
              yield* Queue.end(queue)
              yield* Deferred.succeed(terminal, reason)
            }, Effect.uninterruptible)
            const stop = (reason: Observation.End) =>
              finish(reason).pipe(Effect.andThen(Scope.close(scope, Exit.void)))
            yield* Effect.addFinalizer(() => finish('cancelled'))
            const project = Effect.fnUntraced(function* (event: Envelope) {
              if (yield* Ref.get(ended)) return
              if (event.type === 'resync') {
                const pending = yield* Queue.clear(queue)
                if (pending.some((item) => item.reset)) {
                  const next = yield* projection.reset(
                    event.change.value,
                    event.change.seq,
                    event.tasks,
                  )
                  if (!(yield* Ref.get(ended)))
                    yield* Queue.offer(queue, { value: next, reset: true })
                } else if (!(yield* Ref.get(ended))) yield* Queue.offerAll(queue, pending)
                return
              }
              const next = yield* projection.project(event.change)
              if (next === undefined || (yield* Ref.get(ended))) return
              if ((yield* Queue.size(queue)) >= 100) {
                yield* Queue.clear(queue)
                const reset = yield* projection.reset(
                  event.change.value,
                  event.change.seq,
                  event.tasks,
                )
                if (!(yield* Ref.get(ended)))
                  yield* Queue.offer(queue, { value: reset, reset: true })
              } else yield* Queue.offer(queue, { value: next, reset: false })
            }, Semaphore.withPermit(delivery))
            // Immediate startup installs the PubSub subscription before releasing acquisition serialization.
            yield* Stream.runForEach(Stream.fromPubSub(mount.events), project).pipe(
              Effect.catch(() => stop('listener_error')),
              Effect.onExit((exit) =>
                Exit.isFailure(exit)
                  ? stop(Cause.hasInterruptsOnly(exit.cause) ? 'cancelled' : 'listener_error')
                  : Effect.void,
              ),
              Effect.forkIn(scope, { startImmediately: true }),
            )
            // This independent monitor can end a subscriber even while its projection is blocked.
            yield* Deferred.await(mount.closed).pipe(
              Effect.flatMap(stop),
              Effect.forkIn(scope, { startImmediately: true }),
            )
            const changes = Stream.unwrap(
              Effect.gen(function* () {
                if ((yield* Ref.getAndSet(started, true)) || (yield* Ref.get(ended)))
                  return yield* rejected('Watch is stopped or already consumed')
                // Native Stream.fromQueue drains chunks with takeAll, hiding buffered history
                // from Queue.size. Pull one item so the 100 pending-value policy remains exact.
                return Stream.fromChannel(
                  Channel.fromQueue(queue).pipe(Channel.map((next) => [next] as const)),
                ).pipe(
                  Stream.takeWhile(() => !Ref.getUnsafe(ended)),
                  Stream.tap((next) => Ref.set(value, next.value)),
                  Stream.map((next) => next.value),
                )
              }),
            )
            return makeProjectionWatch({
              get value() {
                return Ref.getUnsafe(value)
              },
              changes,
              closed: Deferred.await(terminal),
              stop: stop('stopped'),
              listen: <E, R>(
                listener: (
                  value: Effect.Success<ReturnType<typeof projection.initial>>,
                ) => Effect.Effect<void, E, R>,
              ) =>
                Stream.runForEach(changes, listener).pipe(
                  Effect.catchCause((cause) =>
                    stop(Cause.hasInterruptsOnly(cause) ? 'cancelled' : 'listener_error').pipe(
                      Effect.andThen(Effect.failCause(cause)),
                    ),
                  ),
                  Effect.ensuring(stop('cancelled')),
                ),
            })
          }).pipe(Scope.provide(scope)),
        )
        .pipe(
          Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
        )
    })
    const watch: Service['watch'] = Effect.fnUntraced(function* (id) {
      const subscription = yield* observe<Change>(
        id,
        makeProjection<Change>({
          initial: Effect.fnUntraced(function* (value) {
            return { seq: yield* Ref.get(after), before: value, value, ops: [], reset: false }
          }),
          project: (change) =>
            Effect.succeed(Arr.isReadonlyArrayEmpty(change.ops) ? undefined : change),
          reset: (value, seq) =>
            Effect.succeed({ seq, before: value, value, ops: [['replace', value]], reset: true }),
        }),
      )
      return makeWatch({
        get value() {
          return subscription.value.value
        },
        changes: subscription.changes,
        closed: subscription.closed,
        stop: subscription.stop,
        listen: subscription.listen,
      })
    })
    return View.of({
      observe,
      watch,
      state: Effect.fnUntraced(function* (id) {
        const subscription = yield* watch(id)
        const cursor = yield* Ref.make(0)
        yield* subscription
          .listen(() => Ref.update(cursor, (value) => value + 1))
          .pipe(Effect.ignore, Effect.forkScoped)
        return handle.make(StateProto, {
          get value() {
            return subscription.value
          },
          get cursor() {
            return Ref.getUnsafe(cursor)
          },
          closed: subscription.closed,
        })
      }),
    })
  },
)
/**
 * Provides shared committed views over a domain Store.
 *
 * **Details**
 *
 * The Layer’s Scope owns conversation mounts; each watch also requires its own acquisition
 * Scope.
 *
 * @category layers
 */
export const layer: Layer.Layer<View, never, Store.Store> = Layer.effect(View, make)

/**
 * Creates a projection watch with live getters and shared inspection.
 *
 * @category constructors
 */
export const makeProjectionWatch = <A>(
  input: handle.Input<ProjectionWatch<A>, typeof ProjectionWatchTypeId>,
): ProjectionWatch<A> => {
  const value = handle.make(
    ProjectionWatchProto,
    handle.marked(input, ProjectionWatchTypeId, { _A: identity }),
  )
  return value
}

/**
 * Checks whether a value carries the nominal `ProjectionWatch` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isProjectionWatch = (input: unknown): input is ProjectionWatch<unknown> =>
  Predicate.hasProperty(input, ProjectionWatchTypeId)

/**
 * Creates a typed conversation projection.
 *
 * @category constructors
 */
export const makeProjection = <A>(
  input: Omit<Projection<A>, typeof ProjectionTypeId>,
): Projection<A> => {
  const value = Object.assign({}, input, { [ProjectionTypeId]: { _A: identity } })
  Object.defineProperties(value, Object.getOwnPropertyDescriptors(input))
  Object.defineProperty(value, ProjectionTypeId, { enumerable: false })
  return value
}
/**
 * Checks whether a value carries the nominal `Projection` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isProjection = (input: unknown): input is Projection<unknown> =>
  Predicate.hasProperty(input, ProjectionTypeId)

/**
 * Creates a watch handle with live getters and shared inspection.
 *
 * @category constructors
 */
export const makeWatch = (input: handle.Input<Watch, typeof ProjectionWatchTypeId>): Watch => {
  const value = handle.make(
    ProjectionWatchProto,
    handle.marked(input, ProjectionWatchTypeId, { _A: identity }),
  )
  return value
}

/**
 * Validates and replays view operations while preserving unchanged branches.
 *
 * @category combinators
 */
export const apply: {
  (ops: ReadonlyArray<Op>): (self: Value) => Result.Result<Value, ViewOperationError>
  (self: Value, ops: ReadonlyArray<Op>): Result.Result<Value, ViewOperationError>
} = dual(2, applyImpl)

/**
 * Type-level contracts for `View`.
 *
 * @category utility types
 */
export declare namespace View {
  /**
   * Conversation, visible entries and built-in documents from one committed snapshot.
   *
   * @category models
   */
  export type Value = typeof Value.Type
  /**
   * Address of an entry or document field within a conversation view.
   *
   * @category models
   */
  export type Path = typeof Path.Type
  /**
   * Structural mutation applied to a committed conversation view.
   *
   * @category models
   */
  export type Op = typeof Op.Type
  /**
   * Before/after view values and structural operations for a committed update.
   *
   * @category models
   */
  export type Change = typeof Change.Type
  /**
   * Scoped initial projection and single-consumer stream of committed updates.
   *
   * **Details**
   *
   * value is the initial projection; changes contains subsequent deliveries. listen joins the
   * listener lifetime. stop ends delivery with a stopped result; Scope still owns resource
   * release.
   *
   * **Gotchas**
   *
   * Process batches in order. A slow watch may receive a reset instead of every intermediate
   * commit. Listener failures close that watch.
   *
   * @category models
   */
  export interface ProjectionWatch<out A> extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly [ProjectionWatchTypeId]: { readonly _A: Types.Covariant<A> }
    readonly value: A
    readonly changes: Stream.Stream<A, StorageError>
    readonly closed: Effect.Effect<Observation.End>
    /** effect-review-allow P3-scope-in-r-not-dispose-method: semantic subscription completion stops future deliveries and resolves closed as stopped; resource release remains owned by Scope. */
    readonly stop: Effect.Effect<void>
    readonly listen: <E, R>(
      listener: (value: A) => Effect.Effect<void, E, R>,
    ) => Effect.Effect<void, E | StorageError, R>
  }
  /**
   * Scoped conversation snapshot and stream of structural changes.
   *
   * @category models
   */
  export interface Watch extends Omit<ProjectionWatch<Change>, 'value'> {
    readonly value: Value
  }
  /**
   * Functions projecting an initial snapshot, incremental change and reset.
   *
   * **Details**
   *
   * initial receives the committed view and tasks. project may return undefined to suppress a
   * delivery. reset reconstructs the projection when retained incremental history is
   * unavailable.
   *
   * @category models
   */
  export interface Projection<out A> {
    readonly [ProjectionTypeId]: { readonly _A: Types.Covariant<A> }
    /**
     * Builds the projection from the committed view and task snapshot.
     */
    readonly initial: (
      view: Value,
      tasks: ReadonlyArray<Record.Task>,
    ) => Effect.Effect<A, StorageError>
    /**
     * Projects an incremental change; undefined suppresses a delivery.
     */
    readonly project: (change: Change) => Effect.Effect<A | undefined, StorageError>
    /**
     * Rebuilds the projection after incremental history is unavailable.
     */
    readonly reset: (
      view: Value,
      seq: Record.Seq | 0,
      tasks: ReadonlyArray<Record.Task>,
    ) => Effect.Effect<A, StorageError>
  }
  /**
   * Live committed view with its latest journal cursor and closing result.
   *
   * @category models
   */
  export interface State extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly value: Value
    readonly cursor: number
    readonly closed: Effect.Effect<Observation.End>
  }
  /**
   * Shared conversation mounts and scoped committed projections.
   *
   * @category models
   */
  export interface Service {
    /**
     * Acquires a custom projection over the conversation’s shared committed mount.
     */
    readonly observe: <A>(
      id: Record.ConversationId,
      projection: Projection<A>,
    ) => Effect.Effect<ProjectionWatch<A>, StorageError, Scope.Scope>
    /**
     * Acquires a committed initial conversation view and subsequent structural change batches in
     * the caller’s Scope.
     */
    readonly watch: (id: Record.ConversationId) => Effect.Effect<Watch, StorageError, Scope.Scope>
    /**
     * Maintains a scoped live conversation value and journal cursor.
     */
    readonly state: (id: Record.ConversationId) => Effect.Effect<State, StorageError, Scope.Scope>
  }
}

/**
 * Replays view operations or throws for an invalid operation.
 *
 * @category combinators
 */
export const applyUnsafe: {
  (ops: ReadonlyArray<Op>): (self: Value) => Value
  (self: Value, ops: ReadonlyArray<Op>): Value
} = dual(2, applyUnsafeImpl)
