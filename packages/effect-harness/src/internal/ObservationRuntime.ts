/** Mounts publish only from Session's trusted post-adoption line. No observer pumps. */
import type * as Cause from 'effect/Cause'
import * as Effect from 'effect/Effect'
import * as Queue from 'effect/Queue'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import type * as Record from '../Record.js'
import type * as Session from '../Session.js'
import type * as Transaction from '../Transaction.js'
import * as Observation from '../Observation.js'
import { ObservationError } from '../ObservationError.js'
import * as Graph from '../TaskGraph.js'
import * as View from '../ConversationView.js'
import * as Events from '../AgentEvent.js'
import * as Runtime from './SessionRuntime.js'
import * as NativeTransaction from './TransactionRuntime.js'
import * as GraphProjection from './TaskGraph.js'
import * as Views from './ConversationViews.js'
import * as EventProjection from './AgentEvents.js'
import * as Value from './DocumentValue.js'

interface Subscriber<F> {
  readonly queue: Queue.Queue<F, Observation.Failure | Cause.Done>
  taken: number
  active: boolean
  rebasing: boolean
}
interface Channel<F> {
  readonly subscribers: Set<Subscriber<F>>
  readonly cursor: (frame: F) => number
  readonly rebase: () => F
}
interface Base {
  readonly publish: (
    commit: Session.Commit,
    documents: ReadonlyMap<Record.DocumentId, Record.StoredDocument | null>,
  ) => Effect.Effect<void>
  readonly close: (error?: Observation.Failure) => Effect.Effect<void>
}
interface GraphMount extends Base {
  readonly kind: 'graph'
  readonly channel: Channel<Graph.Frame>
  readonly snapshot: () => Graph.Snapshot
}
interface Packet {
  readonly view: View.Frame
  readonly events: Events.Frame
}
interface ViewMount extends Base {
  readonly kind: 'view'
  readonly channel: Channel<Packet>
  readonly snapshot: () => View.Snapshot
}
export type Mount = GraphMount | ViewMount
export interface Attachment {
  readonly snapshot: View.Snapshot
  readonly views: Stream.Stream<View.Frame, Observation.Failure>
  readonly events: Stream.Stream<Events.Frame, Observation.Failure>
  readonly detach: Effect.Effect<void>
}
const projectionFailure = (cause: Cause.Cause<Observation.Failure>) =>
  new ObservationError({
    reason: 'projection',
    operation: 'observation.publish',
    message: 'The committed observation projection failed',
    cause,
  })
const offer = <F>(channel: Channel<F>, frame: F) =>
  Effect.gen(function* () {
    for (const subscriber of channel.subscribers) {
      if (!subscriber.active) continue
      if (subscriber.rebasing || !(yield* Queue.offer(subscriber.queue, frame))) {
        subscriber.rebasing = true
        yield* Queue.clear(subscriber.queue).pipe(Effect.catchCause(() => Effect.succeed([])))
        yield* Queue.offer(subscriber.queue, channel.rebase())
      }
    }
  })
const finish = <F>(channel: Channel<F>, error?: Observation.Failure) =>
  Effect.gen(function* () {
    for (const subscriber of channel.subscribers) {
      if (!subscriber.active) continue
      if (error !== undefined) yield* Queue.fail(subscriber.queue, error)
      else {
        const latest = channel.rebase()
        if (channel.cursor(latest) > subscriber.taken) {
          yield* Queue.clear(subscriber.queue).pipe(Effect.catchCause(() => Effect.succeed([])))
          yield* Queue.offer(subscriber.queue, latest)
        }
        yield* Queue.end(subscriber.queue)
      }
    }
  })
const register = <F>(
  runtime: Runtime.State,
  key: string,
  mount: Mount,
  channel: Channel<F>,
  initial: F,
  scope: Scope.Scope,
  capacity: number,
) =>
  Effect.gen(function* () {
    const queue = yield* Queue.dropping<F, Observation.Failure | Cause.Done>(capacity)
    const subscriber: Subscriber<F> = {
      queue,
      taken: channel.cursor(initial),
      active: true,
      rebasing: false,
    }
    const detach = Effect.gen(function* () {
      subscriber.active = false
      channel.subscribers.delete(subscriber)
      if (channel.subscribers.size === 0 && runtime.observations.get(key) === mount)
        runtime.observations.delete(key)
      yield* Queue.shutdown(queue)
    })
    channel.subscribers.add(subscriber)
    runtime.observations.set(key, mount)
    yield* Scope.addFinalizer(scope, detach)
    const pending = Stream.fromPull(
      Effect.succeed(
        Queue.take(queue).pipe(
          Effect.map((frame) => {
            subscriber.taken = channel.cursor(frame)
            subscriber.rebasing = false
            return [frame] as const
          }),
        ),
      ),
    )
    // Initial state is outside the pending buffer, so overflow before the first pull cannot replace it.
    return { stream: Stream.concat(Stream.succeed(initial), pending), detach }
  })
const seq = (commitSeq: Session.Commit['seq'] | undefined) =>
  commitSeq === undefined ? {} : { commitSeq }
const graphMount = Effect.fnUntraced(function* (runtime: Runtime.State) {
  const tasks = new Map(
    (yield* runtime.storage.scanTasks({}).pipe(Stream.runCollect)).map((t) => [t.id, t]),
  )
  const conversations = new Map(
    (yield* runtime.storage.scanConversations({}).pipe(Stream.runCollect)).map((c) => [c.id, c]),
  )
  const epoch = globalThis.crypto.randomUUID()
  let cursor = 0
  let commitSeq = runtime.observationCommitSeq
  let value = GraphProjection.project(tasks, conversations)
  const snapshot = (): Graph.Snapshot => ({
    _tag: 'Snapshot',
    protocol: 1,
    target: { _tag: 'sessionGraph' },
    revision: { epoch, cursor },
    ...seq(commitSeq),
    value,
  })
  const channel: Channel<Graph.Frame> = {
    subscribers: new Set(),
    cursor: (f) => (f._tag === 'Delta' ? f.to : f.revision.cursor),
    rebase: () => ({ ...snapshot(), _tag: 'Rebase' }),
  }
  const mount: GraphMount = {
    kind: 'graph',
    channel,
    snapshot,
    close: (error) => finish(channel, error),
    publish: (commit) =>
      Effect.gen(function* () {
        for (const write of commit.changes)
          if (write._tag === 'task') tasks.set(write.value.id, write.value)
        for (const write of commit.changes)
          if (write._tag === 'conversation') conversations.set(write.value.id, write.value)
        const next = GraphProjection.project(tasks, conversations)
        if (Schema.toEquivalence(Graph.ValueSchema)(value, next)) return
        value = next
        commitSeq = commit.seq
        const from = cursor++
        yield* offer(channel, {
          _tag: 'Delta',
          protocol: 1,
          target: { _tag: 'sessionGraph' },
          epoch,
          from,
          to: cursor,
          commitSeq: commit.seq,
          ops: [{ _tag: 'Replace', value }],
        })
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            if (runtime.observations.get('graph') === mount) runtime.observations.delete('graph')
            yield* finish(channel, projectionFailure(cause))
          }),
        ),
      ),
  }
  return mount
})
const viewMount = Effect.fnUntraced(function* (runtime: Runtime.State, id: Record.ConversationId) {
  const facts = yield* Views.hydrate(runtime, id)
  const epoch = globalThis.crypto.randomUUID()
  let cursor = 0
  let commitSeq = runtime.observationCommitSeq
  let value = yield* Views.project(facts)
  const snapshot = (): View.Snapshot => ({
    _tag: 'Snapshot',
    protocol: 1,
    target: { _tag: 'conversation', conversationId: id },
    revision: { epoch, cursor },
    ...seq(commitSeq),
    value,
  })
  const rebase = (): Packet => {
    const state = { ...snapshot(), _tag: 'Rebase' as const }
    return { view: state, events: state }
  }
  const channel: Channel<Packet> = {
    subscribers: new Set(),
    cursor: (f) => (f.view._tag === 'Delta' ? f.view.to : f.view.revision.cursor),
    rebase,
  }
  const mount: ViewMount = {
    kind: 'view',
    channel,
    snapshot,
    close: (error) => finish(channel, error),
    publish: (commit, documents) =>
      Effect.gen(function* () {
        Views.advance(facts, commit, documents)
        const next = yield* Views.project(facts)
        if (Schema.toEquivalence(View.ValueSchema)(value, next)) return
        const events = EventProjection.translate(value, next)
        value = next
        commitSeq = commit.seq
        const from = cursor++
        const fields = {
          protocol: 1 as const,
          target: { _tag: 'conversation' as const, conversationId: id },
          epoch,
          from,
          to: cursor,
          commitSeq: commit.seq,
        }
        yield* offer(channel, {
          view: { _tag: 'Delta', ...fields, ops: [{ _tag: 'Replace', value }] },
          events: { _tag: 'Batch', ...fields, events, value },
        })
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            if (runtime.observations.get(`conversation:${id}`) === mount)
              runtime.observations.delete(`conversation:${id}`)
            yield* finish(channel, projectionFailure(cause))
          }),
        ),
      ),
  }
  return mount
})
const findView = Effect.fnUntraced(function* (runtime: Runtime.State, id: Record.ConversationId) {
  const current = runtime.observations.get(`conversation:${id}`)
  return current?.kind === 'view' ? current : yield* viewMount(runtime, id)
})
const optionsCapacity = Effect.fnUntraced(function* (options?: Observation.WatchOptions) {
  const owned = yield* Value.copy(Observation.WatchOptionsSchema, options ?? {})
  return owned.capacity ?? Observation.defaultCapacity
})
const attach = Effect.fnUntraced(function* (
  runtime: Runtime.State,
  id: Record.ConversationId,
  scope: Scope.Scope,
  options?: Observation.WatchOptions,
): Effect.fn.Return<Attachment, Observation.Failure> {
  yield* Runtime.open(runtime)
  const capacity = yield* optionsCapacity(options)
  const mount = yield* findView(runtime, id)
  const snapshot = mount.snapshot()
  const subscription = yield* register(
    runtime,
    `conversation:${id}`,
    mount,
    mount.channel,
    { view: snapshot, events: snapshot },
    scope,
    capacity,
  )
  return {
    snapshot: yield* Value.copy(View.SnapshotSchema, snapshot),
    views: subscription.stream.pipe(
      Stream.mapEffect((packet) => Value.copy(View.FrameSchema, packet.view)),
    ),
    events: subscription.stream.pipe(
      Stream.mapEffect((packet) => Value.copy(Events.FrameSchema, packet.events)),
    ),
    detach: subscription.detach,
  }
})
/** The caller already holds Session's line. Never recursively enter a Session operation. */
export const attachObservation = (
  session: Session.Session,
  tx: Transaction.Transaction,
  id: Record.ConversationId,
  consumerScope: Scope.Scope,
  options?: Observation.WatchOptions,
) =>
  NativeTransaction.use(tx, (transaction) =>
    Effect.gen(function* () {
      const runtime = yield* Runtime.get(session)
      if (transaction.runtime !== runtime)
        return yield* new ObservationError({
          reason: 'invalid',
          operation: 'observation.attach',
          message: 'Attachment requires the matching native transaction',
        })
      return yield* attach(runtime, id, consumerScope, options)
    }),
  )
export const make = (session: Session.Session): Observation.Capabilities => ({
  graphSnapshot: () =>
    Runtime.line(session, 'graph.snapshot', (runtime) =>
      Effect.gen(function* () {
        const existing = runtime.observations.get('graph')
        const mount = existing?.kind === 'graph' ? existing : yield* graphMount(runtime)
        return yield* Value.copy(Graph.SnapshotSchema, mount.snapshot())
      }),
    ),
  watchGraph: (options) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const scope = yield* Scope.Scope
        return yield* Runtime.line(session, 'graph.watch', (runtime) =>
          Effect.gen(function* () {
            const capacity = yield* optionsCapacity(options)
            const existing = runtime.observations.get('graph')
            const mount = existing?.kind === 'graph' ? existing : yield* graphMount(runtime)
            const subscriber = yield* register(
              runtime,
              'graph',
              mount,
              mount.channel,
              mount.snapshot(),
              scope,
              capacity,
            )
            return subscriber.stream.pipe(
              Stream.mapEffect((frame) => Value.copy(Graph.FrameSchema, frame)),
            )
          }),
        )
      }),
    ),
  conversationSnapshot: (id) =>
    Runtime.line(session, 'view.snapshot', (runtime) =>
      findView(runtime, id).pipe(
        Effect.flatMap((mount) => Value.copy(View.SnapshotSchema, mount.snapshot())),
      ),
    ),
  watchView: (id, options) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const scope = yield* Scope.Scope
        const attached = yield* Runtime.line(session, 'view.watch', (runtime) =>
          attach(runtime, id, scope, options),
        )
        return attached.views
      }),
    ),
  watchEvents: (id, options) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const scope = yield* Scope.Scope
        const attached = yield* Runtime.line(session, 'events.watch', (runtime) =>
          attach(runtime, id, scope, options),
        )
        return attached.events
      }),
    ),
})
