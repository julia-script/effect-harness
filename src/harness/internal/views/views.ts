/**
 * The view mounts of one Harness, ported from pi's `ConversationViews` and `TaskGraphView`: at
 * most one per conversation and one task graph, built in a commit by their first observer and
 * dropped with their last. Every mount advances from the Session's commit publications, on the
 * line, so an observer attached in a commit sees every later commit and nothing earlier.
 */
import { Effect } from 'effect'
import type { Schema, Scope } from 'effect'
import type { CommitPublication, Service as SessionService, Tx } from '../../../session/Session.ts'
import { SessionFailed } from '../../../session/SessionError.ts'
import type { CommitError, ReadAfterWrite } from '../../../session/SessionError.ts'
import type { Op } from '../../../storage/schema/Document.ts'
import type { ConversationId, TaskId } from '../../../storage/schema/Ids.ts'
import type { AgentEvent, Events } from '../../../protocol/AgentEvent.ts'
import type { Change, ConversationView, TaskGraph, Watch } from '../../../protocol/ViewState.ts'
import { readsFirst } from '../../../internal/readsFirst.ts'
import { RUN_KIND } from '../run.ts'
import * as Conversation from './conversation.ts'
import { snapshotOf } from './events.ts'
import * as Feed from './feed.ts'
import * as Graph from './graph.ts'
import { translate } from './translate.ts'

/** Takes each publication after its mount took it: the value before and after, and the ops. */
interface Observer<T> {
  readonly advance: (
    before: T,
    after: T,
    ops: ReadonlyArray<Op>,
    publication: CommitPublication,
  ) => Effect.Effect<void>
  readonly end: (failure?: SessionFailed) => Effect.Effect<void>
  readonly close: Effect.Effect<void>
}

interface Mount<T> {
  readonly observers: Set<Observer<T>>
}

type ViewMount = Mount<ConversationView> & { readonly view: Conversation.View }
type GraphMount = Mount<TaskGraph> & { graph: TaskGraph }

export interface Views {
  readonly view: (
    id: ConversationId,
  ) => Effect.Effect<Watch<ConversationView, SessionFailed>, CommitError, Scope.Scope>
  readonly events: (
    id: ConversationId,
  ) => Effect.Effect<Events<SessionFailed>, CommitError, Scope.Scope>
  readonly taskGraph: Effect.Effect<Watch<TaskGraph, SessionFailed>, CommitError, Scope.Scope>
}

const notify = <T>(
  mount: Mount<T>,
  before: T,
  after: T,
  ops: ReadonlyArray<Op>,
  publication: CommitPublication,
) =>
  Effect.forEach([...mount.observers], (observer) =>
    observer.advance(before, after, ops, publication),
  )

/** A watch observer: each change with operations, collapsing to a replacement on overflow. */
const watchOf = <T extends Schema.Json>(value: T, current: () => T) =>
  Effect.map(
    Feed.make<Change<T>>(() => {
      const latest = current()
      return { value: latest, ops: [['r', latest]] }
    }),
    (feed) => {
      const observer: Observer<T> = {
        advance: (_before, after, ops) =>
          ops.length === 0 ? Effect.void : feed.offer({ value: after, ops }),
        end: feed.end,
        close: feed.close,
      }
      const watch: Watch<T, SessionFailed> = { value, changes: feed.stream }
      return { observer, observation: watch }
    },
  )

/** An events observer: one batch per commit that causes events; a snapshot on overflow. */
const eventsOf = (id: ConversationId, view: Conversation.View, held: Set<TaskId>) =>
  Effect.map(
    Feed.make<ReadonlyArray<AgentEvent>>(() => [snapshotOf(view.value)]),
    (feed) => {
      const observer: Observer<ConversationView> = {
        advance: (before, after, ops, publication) => {
          const events = translate(id, before, after, ops, publication, held)
          return events.length === 0 ? Effect.void : feed.offer(events)
        },
        end: feed.end,
        close: feed.close,
      }
      const events: Events<SessionFailed> = {
        snapshot: snapshotOf(view.value),
        batches: feed.stream,
      }
      return { observer, observation: events }
    },
  )

/** Generations of `id` whose held outcome already ended their turn. */
const completing = (tx: Tx, id: ConversationId) =>
  Effect.map(
    tx.scanTasks({ conversationId: id, kind: RUN_KIND, status: 'completing' }, 1000),
    (page) => new Set(page.items.map((record) => record.id)),
  )

interface Attached<O, T> {
  readonly observer: Observer<T>
  readonly observation: O
}

/**
 * Attach in a commit what `create` makes of the mount at `key`, found or built there, until the
 * scope closes; the commit writes nothing. The mount leaves `mounts` with its last observer.
 */
const attach = <K, T, M extends Mount<T>, O, E>(
  session: SessionService,
  mounts: Map<K, M>,
  key: K,
  build: (tx: Tx) => Effect.Effect<M, E>,
  create: (mount: M, tx: Tx) => Effect.Effect<Attached<O, T>, E>,
): Effect.Effect<O, Exclude<E, ReadAfterWrite> | CommitError, Scope.Scope> => {
  const acquire = readsFirst(
    session.commit((tx) =>
      Effect.gen(function* () {
        const mount = mounts.get(key) ?? (yield* build(tx))
        const attached = yield* create(mount, tx)
        mounts.set(key, mount)
        mount.observers.add(attached.observer)
        return { mount, attached }
      }),
    ),
  )
  return Effect.map(
    Effect.acquireRelease(acquire, ({ mount, attached }) =>
      Effect.suspend(() => {
        mount.observers.delete(attached.observer)
        if (mount.observers.size === 0 && mounts.get(key) === mount) mounts.delete(key)
        return attached.observer.close
      }),
    ),
    ({ attached }) => attached.observation,
  )
}

/** End every observer once the Session ended; a failure fails them. */
const endAll = (session: SessionService, observers: () => ReadonlyArray<Observer<never>>) =>
  Effect.flatMap(session.ended, (end) => {
    const failure =
      end.reason === 'failed'
        ? new SessionFailed({
            message: 'Session failed after a storage error; reopen it',
            cause: end.error,
          })
        : undefined
    return Effect.forEach(observers(), (observer) => observer.end(failure), { discard: true })
  })

interface Mounts {
  readonly views: Map<ConversationId, ViewMount>
  readonly graphs: Map<'graph', GraphMount>
}

/** Advance every mount by one publication, and hand it to their observers. */
const advanceAll = (mounts: Mounts) =>
  Effect.fnUntraced(function* (publication: CommitPublication) {
    for (const [id, mount] of mounts.views) {
      const before = mount.view.value
      const ops = Conversation.advance(id, mount.view, publication)
      yield* notify(mount, before, mount.view.value, ops, publication)
    }
    for (const mount of mounts.graphs.values()) {
      const before = mount.graph
      const next = Graph.advance(before, publication)
      mount.graph = next.graph
      yield* notify(mount, before, next.graph, next.ops, publication)
    }
  })

const buildView = (id: ConversationId) => (tx: Tx) =>
  Effect.map(Conversation.build(tx, id), (view): ViewMount => ({ view, observers: new Set() }))

const buildGraph = (tx: Tx) =>
  Effect.map(Graph.build(tx), (graph): GraphMount => ({ graph, observers: new Set() }))

type AttachError = CommitError | ReadAfterWrite

const attachView = <O>(
  session: SessionService,
  views: Mounts['views'],
  id: ConversationId,
  create: (mount: ViewMount, tx: Tx) => Effect.Effect<Attached<O, ConversationView>, AttachError>,
) =>
  attach<ConversationId, ConversationView, ViewMount, O, AttachError>(
    session,
    views,
    id,
    buildView(id),
    create,
  )

const service = (session: SessionService, { views, graphs }: Mounts): Views => ({
  view: (id) =>
    attachView(session, views, id, (mount) => watchOf(mount.view.value, () => mount.view.value)),
  events: (id) =>
    attachView(session, views, id, (mount, tx) =>
      Effect.flatMap(completing(tx, id), (held) => eventsOf(id, mount.view, held)),
    ),
  taskGraph: attach<'graph', TaskGraph, GraphMount, Watch<TaskGraph, SessionFailed>, AttachError>(
    session,
    graphs,
    'graph',
    buildGraph,
    (mount) => watchOf(mount.graph, () => mount.graph),
  ),
})

/** The view mounts of `session`, advanced while the scope is open; closing it ends them. */
export const make = Effect.fnUntraced(function* (session: SessionService) {
  const mounts: Mounts = { views: new Map(), graphs: new Map() }
  yield* session.observeCommits(advanceAll(mounts))
  const observers = (): ReadonlyArray<Observer<never>> => {
    const all: Array<Observer<never>> = []
    for (const mount of mounts.views.values()) all.push(...mount.observers)
    for (const mount of mounts.graphs.values()) all.push(...mount.observers)
    return all
  }
  yield* Effect.forkScoped(endAll(session, observers))
  yield* Effect.addFinalizer(() =>
    Effect.forEach(observers(), (observer) => observer.end(), { discard: true }),
  )
  return service(session, mounts)
})
