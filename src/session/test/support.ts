import { Effect, PubSub } from 'effect'
import type { Scope } from 'effect'
import { Storage, StorageError } from '../../storage/index.ts'
import type { Ids, Task, Write } from '../../storage/schema/index.ts'
import { Session } from '../index.ts'

/** A Storage that records every committed batch and can be told to fail its next commits. */
export interface Recorder {
  readonly storage: Storage.Service
  readonly commits: Array<ReadonlyArray<Write.StorageWrite>>
  /** Make every later commit fail with a `StorageError`. */
  readonly failCommits: () => void
}

export const record = (backend: Storage.Service): Recorder => {
  const commits: Array<ReadonlyArray<Write.StorageWrite>> = []
  let failing = false
  return {
    storage: {
      ...backend,
      commit: (writes) =>
        failing
          ? Effect.fail(new StorageError.StorageError({ message: 'disk full' }))
          : Effect.tap(backend.commit(writes), () =>
              Effect.sync(() => {
                commits.push(writes)
              }),
            ),
    },
    commits,
    failCommits: () => {
      failing = true
    },
  }
}

export interface Harness {
  readonly session: Session.Service
  /** The memory store behind the Session, for reads outside it. */
  readonly storage: Storage.Service
  readonly recorder: Recorder
}

/** Open a Session over a recorded memory store. */
export const openSession = Effect.gen(function* () {
  const storage = yield* Storage.makeMemory
  const recorder = record(storage)
  const session = yield* Session.make.pipe(Effect.provideService(Storage.Storage, recorder.storage))
  const harness: Harness = { session, storage, recorder }
  return harness
})

/** Run a test body against a fresh Session; any failure it leaves unhandled is a defect. */
export const runSession = <A, E>(
  body: (harness: Harness) => Effect.Effect<A, E, Scope.Scope>,
): Promise<A> => Effect.runPromise(Effect.scoped(Effect.orDie(Effect.flatMap(openSession, body))))

export const Work: Session.TaskKind<{ readonly path: string }> = {
  name: 'test.work',
  version: 1,
  initial: (input) => ({ phase: 'start', path: input.path }),
}

/** Commit an ownerless conversation and return its ID. */
export const createConversation = (session: Session.Service) =>
  session.commit((tx) =>
    Effect.map(tx.createConversation({ ownership: { kind: 'ownerless' } }), ({ id }) => id),
  )

/** Commit a conversation-owned task. */
export const createTask = (session: Session.Service, conversationId: Ids.ConversationId) =>
  session.commit((tx) =>
    tx.createTask(Work, { path: 'a' }, { ownership: { kind: 'conversation' }, conversationId }),
  )

/** The committed record of a task that must exist. */
export const readTask = (session: Session.Service, id: Ids.TaskId) =>
  Effect.orDie(
    Effect.flatMap(
      session.commit((tx) => tx.task(id)),
      Effect.fromNullishOr,
    ),
  )

/** The same task record, terminal with a completed outcome. */
export const terminal = (task: Task.TaskRecord): Task.TaskRecord => ({
  id: task.id,
  conversationId: task.conversationId,
  kind: task.kind,
  version: task.version,
  input: task.input,
  background: task.background,
  abortRequested: task.abortRequested,
  state: { status: 'terminal', outcome: { status: 'completed', result: { ok: true } } },
})

/** Commit `change` while subscribed, and return its result with its publication. */
export const published = <A, E, R>(
  session: Session.Service,
  change: (tx: Session.Tx) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const subscription = yield* session.subscribe
      const result = yield* session.commit(change)
      return { result, publication: yield* PubSub.take(subscription) }
    }),
  )
