import { Cause, Data, Deferred, Effect, Exit, Logger, Schedule, Scope } from 'effect'
import type { Schema } from 'effect'
import { Session } from '../session/index.ts'
import { Storage } from '../storage/index.ts'
import type { Ids } from '../storage/schema/index.ts'
import { Registry, Scheduler, Task } from '../task/index.ts'

/** A failure task code raises on purpose. */
export class Boom extends Data.TaggedError('Boom')<{ readonly message: string }> {}

export type Step = { readonly phase: 'run' }
export type StepRuntime<R extends Schema.Json> = Task.Runtime<null, Step, R>

export const completed = <R>(result: R) =>
  ({ status: 'terminal', outcome: { status: 'completed', result } }) as const

export const abortedWith = (reason: string) =>
  ({ status: 'terminal', outcome: { status: 'aborted', reason } }) as const

/** A one-phase task; the default abort handler settles `aborted` with reason `test`. */
export const oneStep = <R extends Schema.Json = null, E1 = never, E2 = Task.RuntimeError>(
  name: string,
  run: (task: Task.RunningTask<null, Step>, runtime: StepRuntime<R>) => Effect.Effect<void, E1>,
  abort?: (runtime: StepRuntime<R>) => Effect.Effect<void, E2>,
) =>
  Task.make<null, Step, R>({
    name,
    version: 1,
    initial: () => ({ phase: 'run' }),
    phases: { run },
    abort: (_task, runtime) =>
      abort === undefined
        ? runtime.commit(() => Effect.succeed(abortedWith('test')))
        : abort(runtime),
  })

export interface Opened {
  readonly session: Session.Service
  readonly registry: Registry.Service
  readonly scheduler: Scheduler.Service
}

/** Open a Session, a Registry holding `tasks`, and a Scheduler over `storage` in the scope. */
export const open = (
  storage: Storage.Service,
  tasks: ReadonlyArray<Task.Any> = [],
  registry?: Registry.Service,
): Effect.Effect<Opened, never, Scope.Scope> =>
  Effect.gen(function* () {
    const session = yield* Session.make
    const opened = registry ?? (yield* Registry.make(tasks))
    const scheduler = yield* Scheduler.make.pipe(
      Effect.provideService(Session.Session, session),
      Effect.provideService(Registry.Registry, opened),
    )
    return { session, registry: opened, scheduler }
  }).pipe(Effect.provideService(Storage.Storage, storage), Effect.orDie)

export interface Closable extends Opened {
  /** Close the Scheduler and the Session, as closing their scope does. */
  readonly close: Effect.Effect<void>
}

/** `open` in a scope of its own, closed by `close`: a later `open` over `storage` reopens. */
export const openClosable = (
  storage: Storage.Service,
  tasks: ReadonlyArray<Task.Any> = [],
  registry?: Registry.Service,
): Effect.Effect<Closable> =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const opened = yield* Scope.provide(open(storage, tasks, registry), scope)
    return { ...opened, close: Scope.close(scope, Exit.void) }
  })

/** Commit an ownerless conversation and return its ID. */
export const conversation = (session: Session.Service) =>
  Effect.orDie(
    session.commit((tx) =>
      Effect.map(tx.createConversation({ ownership: { kind: 'ownerless' } }), ({ id }) => id),
    ),
  )

/** Commit a conversation-owned task. */
export const createTask = <I extends Schema.Json>(
  session: Session.Service,
  conversationId: Ids.ConversationId,
  kind: Session.TaskKind<I>,
  input: I,
) =>
  Effect.orDie(
    session.commit((tx) =>
      tx.createTask(kind, input, { ownership: { kind: 'conversation' }, conversationId }),
    ),
  )

/** The committed record of a task. */
export const getTask = (session: Session.Service, id: Ids.TaskId) =>
  Effect.orDie(session.commit((tx) => tx.task(id)))

/** The committed record's status. */
export const statusOf = (session: Session.Service, id: Ids.TaskId) =>
  Effect.map(getTask(session, id), (record) => record?.state.status)

/** Retry `check` on a short interval until it holds. */
export const eventually = <E, R>(check: Effect.Effect<boolean, E, R>) =>
  Effect.repeat(check, {
    until: (done) => done,
    schedule: Schedule.spaced('1 millis'),
  }).pipe(Effect.timeout('2 seconds'), Effect.orDie)

/** Let every fiber that is ready run. */
export const settle = Effect.sleep('5 millis')

/** A Storage whose commits can be held: each held commit waits for `release`. */
export interface Held {
  readonly storage: Storage.Service
  /** Hold the next commits; `entered` completes once one reached Storage. */
  readonly hold: Effect.Effect<{
    readonly entered: Deferred.Deferred<void>
    readonly release: Effect.Effect<void>
  }>
}

interface Gate {
  readonly entered: Deferred.Deferred<void>
  readonly release: Deferred.Deferred<void>
}

export const holdable = (backend: Storage.Service): Held => {
  let gate: Gate | undefined
  return {
    storage: {
      ...backend,
      commit: (writes) =>
        Effect.suspend(() => {
          const held = gate
          if (held === undefined) return backend.commit(writes)
          return Deferred.done(held.entered, Exit.void).pipe(
            Effect.andThen(Deferred.await(held.release)),
            Effect.andThen(backend.commit(writes)),
          )
        }),
    },
    hold: Effect.sync(() => {
      const opened: Gate = { entered: Deferred.makeUnsafe(), release: Deferred.makeUnsafe() }
      gate = opened
      return {
        entered: opened.entered,
        release: Effect.suspend(() => {
          gate = undefined
          return Deferred.done(opened.release, Exit.void)
        }),
      }
    }),
  }
}

/** What the code under test logged at error level: message and cause, as text. */
export type Reports = Array<string>

const capture = (reports: Reports) =>
  Logger.make<unknown, void>(({ message, cause, logLevel }) => {
    if (logLevel !== 'Error') return
    const parts: ReadonlyArray<unknown> = Array.isArray(message) ? message : [message]
    reports.push([...parts.map(String), Cause.pretty(cause)].join(' '))
  })

/**
 * Run a scoped test body over a fresh memory store, capturing error logs; failures it leaves
 * are defects.
 */
export const runTest = <A, E>(
  body: (storage: Storage.Service, reports: Reports) => Effect.Effect<A, E, Scope.Scope>,
): Promise<A> => {
  const reports: Reports = []
  const test = Effect.flatMap(Storage.makeMemory, (storage) => body(storage, reports))
  return Effect.runPromise(
    // oxlint-disable-next-line effecttsgo/strict-effect-provide -- each test is an entry point
    Effect.scoped(Effect.orDie(test)).pipe(Effect.provide(Logger.layer([capture(reports)]))),
  )
}
