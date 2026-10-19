/**
 * Tasks for the ownership tests, ported from pi's `harness-ownership.test.ts`: `Hold` runs until
 * its named gate opens or it is aborted, and the gates belong to one test.
 */
import { Deferred, Effect, Exit, Fiber, Scope } from 'effect'
import { Session } from '../../session/index.ts'
import { Storage } from '../../storage/index.ts'
import type { ConversationId, TaskId } from '../../storage/schema/Ids.ts'
import type { TaskRecord, TaskState } from '../../storage/schema/Task.ts'
import type { Scheduler } from '../index.ts'
import { Task } from '../index.ts'
import { completed, eventually, getTask, settle } from './support.ts'

/** Outcome a held task commits once its gate opens. */
export type Ending = 'completed' | 'failed'

export interface HoldInput {
  readonly [key: string]: string | boolean
  readonly name: string
  /** Its abort handler first waits for the gate `abort.<name>`. */
  readonly slowAbort: boolean
}

const aborted = { status: 'terminal', outcome: { status: 'aborted' } } as const

const failed = {
  status: 'terminal',
  outcome: { status: 'failed', error: { message: 'gate failed' } },
} as const

type Gate = (name: string) => Deferred.Deferred<Ending>

const makeHold = (gate: Gate, runs: Map<string, number>) =>
  Task.make<HoldInput, { readonly phase: 'hold' }, null>({
    name: 'test.hold',
    version: 1,
    initial: () => ({ phase: 'hold' }),
    phases: {
      hold: (task, runtime) =>
        Effect.gen(function* () {
          runs.set(task.input.name, (runs.get(task.input.name) ?? 0) + 1)
          const ending = yield* Deferred.await(gate(task.input.name))
          yield* runtime.commit(() =>
            Effect.succeed(ending === 'completed' ? completed(null) : failed),
          )
        }),
    },
    abort: (task, runtime) =>
      Effect.gen(function* () {
        if (task.input.slowAbort) yield* Deferred.await(gate(`abort.${task.input.name}`))
        yield* runtime.commit(() => Effect.succeed(aborted))
      }),
  })

/** Waits on the tasks in its input, then completes. */
const Waiter = Task.make<
  { readonly on: ReadonlyArray<TaskId> },
  { readonly phase: 'wait' } | { readonly phase: 'done' },
  null
>({
  name: 'test.waiter',
  version: 1,
  initial: () => ({ phase: 'wait' }),
  phases: {
    wait: (task, runtime) =>
      runtime.commit(() =>
        Effect.succeed({
          status: 'waiting' as const,
          checkpoint: { phase: 'done' as const },
          on: task.input.on,
          policy: 'allSettled' as const,
        }),
      ),
    done: (_task, runtime) => runtime.commit(() => Effect.succeed(completed(null))),
  },
  abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
})

/** Creates, in the commit that ends it, a conversation it owns with a held task in it. */
const makeSpawner = (Hold: ReturnType<typeof makeHold>) =>
  Task.make<{ readonly name: string }, { readonly phase: 'spawn' }, null>({
    name: 'test.spawner',
    version: 1,
    initial: () => ({ phase: 'spawn' }),
    phases: {
      spawn: (task, runtime) =>
        runtime.commit((tx) =>
          Effect.gen(function* () {
            const child = yield* tx.createConversation({
              ownership: { kind: 'task', taskId: task.id },
            })
            yield* tx.createTask(
              Hold,
              { name: task.input.name, slowAbort: false },
              { ownership: { kind: 'conversation' }, conversationId: child.id },
            )
            return completed(null)
          }),
        ),
    },
    abort: (_task, runtime) => runtime.commit(() => Effect.succeed(aborted)),
  })

/** The tasks of one test, over gates of their own. */
export const holds = () => {
  const gates = new Map<string, Deferred.Deferred<Ending>>()
  /** How often each named task started its run phase. */
  const runs = new Map<string, number>()
  const gate: Gate = (name) => {
    const found = gates.get(name) ?? Deferred.makeUnsafe<Ending>()
    gates.set(name, found)
    return found
  }
  const open = (name: string, ending: Ending = 'completed') =>
    Effect.asVoid(Deferred.succeed(gate(name), ending))
  const Hold = makeHold(gate, runs)
  const Spawner = makeSpawner(Hold)
  return { Hold, Waiter, Spawner, gate, open, runs, tasks: [Hold, Waiter, Spawner] }
}

export type Holds = ReturnType<typeof holds>

/** A held task's input. */
export const hold = (name: string, slowAbort = false): HoldInput => ({ name, slowAbort })

export interface OwnedChild {
  readonly owner: TaskId
  readonly child: ConversationId
  readonly inner: TaskId
}

/** In `parent`, commit task `owner` and a conversation it owns holding task `inner`. */
export const ownedChild = (
  session: Session.Service,
  tasks: Holds,
  parent: ConversationId,
  name: string,
  options: { background?: boolean; slowInner?: boolean; slowOwner?: boolean } = {},
) =>
  Effect.orDie(
    session.commit((tx) =>
      Effect.gen(function* () {
        const owner = yield* tx.createTask(tasks.Hold, hold(name, options.slowOwner), {
          ownership: { kind: 'conversation' },
          conversationId: parent,
          background: options.background ?? false,
        })
        const child = yield* tx.createConversation({ ownership: { kind: 'task', taskId: owner } })
        const inner = yield* tx.createTask(tasks.Hold, hold(`${name}.inner`, options.slowInner), {
          ownership: { kind: 'conversation' },
          conversationId: child.id,
        })
        const created: OwnedChild = { owner, child: child.id, inner }
        return created
      }),
    ),
  )

/** The committed state of a task that exists. */
export const stateOf = (session: Session.Service, id: TaskId) =>
  Effect.flatMap(getTask(session, id), (record) =>
    record === undefined ? Effect.die(`Task ${id} does not exist`) : Effect.succeed(record.state),
  )

/** The outcome status of a terminal state, or its status. */
export const outcomeOf = (state: TaskState) =>
  'outcome' in state ? state.outcome.status : state.status

/** Whether `effect` finishes within a few scheduler turns; it is interrupted otherwise. */
export const finishes = <A, E>(
  effect: Effect.Effect<A, E>,
): Effect.Effect<boolean, never, Scope.Scope> =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkScoped(effect)
    yield* settle
    yield* settle
    const done = fiber.pollUnsafe() !== undefined
    if (!done) yield* Fiber.interrupt(fiber)
    return done
  })

/** Commit a held task owned by `conversationId`. */
export const holdIn = (
  session: Session.Service,
  tasks: Holds,
  conversationId: ConversationId,
  name: string,
  slowAbort = false,
) =>
  Effect.orDie(
    session.commit((tx) =>
      tx.createTask(tasks.Hold, hold(name, slowAbort), {
        ownership: { kind: 'conversation' },
        conversationId,
      }),
    ),
  )

/** Wait until the committed task has `status`. */
export const statusIs = (session: Session.Service, id: TaskId, status: string) =>
  eventually(Effect.map(stateOf(session, id), (state) => state.status === status))

/** The outcome status the task ends with. */
export const outcome = (scheduler: Scheduler.Service, id: TaskId) =>
  Effect.map(scheduler.waitForTask(id), (receipt) => receipt.state.outcome.status)

/** A Session over `storage` with no scheduler, so nothing derives marks; `close` ends it. */
export const bareSession = (storage: Storage.Service) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make()
    const session = yield* Session.make.pipe(
      Effect.provideService(Storage.Storage, storage),
      Scope.provide(scope),
    )
    return { session, close: Scope.close(scope, Exit.void) }
  })

/** A Storage that calls `see` with each task record a commit writes. */
export const watchTaskWrites = (
  backend: Storage.Service,
  see: (record: TaskRecord) => void,
): Storage.Service => ({
  ...backend,
  commit: (writes) =>
    Effect.suspend(() => {
      for (const write of writes) if (write.type === 'task') see(write.value)
      return backend.commit(writes)
    }),
})
