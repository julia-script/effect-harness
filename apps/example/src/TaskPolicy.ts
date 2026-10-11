/** Run after integration build: bun apps/example/src/TaskPolicy.ts. No scheduler is activated. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as TaskPolicy from 'effect-harness/TaskPolicy'
import * as Transaction from 'effect-harness/Transaction'

export const program = Effect.scoped(
  Effect.gen(function* () {
    const session = yield* Session.make()
    const task = yield* Session.commit(session, (tx) =>
      Effect.gen(function* () {
        yield* Transaction.ensureRoot(tx)
        const task = yield* Transaction.createTask(tx, {
          conversationId: Record.ROOT_CONVERSATION_ID,
          kind: 'example.policy',
          version: 1,
          input: null,
          background: false,
          abortRequested: false,
          restartPolicy: 'abandon',
          schedulerFormat: { version: 1, minimumReader: 1 },
          state: { status: 'running', checkpoint: { phase: 'work' } },
        })
        // Prepare the mark explicitly. A future scheduler must wait for owned work and run cleanup.
        yield* Transaction.putTask(tx, TaskPolicy.recover(task))
        yield* Transaction.requestTaskAbort(tx, task.id, 'user requested cancellation')
        return task
      }),
    )
    const current = Option.getOrThrow(yield* Session.task(session, task.id))
    yield* Effect.log(
      'Persisted policy and abort intent; dispatch remains a scheduler integration.',
      {
        restart: TaskPolicy.restartPolicy(current),
        abort: TaskPolicy.abortIntent(current),
      },
    )
  }),
).pipe(Effect.provide(Storage.layerMemory))

if (import.meta.main) await Effect.runPromise(program)
