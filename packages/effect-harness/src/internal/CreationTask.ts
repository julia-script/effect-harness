/** Conversation creation hooks run as recoverable work outside the persistence mutation line. */
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import type * as Agent from '../Agent.ts'
import type * as Executor from '../Executor.ts'
import * as Hook from '../Hook.ts'
import * as Invocation from '../Invocation.ts'
import * as Registry from '../Registry.ts'
import type { StorageError } from '../StorageError.ts'
import * as Task from '../Task.ts'
import { TaskRuntime } from '../TaskRuntime.ts'
import * as ConversationState from './ConversationState.ts'
import type * as Session from './Session.ts'

export const name = 'harness.created'
const Checkpoint = Schema.Struct({ phase: Schema.tag('created') })

/** Interrupted callbacks may run again; completed callbacks are remembered after their effects finish. */
export const make = Effect.fn('CreationTask.make')(function* (
  session: Session.Service,
  executor: Executor.Executor['Service'],
  options: {
    readonly cwd: string
    readonly settings: Agent.Settings
    readonly report: (error: unknown) => Effect.Effect<void>
  },
): Effect.fn.Return<Task.BoundDefinition> {
  const definition = Task.define({
    name,
    version: 1,
    input: Schema.Json,
    checkpoint: Checkpoint,
    result: Schema.Json,
    initial: () => ({ phase: 'created' as const }),
    run: Effect.fn('CreationTask.run')(function* (
      task: Task.Snapshot<Schema.Json, typeof Checkpoint.Type>,
    ): Effect.fn.Return<Task.Transition<never, null>, StorageError, TaskRuntime> {
      const runtime = yield* TaskRuntime
      if ((yield* runtime.memo('created')) === true) return Task.complete(null)
      const snapshot = yield* session.snapshot(ConversationState.AgentDoc, {
        owner: task.conversationId,
      })
      const state: Agent.State = Option.isSome(snapshot) ? snapshot.value.value : {}
      const invoke = Effect.gen(function* () {
        const agent = yield* executor.resolve(state, options.settings)
        yield* Hook.conversationCreated(
          Registry.handlers(agent, 'conversation'),
          task.conversationId,
        )
      }).pipe(
        Effect.provideService(Invocation.Invocation, {
          cwd: state.cwd ?? options.cwd,
          report: options.report,
          progress: () => Effect.void,
        }),
      )
      yield* invoke
      // This acknowledgement closes the hook-success-to-task-completion gap after it commits.
      // A crash before acknowledgement may replay effects; application hooks must tolerate that.
      yield* runtime.memo('created', true)
      return Task.complete(null)
    }),
  })
  return yield* Task.bind(definition)
})
