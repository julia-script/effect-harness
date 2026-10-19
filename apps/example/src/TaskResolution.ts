/** Run after integration build: bun apps/example/src/TaskResolution.ts. Inspection runs no handlers. */
import * as Effect from 'effect/Effect'
import * as Record from 'effect-harness/Record'
import * as Task from 'effect-harness/Task'
import * as TaskResolution from 'effect-harness/TaskResolution'
import { Counter } from './TaskDefinition.js'

export const program = Effect.gen(function* () {
  const wire = yield* Task.initialize(Counter, { count: 4 })
  const record: Record.Task = {
    id: Record.TaskId.make(2),
    conversationId: Record.ROOT_CONVERSATION_ID,
    kind: Counter.kind,
    version: Counter.version,
    input: wire.input,
    background: false,
    abortRequested: false,
    state: { status: 'pending', checkpoint: wire.checkpoint },
  }
  const inspected = yield* TaskResolution.inspect(Counter, record)
  const missing = yield* TaskResolution.inspect<typeof Counter>(undefined, record)
  yield* Effect.log(
    'Definition compatibility is distinct from scheduling readiness.',
    inspected._tag,
    missing._tag,
  )
})

if (import.meta.main) await Effect.runPromise(program)
