import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const Progress = Document.define({
  kind: 'example.task-progress',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ steps: Schema.Natural }),
  initial: () => ({ steps: 0 }),
})

// Run after building: node apps/example/dist/tour/TaskDocumentRetirement.js
const program = Effect.gen(function* () {
  const session = yield* Session.make()
  const task = yield* Session.commit(session, (tx) =>
    Effect.gen(function* () {
      yield* Transaction.ensureRoot(tx)
      const task = yield* Transaction.createTask(tx, {
        conversationId: Record.ROOT_CONVERSATION_ID,
        kind: 'example.work',
        version: 1,
        input: null,
        background: false,
        abortRequested: false,
        state: { status: 'running', checkpoint: { phase: 'work' } },
      })
      yield* Transaction.ensureDocument(tx, Progress, { scope: { _tag: 'task', taskId: task.id } })
      return task
    }),
  )
  const target = { scope: { _tag: 'task', taskId: task.id } } satisfies Document.Target
  yield* Session.commit(session, (tx) =>
    Transaction.putTask(tx, {
      ...task,
      state: { status: 'terminal', outcome: { status: 'completed', result: 'done' } },
    }),
  )
  // Settlement and retirement persisted together. Old targets cannot recreate the document.
  const absent = Option.isNone(yield* Session.snapshot(session, Progress, target))
  const failure = yield* Session.commit(session, (tx) =>
    Transaction.ensureDocument(tx, Progress, target),
  ).pipe(Effect.flip)
  if (!absent || failure._tag !== 'SessionError' || failure.reason !== 'conflict')
    return yield* Effect.die('Task document retirement contract failed')
  yield* Effect.log('Task document retired; reacquisition rejected', {
    absent,
    reason: failure.reason,
  })
}).pipe(Effect.scoped, Effect.provide(Storage.layerMemory))

NodeRuntime.runMain(program)
