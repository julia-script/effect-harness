/** An absolute saved deadline survives close; background reminders have an explicit cancellation boundary. */
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Task from 'effect-harness/Task'
import { TaskRuntime } from 'effect-harness/TaskRuntime'
import * as Memory from 'effect-harness/storage/Memory'
import * as Runtime from './Runtime.ts'

const Input = Schema.Struct({ text: Schema.String, delay: Schema.Int })
const Checkpoint = Schema.Union([
  Schema.Struct({ phase: Schema.tag('schedule') }),
  Schema.Struct({ phase: Schema.tag('sleep'), until: Schema.Finite }),
])
export const Result = Schema.Struct({
  deadline: Schema.NullOr(Schema.Finite),
  messages: Schema.Array(Schema.String),
  recovered: Task.Outcome,
  background: Task.Outcome,
  boundary: Schema.String,
})

export const run = Effect.scoped(
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>()
    const scheduled = yield* Ref.make<ReadonlyArray<number>>([])
    const fired = yield* Ref.make<ReadonlyArray<string>>([])
    const reminder = yield* Task.bind(
      Task.define({
        name: 'tour/reminder',
        version: 1,
        input: Input,
        checkpoint: Checkpoint,
        result: Schema.String,
        initial: () => ({ phase: 'schedule' as const }),
        run: Effect.fn('tour.reminder')(function* (task) {
          const runtime = yield* TaskRuntime
          if (task.checkpoint.phase === 'schedule') {
            const until = (yield* runtime.now) + task.input.delay
            yield* runtime.checkpoint({ phase: 'sleep', until })
            yield* Ref.update(scheduled, (targets) => [...targets, until])
            yield* Deferred.succeed(entered, undefined)
            yield* runtime.sleepUntil(until)
          } else {
            yield* runtime.sleepUntil(task.checkpoint.until)
          }
          yield* Ref.update(fired, (messages) => [...messages, task.input.text])
          return Task.complete(task.input.text)
        }),
      }),
    )
    const store = yield* Memory.make
    const first = yield* Runtime.open({ store, tasks: [reminder] })
    const conversation = yield* first.harness.root
    const saved = yield* first.harness.spawn(
      reminder,
      { text: 'Saved deadline reached', delay: 40 },
      {
        conversationId: conversation.id,
      },
    )
    yield* Deferred.await(entered)
    yield* first.harness.close
    const second = yield* Runtime.open({ store, tasks: [reminder] })
    yield* second.harness.resume
    const completed = yield* second.harness.awaitTask(saved)
    const recovered = yield* Schema.decodeUnknownEffect(Task.Outcome)(completed.state.outcome)
    yield* Runtime.check(recovered.status === 'completed', 'Saved reminder must fire')
    yield* Runtime.check(
      (yield* Ref.get(scheduled)).length === 1,
      'Reopen must retain the absolute deadline',
    )
    const background = yield* second.harness.spawn(
      reminder,
      { text: 'Background reminder', delay: 60_000 },
      {
        conversationId: conversation.id,
        background: true,
      },
    )
    yield* second.harness.awaitIdle(conversation.id)
    yield* second.harness.abort(conversation.id)
    const snapshot = yield* second.harness.snapshot(conversation.id)
    yield* Runtime.check(
      snapshot.tasks.some((task) => task.id === background && task.state.status !== 'terminal'),
      'Ordinary idle and abort must leave background work live',
    )
    yield* second.harness.abort(conversation.id, true)
    const cancelled = yield* second.harness.awaitTask(background)
    const cancelledOutcome = yield* Schema.decodeUnknownEffect(Task.Outcome)(
      cancelled.state.outcome,
    )
    yield* Runtime.check(
      cancelledOutcome.status === 'aborted',
      'Explicit background abort must settle',
    )
    return yield* Schema.decodeEffect(Result)({
      deadline: (yield* Ref.get(scheduled))[0] ?? null,
      messages: yield* Ref.get(fired),
      recovered,
      background: cancelledOutcome,
      boundary:
        'Ordinary idle and abort exclude background tasks; explicit inclusion cancels them.',
    })
  }),
)
