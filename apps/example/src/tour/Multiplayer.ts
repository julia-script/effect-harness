/** Two local clients share committed observations and steer one active conversation. */
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Ref from 'effect/Ref'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Tool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Conversation from 'effect-harness/Conversation'
import * as Invocation from 'effect-harness/Invocation'
import * as Observation from 'effect-harness/Observation'
import * as Submission from 'effect-harness/Submission'
import * as ToolRegistration from 'effect-harness/ToolRegistration'
import * as ToolError from 'effect-harness/ToolError'
import * as Runtime from './Runtime.ts'

export const Result = Schema.Struct({
  clients: Schema.Int,
  firstChanges: Schema.Int,
  secondChanges: Schema.Int,
  lateClientSawActiveWork: Schema.Boolean,
  settledInputs: Schema.Int,
  wireSchema: Schema.Literal('Observation.Change'),
})

export const run = Effect.scoped(
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const lookup = Tool.make('logs', {
      description: 'Look up simulated logs.',
      parameters: Schema.Struct({}),
      success: Schema.String,
      failure: ToolError.ToolError,
    }).addDependency(Invocation.ToolCall)
    const toolkit = Toolkit.make(lookup)
    const tools = yield* ToolRegistration.bind(toolkit, { logs: { replay: 'safe' } }).pipe(
      Effect.provide(
        toolkit.toLayer({
          logs: Effect.fn('tour.logs')(function* () {
            yield* (yield* Invocation.ToolCall).output('Reading deployment logs...')
            yield* Deferred.succeed(entered, undefined)
            yield* Deferred.await(release)
            return 'The staging health check failed.'
          }),
        }),
      ),
    )
    const provider: Runtime.Provider = {
      generateText: () =>
        Effect.succeed([{ type: 'text', text: 'summary' }, Runtime.finish('stop')]),
      streamText: ({ prompt }) =>
        prompt.content.some((message) => message.role === 'tool')
          ? Runtime.answer(`Investigated: ${Runtime.lastUserText(prompt)}`)
          : Stream.fromIterable([
              {
                type: 'tool-call' as const,
                id: 'logs-1',
                name: 'logs',
                params: {},
                providerExecuted: false,
              },
              Runtime.finish('tool-calls'),
            ]),
    }
    const { harness } = yield* Runtime.open({ provider, extensions: [{ name: 'ops', tools }] })
    const root = yield* harness.root
    const client = Effect.fn('tour.attach')(function* () {
      const changes = yield* Ref.make<ReadonlyArray<Observation.Change>>([])
      const joined = yield* Deferred.make<void>()
      const settled = yield* Deferred.make<void>()
      const fiber = yield* Conversation.watch(root).pipe(
        Stream.runForEach((change) =>
          Effect.gen(function* () {
            // This is the same schema an application can encode for a socket or SSE.
            const wire = yield* Schema.encodeEffect(Schema.fromJsonString(Observation.Change))(
              change,
            )
            const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(Observation.Change))(
              wire,
            )
            yield* Ref.update(changes, (values) => [...values, decoded])
            if (change._tag === 'snapshot') yield* Deferred.succeed(joined, undefined)
            if (
              change._tag === 'commit' &&
              change.frame.writes.some(
                (write) => write._tag === 'submission' && write.value.status === 'done',
              )
            )
              yield* Deferred.succeed(settled, undefined)
          }),
        ),
        Effect.forkChild,
      )
      yield* Deferred.await(joined)
      return { changes, settled, fiber }
    })
    const first = yield* client()
    const request = yield* Conversation.submit(root, 'Why did deployment fail?')
    yield* Deferred.await(entered)
    const second = yield* client()
    const steering = yield* Conversation.submit(root, 'Check staging before production.', {
      mode: 'steering',
    })
    yield* Deferred.succeed(release, undefined)
    const results = yield* Effect.all([Submission.wait(request), Submission.wait(steering)])
    yield* Deferred.await(first.settled)
    yield* Deferred.await(second.settled)
    const firstChanges = yield* Ref.get(first.changes)
    const secondChanges = yield* Ref.get(second.changes)
    yield* Fiber.interrupt(first.fiber)
    yield* Fiber.interrupt(second.fiber)
    const late = secondChanges[0]
    yield* Runtime.check(
      late?._tag === 'snapshot' &&
        late.value.tasks.some((task) => task.state.status !== 'terminal'),
      'A late client missed the active task',
    )
    yield* Runtime.check(
      results.every((result) => result.status === 'done'),
      'Steering did not settle with the active run',
    )
    return yield* Schema.decodeEffect(Result)({
      clients: 2,
      firstChanges: firstChanges.length,
      secondChanges: secondChanges.length,
      lateClientSawActiveWork: true,
      settledInputs: results.length,
      wireSchema: 'Observation.Change',
    })
  }),
)
