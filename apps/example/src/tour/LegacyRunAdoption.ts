import * as ScriptedStream from '../ScriptedStream.js'
/** One runtime runs registered tasks and ordinary durable conversation submissions. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Layer from 'effect/Layer'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Registry from 'effect-harness/Registry'
import * as Storage from 'effect-harness/Storage'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Toolkit from 'effect-harness/Toolkit'

const contract = {
  kind: 'example.custom',
  version: 1,
  input: Schema.Natural,
  checkpoint: Schema.Struct({ phase: Schema.Literal('run') }),
  success: Schema.Natural,
  failure: Schema.String,
}
const custom = Task.make({
  ...contract,
  initial: () => ({ phase: 'run' as const }),
  phases: {
    run: ({ input }) =>
      Effect.gen(function* () {
        const execution = yield* TaskExecution
        yield* execution.commit((tx) =>
          execution.progress(tx, contract, {
            status: 'completing',
            outcome: { status: 'completed', result: input * 2 },
          }),
        )
      }),
  },
})
const model = Layer.effect(
  LanguageModel.LanguageModel,
  ScriptedStream.make({
    generateText: () =>
      Effect.succeed([
        { type: 'text', text: 'Hello from the supervised conversation.' },
        {
          type: 'finish',
          reason: 'stop',
          usage: {
            inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
            outputTokens: { total: 1, text: 1, reasoning: undefined },
          },
          response: undefined,
        },
      ]),
  }),
)
const program = Effect.scoped(
  Effect.gen(function* () {
    const runtime = yield* HarnessRuntime.make()
    const conversationId = yield* runtime.backend.root
    yield* runtime.registry.install(
      Registry.withTask(Registry.toolkit({ id: 'example', version: 1 }, Toolkit.make()), custom),
    )
    const task = yield* runtime.scheduler.start(custom, 21, { conversationId })
    const input = yield* runtime.backend.submit({
      conversationId,
      draft: { type: 'input', content: 'hello' },
    })
    const result = yield* runtime.scheduler.wait(task)
    const answer = yield* runtime.backend.wait(input.id)
    yield* Effect.log({ custom: result, conversation: answer.status })
  }),
).pipe(Effect.provide([Storage.layerMemory, model]))
Effect.runPromise(program).catch(console.error)
