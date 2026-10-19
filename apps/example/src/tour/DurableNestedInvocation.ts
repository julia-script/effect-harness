import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Registry from 'effect-harness/Registry'
import * as Storage from 'effect-harness/Storage'
import * as Task from 'effect-harness/Task'
import { TaskExecution } from 'effect-harness/TaskExecution'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as ToolInvocation from 'effect-harness/ToolInvocation'
import * as Toolkit from 'effect-harness/Toolkit'
import { ExecutionError } from 'effect-harness/ExecutionError'
import { SessionError } from 'effect-harness/SessionError'
import { StorageError } from 'effect-harness/StorageError'

// After building, run with node or bun apps/example/dist/tour/DurableNestedInvocation.js.
// Stable keys identify calls within a parent. Omitted keys require stable replay order.
const leaf = Tool.makeResult('leaf', {
  parameters: Schema.Struct({ text: Schema.String }),
  success: Schema.String,
  resultValidation: 'strict',
  callers: ['tools'],
  replay: 'safe',
})
const outer = Tool.makeResult('outer', {
  success: Schema.String,
  resultValidation: 'strict',
  callers: ['model'],
})
const toolkit = Toolkit.make(leaf, outer)
const contract = {
  kind: 'example.nested.program',
  version: 1,
  input: Schema.Void,
  checkpoint: Schema.Union([
    Schema.Struct({ phase: Schema.tag('admit') }),
    Schema.Struct({ phase: Schema.tag('consume'), handle: ToolInvocation.HandleSchema }),
  ]),
  success: Schema.String,
  failure: Schema.String,
} as const
const program = Task.make({
  ...contract,
  initial: () => ({ phase: 'admit' as const }),
  phases: {
    admit: () =>
      Effect.gen(function* () {
        const access = yield* TaskExecution
        yield* access.commit((tx) =>
          Effect.gen(function* () {
            const admission = yield* access.invocation.admit(tx, {
              name: leaf.name,
              arguments: { text: 'from a task' },
              options: { key: 'leaf', progress: false },
            })
            if (admission._tag === 'denied') return yield* Effect.fail('Leaf denied')
            const handle = yield* access.memoize(
              tx,
              'leaf',
              ToolInvocation.HandleSchema,
              admission.handle,
            )
            yield* access.progress(tx, contract, {
              status: 'waiting',
              on: [handle.taskId],
              policy: 'allSettled',
              checkpoint: { phase: 'consume', handle },
            })
          }),
        )
      }),
    consume: ({ checkpoint }) =>
      Effect.gen(function* () {
        const access = yield* TaskExecution
        const receipt = yield* ToolInvocation.decode(access, leaf, checkpoint.handle)
        if (receipt.status !== 'succeeded' || receipt.valueKind !== 'decoded')
          return yield* Effect.fail('Leaf failed')
        yield* access.commit((tx) =>
          access.progress(tx, contract, {
            status: 'completing',
            outcome: { status: 'completed', result: receipt.value },
          }),
        )
      }),
  },
})
const run = Effect.scoped(
  Effect.gen(function* () {
    let leafRuns = 0
    const model = yield* LanguageModel.make({
      generateText: () => Effect.die('The example uses streamText'),
      streamText: () =>
        Stream.fromIterable([
          { type: 'tool-call', id: 'outer', name: outer.name, params: {}, providerExecuted: false },
          {
            type: 'finish',
            reason: 'tool-calls',
            usage: {
              inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
              outputTokens: { total: 1, text: 1, reasoning: undefined },
            },
            response: undefined,
          },
        ]),
    })
    const runtime = yield* HarnessRuntime.make({ tools: toolkit }).pipe(
      Effect.provideService(LanguageModel.LanguageModel, model),
      Effect.provide(
        toolkit.toLayer({
          leaf: ({ text }) =>
            Effect.gen(function* () {
              leafRuns++
              const access = yield* ToolExecution
              yield* access.details({ locallyAccepted: true })
              return {
                content: [Prompt.textPart({ text: 'private leaf content' })],
                structuredOutput: text.toUpperCase(),
                spend: { amount: 1, currency: 'USD', source: 'example' },
                control: { terminate: true },
              }
            }),
          outer: () =>
            Effect.gen(function* () {
              const access = yield* ToolExecution
              const receipt = yield* ToolInvocation.invoke(
                access,
                leaf,
                { text: 'from a tool' },
                { key: 'leaf', progress: false },
              ).pipe(
                Effect.mapError((cause) =>
                  Schema.is(SessionError)(cause) ||
                  Schema.is(StorageError)(cause) ||
                  Schema.isSchemaError(cause) ||
                  Schema.is(ExecutionError)(cause)
                    ? cause
                    : new ExecutionError({
                        reason: 'invalid',
                        operation: 'example.nested',
                        message: cause.message,
                        cause,
                      }),
                ),
              )
              if (receipt.status !== 'succeeded' || receipt.valueKind !== 'decoded')
                return yield* Effect.die('Leaf failed')
              return {
                content: [Prompt.textPart({ text: receipt.value })],
                structuredOutput: receipt.value,
                control: { terminate: true },
              }
            }),
        }),
      ),
    )
    yield* runtime.registry.install(
      Registry.withTask(
        Registry.toolkit({ id: 'example-program', version: 1 }, Toolkit.make()),
        program,
      ),
    )
    const root = yield* runtime.backend.root
    const task = yield* runtime.scheduler.start(program, undefined, { conversationId: root })
    const taskResult = yield* runtime.scheduler.wait(task)
    const submission = yield* runtime.backend.submit({
      conversationId: root,
      draft: { type: 'input', content: 'Call outer' },
    })
    const toolResult = yield* runtime.backend.wait(submission.id)
    yield* Effect.log({ taskResult, toolStatus: toolResult.status, leafRuns })
  }),
).pipe(Effect.provide(Storage.layerMemory))

if ('Bun' in globalThis) (await import('@effect/platform-bun/BunRuntime')).runMain(run)
else (await import('@effect/platform-node/NodeRuntime')).runMain(run)
