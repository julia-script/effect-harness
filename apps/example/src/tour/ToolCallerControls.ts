import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as Registry from 'effect-harness/Registry'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolInventory from 'effect-harness/ToolInventory'
import * as ToolControl from 'effect-harness/ToolControl'
import * as Record from 'effect-harness/Record'
import { ToolExecution } from 'effect-harness/ToolExecution'

// A host supplies invocation access. This example performs no document operations.
const invocation = {
  conversationId: Schema.decodeSync(Record.ConversationId)(1),
  taskId: Schema.decodeSync(Record.TaskId)(1),
  callId: 'example',
  snapshot: () => Effect.succeedNone,
  watch: () => Stream.empty,
  commit: () => Effect.die('The example does not perform commits'),
  output: () => Effect.void,
  details: () => Effect.void,
  diagnostic: () => Effect.void,
}
const run = Effect.scoped(
  Effect.gen(function* () {
    const lookup = Tool.makeResult('lookup', {
      callers: ['tools'],
      success: Schema.Struct({ count: Schema.Int }),
      resultValidation: 'strict',
    })
    const finish = Tool.makeResult('finish', {
      callers: ['model'],
      success: Schema.String,
      resultValidation: 'strict',
    })
    const toolkit = Toolkit.make(lookup, finish)
    const registry = yield* Registry.make
    yield* registry.install(
      Registry.provide(
        Registry.toolkit({ id: 'application', version: 1 }, toolkit),
        toolkit.toLayer({
          lookup: () =>
            Effect.succeed({
              content: [Prompt.textPart({ text: 'one item' })],
              structuredOutput: { count: 1 },
              details: { panel: 'lookup' },
            }),
          finish: () =>
            Effect.succeed({
              content: [Prompt.textPart({ text: 'complete' })],
              structuredOutput: 'done',
              control: { terminate: true },
            }),
        }),
      ),
    )
    const lease = yield* registry.acquire
    const options = {
      agent: {
        modelTools: ['finish'],
        toolkits: { inherit: false, registrations: ['application'] },
      },
    } as const
    const hidden = yield* ToolInventory.invoke(
      lease,
      { id: 'denied', name: 'lookup', arguments: {} },
      'model',
      options,
    ).pipe(Effect.flip)
    const nested = yield* ToolInventory.invokeNested(
      lease,
      { id: 'nested', name: 'lookup', arguments: {} },
      options,
    )
    const final = yield* ToolInventory.invoke(
      lease,
      { id: 'finish', name: 'finish', arguments: {} },
      'model',
      options,
    )
    const barrier = yield* ToolControl.reduce(
      ['finish'],
      [
        {
          callId: 'finish',
          caller: 'model',
          status: 'succeeded',
          ...(final.control === undefined ? {} : { control: final.control }),
        },
      ],
      ['finish'],
    )
    yield* Effect.log({ denial: hidden._tag, programmatic: nested.structuredOutput, barrier })
    // The host's later versioned run-loop consumer applies the barrier and persists settlement.
  }),
).pipe(Effect.provideService(ToolExecution, invocation))

Effect.runPromise(run).catch(console.error)
