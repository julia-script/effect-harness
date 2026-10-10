/** Run after building: node apps/example/dist/StructuredResult.js */
import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Tool from 'effect-harness/Tool'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Toolkit from 'effect-harness/Toolkit'
import * as DemoModel from './DemoModel.js'

const structured = Schema.Struct({ uppercase: Schema.String, length: Schema.Natural })
const tools = Toolkit.make(
  Tool.makeResult('uppercase', {
    parameters: Schema.Struct({ text: Schema.String }),
    success: structured,
    replay: 'safe',
  }),
)
const handlers = tools.toLayer({
  uppercase: ({ text }) =>
    Effect.succeed({
      content: [Prompt.textPart({ text: `Converted ${text.length} characters.` })],
      structuredOutput: { uppercase: text.toUpperCase(), length: text.length },
      details: { original: text, panel: 'conversion' },
    }),
})
const dependencies = Layer.mergeAll(Storage.layerMemory, DemoModel.layer, handlers)

export const program = Effect.scoped(
  Effect.gen(function* () {
    const runtime = yield* HarnessRuntime.make({ tools })
    const root = yield* runtime.backend.root
    const job = yield* runtime.backend.submit({
      conversationId: root,
      draft: { type: 'input', content: 'hello' },
    })
    yield* runtime.backend.wait(job.id)
    const entries = yield* Session.scanEntries(runtime.session, { conversationId: root }).pipe(
      Stream.runCollect,
    )
    const saved = entries.find((entry) => entry.kind === 'tool.result')
    const receipt = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(ToolResult.ResultSchema))(
      saved?.data,
    )
    const value = yield* Schema.decodeUnknownEffect(Schema.toCodecJson(structured))(
      receipt.structuredOutput,
    )
    yield* Effect.log({ programmatic: value, modelVisible: receipt.content, ui: receipt.details })
  }),
).pipe(Effect.provide(dependencies))

NodeRuntime.runMain(program)
