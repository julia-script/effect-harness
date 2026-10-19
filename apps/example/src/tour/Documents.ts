import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Conversation from 'effect-harness/Conversation'
import * as Document from 'effect-harness/Document'
import * as Harness from 'effect-harness/Harness'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as Tool from 'effect-harness/Tool'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as Toolkit from 'effect-harness/Toolkit'
import * as Transaction from 'effect-harness/Transaction'
import * as DemoModel from '../DemoModel.js'

export const Calls = Document.define({
  kind: 'example.calls',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Natural }),
  initial: () => ({ count: 0 }),
})
export const target = { scope: { _tag: 'session' } } as const
const tools = Toolkit.make(
  Tool.make('uppercase', {
    parameters: Schema.Struct({ text: Schema.String }),
    success: Schema.String,
    // The document update is a side effect: replay would increment it twice.
    replay: 'unsafe',
  }),
)
const handlers = tools.toLayer({
  uppercase: Effect.fn('uppercase.counted')(function* ({ text }) {
    const execution = yield* ToolExecution
    yield* execution.commit((tx) =>
      Effect.gen(function* () {
        yield* Transaction.ensureDocument(tx, Calls, target)
        yield* Transaction.updateDocument(tx, Calls, target, ({ count }) => ({ count: count + 1 }))
        yield* Transaction.appendEntry(tx, execution.conversationId, {
          kind: 'example.counted',
          data: { text },
        })
      }),
    )
    return text.toUpperCase()
  }),
})
export const layer = Harness.layerLocal({ tools }).pipe(
  Layer.provide(handlers),
  Layer.provide(DemoModel.layer),
  Layer.provide(Storage.layerMemory),
)
export const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const settled = yield* root.pipe(
    Conversation.submit({ type: 'input', content: 'Count this call' }),
    Effect.flatMap(Submission.wait),
  )
  const snapshot = yield* root.pipe(Conversation.snapshot(Calls, target))
  return { settled, snapshot }
})
