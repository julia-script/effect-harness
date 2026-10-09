import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Conversation from 'effect-harness/Conversation'
import * as Extension from 'effect-harness/Extension'
import * as Harness from 'effect-harness/Harness'
import * as Hook from 'effect-harness/Hook'
import * as PromptSection from 'effect-harness/PromptSection'
import * as Storage from 'effect-harness/Storage'
import * as Submission from 'effect-harness/Submission'
import * as DemoModel from '../DemoModel.js'
import * as Uppercase from '../Uppercase.js'

const extension = Extension.make({
  name: 'text-tools',
  tools: Uppercase.tools,
  sections: [
    PromptSection.make({
      key: 'format',
      render: () => Effect.succeed('Keep the final answer brief.'),
    }),
  ],
  hooks: [
    Hook.make({
      event: 'beforeTool',
      execute: ({ call }) =>
        Effect.succeed(
          call.name === 'uppercase'
            ? { _tag: 'allow' }
            : { _tag: 'block', message: 'Unknown tool' },
        ),
    }),
  ],
}).pipe(Extension.provide(Uppercase.layer))

export const layer = Harness.layerLocal({ extensions: [extension] }).pipe(
  Layer.provide(DemoModel.layer),
  Layer.provide(Storage.layerMemory),
)
export const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  return yield* root.pipe(
    Conversation.submit({ type: 'input', content: 'Extensions compose' }),
    Effect.flatMap(Submission.wait),
  )
})
