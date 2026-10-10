import { NodeRuntime } from '@effect/platform-node'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Conversation from 'effect-harness/Conversation'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as ConversationInitializer from 'effect-harness/ConversationInitializer'
import * as Document from 'effect-harness/Document'
import * as Harness from 'effect-harness/Harness'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const State = Document.define({
  kind: 'example.initialized',
  version: 1,
  scope: 'conversation',
  history: 'rewindable',
  fork: 'asOf',
  schema: Schema.Struct({ initialized: Schema.Boolean }),
  initial: () => ({ initialized: true }),
})
const initializers = [
  ConversationInitializer.make({
    execute: (tx, record) =>
      Transaction.ensureDocument(tx, State, {
        scope: { _tag: 'conversation', conversationId: record.id },
      }).pipe(Effect.asVoid),
  }),
] as const
const model = Layer.effect(
  LanguageModel.LanguageModel,
  LanguageModel.make({
    generateText: () => Effect.die('This example only creates a conversation'),
    streamText: () => Stream.empty,
  }),
)

// Run after building: node apps/example/dist/tour/ConversationInitialization.js
// An initializer failure would roll back the conversation and its document together.
const program = Effect.gen(function* () {
  const harness = yield* Harness.Harness
  const root = yield* harness.root
  const saved = yield* Conversation.snapshot(root, State, {
    scope: { _tag: 'conversation', conversationId: root.id },
  })
  if (Option.isNone(saved) || !saved.value.value.initialized)
    return yield* Effect.die('Conversation was committed without required initialization')
  yield* Effect.log('Conversation and required state committed atomically', {
    conversationId: root.id,
  })
}).pipe(
  Effect.provide(
    Harness.layerLocal({ initializers }).pipe(
      Layer.provide(Layer.merge(model, Storage.layerMemory)),
    ),
  ),
)

NodeRuntime.runMain(program)
