import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type { HarnessError } from 'effect-harness/HarnessError'
import * as Model from 'effect-harness/Model'
import type * as Prepared from 'effect-harness/PreparedRequest'
import type { Storage } from 'effect-harness/Storage'
import type { StorageFormat } from 'effect-harness/StorageFormat'

class Configuration extends Context.Service<Configuration, string>()(
  'DurableDeferred/types/Configuration',
) {}
declare const native: typeof LanguageModel.LanguageModel.Service
declare const adapter: Model.Deferred
declare const snapshot: Prepared.Snapshot
const model = Model.make({
  definition: {
    ref: { provider: 'custom', modelId: 'deferred' },
    capabilities: { tools: true, reasoning: false, images: false, structuredOutput: false },
  },
  options: Schema.JsonObject,
  languageModel: native,
  deferred: adapter,
  configure: () => Configuration.pipe(Effect.as(Context.empty())),
})

test('saved execution is explicit while legacy missing selection remains representable', () => {
  expect(snapshot.execution).type.toBe<'streaming' | 'deferred' | undefined>()
})

test('deferred activation keeps captured descriptor services out of runtime requirements and preserves configure services', () => {
  expect<Model.Requirements<typeof model>>().type.toBe<Configuration>()
  expect(HarnessRuntime.make({ models: [model] })).type.toBe<
    Effect.Effect<
      HarnessRuntime.HarnessRuntimeService,
      HarnessError,
      Scope.Scope | Storage | StorageFormat | Configuration
    >
  >()
  expect(adapter.cancel('encoded-handle')).type.toBe<Effect.Effect<void, Model.DeferredError>>()
})
