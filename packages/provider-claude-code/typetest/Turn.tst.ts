import { expect, test } from 'tstyche'
import * as Catalog from '@effect-harness/provider-claude-code/Catalog'
import * as Cli from '@effect-harness/provider-claude-code/Cli'
import * as IntentServer from '@effect-harness/provider-claude-code/IntentServer'
// effect-review-allow P9-namespace-alias-equals-module: @effect-harness/provider-claude-code/LanguageModel and effect/ai/LanguageModel both bind LanguageModel; Model distinguishes the concepts.
import * as Model from '@effect-harness/provider-claude-code/LanguageModel'
import * as Protocol from '@effect-harness/provider-claude-code/Protocol'
import * as Turn from '@effect-harness/provider-claude-code/Turn'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import type * as AiError from 'effect/ai/AiError'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Response from 'effect/ai/Response'
import type { ModelError } from '@effect-harness/harness/ModelError'

class Caller extends Context.Service<Caller, { readonly event: Protocol.Event }>()(
  'typetest/Turn/Caller',
) {}
const failure = { _tag: 'CallerFailure' as const }
const source = Stream.fromEffect(Effect.flatMap(Caller, () => Effect.fail(failure)))

test('translation and collection preserve generic foreign errors and requirements', () => {
  const translated = Turn.translate(source, new Map<string, string>())
  expect(translated).type.toBe<
    Stream.Stream<Response.StreamPartEncoded, typeof failure | AiError.AiError, Caller>
  >()
  expect(Turn.collect(source)).type.toBe<
    Effect.Effect<Array<Response.PartEncoded>, typeof failure, Caller>
  >()
  expect(Turn.collect(translated)).type.toBe<
    Effect.Effect<Array<Response.PartEncoded>, typeof failure | AiError.AiError, Caller>
  >()
  expect(Turn.translate).type.not.toBeCallableWith(source, new Map<number, number>())
})

test('model and catalogue expose the captured CLI and exact construction channels', () => {
  expect(Model.make({ model: 'declared' })).type.toBe<
    Effect.Effect<
      typeof LanguageModel.LanguageModel.Service,
      AiError.AiError,
      Cli.Cli | IntentServer.IntentServer
    >
  >()
  expect(Model.layer({ model: 'declared' })).type.toBe<
    Layer.Layer<
      LanguageModel.LanguageModel | Cli.Cli,
      AiError.AiError,
      Cli.Cli | IntentServer.IntentServer
    >
  >()
  expect(
    Catalog.descriptor({ modelId: 'declared', contextWindow: 200000, maxOutputTokens: 32000 }),
  ).type.toBe<
    Effect.Effect<
      Catalog.Descriptor,
      ModelError | AiError.AiError,
      Cli.Cli | IntentServer.IntentServer
    >
  >()
  expect(Model.make).type.not.toBeCallableWith({ model: 1 })
  expect(Protocol.decode('{}')).type.toBe<Effect.Effect<Protocol.Event, AiError.AiError>>()
})

test('curried CLI transformations preserve generic caller channels and reject ambiguous options', () => {
  expect(Turn.translate(new Map<string, string>())(source)).type.toBe<
    Stream.Stream<Response.StreamPartEncoded, typeof failure | AiError.AiError, Caller>
  >()
  const entry = { modelId: 'declared', contextWindow: 200000, maxOutputTokens: 32000 }
  expect(Catalog.descriptor()(entry)).type.toBe<
    Effect.Effect<
      Catalog.Descriptor,
      ModelError | AiError.AiError,
      Cli.Cli | IntentServer.IntentServer
    >
  >()
  expect(Catalog.descriptor({ provider: 'custom' })(entry)).type.toBe<
    Effect.Effect<
      Catalog.Descriptor,
      ModelError | AiError.AiError,
      Cli.Cli | IntentServer.IntentServer
    >
  >()
  expect(Catalog.descriptor).type.not.toBeCallableWith({})
})

import * as ProviderClaudeCode from '@effect-harness/provider-claude-code'
import * as ClaudeCodeError from '@effect-harness/provider-claude-code/ClaudeCodeError'

test('root namespaces retain canonical model and semantic error constructors', () => {
  expect(ProviderClaudeCode.ClaudeCodeLanguageModel.layer).type.toBe<typeof Model.layer>()
  expect(ProviderClaudeCode.ClaudeCodeError.authentication).type.toBe<
    typeof ClaudeCodeError.authentication
  >()
})
