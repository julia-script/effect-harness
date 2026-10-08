import { describe, expect, it } from 'tstyche'
import * as Turn from 'effect-harness/provider-claude-code/Turn'
import * as OAuth from 'effect-harness/provider-anthropic/OAuth'
import * as ChatGpt from 'effect-harness/provider-openai/ChatGpt'
import * as Config from 'effect/Config'
import * as Context from 'effect/Context'
import type * as Effect from 'effect/Effect'
import type * as Stream from 'effect/Stream'
import type * as Protocol from 'effect-harness/provider-claude-code/Protocol'
import type * as AiError from 'effect/ai/AiError'
import type * as Response from 'effect/ai/Response'

class Caller extends Context.Service<Caller, { readonly value: string }>()(
  'test/residual/Caller',
) {}
declare const events: Stream.Stream<Protocol.Event, { readonly _tag: 'CallerError' }, Caller>

describe('ResidualContracts', () => {
  it('translation preserves caller failure and service types in both overloads', () => {
    expect(Turn.translate(events, new Map())).type.toBe<
      Stream.Stream<
        Response.StreamPartEncoded,
        { readonly _tag: 'CallerError' } | AiError.AiError,
        Caller
      >
    >()
    expect(Turn.translate(new Map())(events)).type.toBe<
      Stream.Stream<
        Response.StreamPartEncoded,
        { readonly _tag: 'CallerError' } | AiError.AiError,
        Caller
      >
    >()
    expect(Turn.collect(Turn.translate(events, new Map()))).type.toBe<
      Effect.Effect<
        Array<Response.PartEncoded>,
        { readonly _tag: 'CallerError' } | AiError.AiError,
        Caller
      >
    >()
  })
  it('admits native duration layer and config options', () => {
    expect(OAuth.layer({ authorizationLifetime: '2 seconds', refreshSkew: 0 })).type.toBe<
      ReturnType<typeof OAuth.layer>
    >()
    expect(ChatGpt.layer({ appName: 'typed', authorizationLifetime: 123.25 })).type.toBe<
      ReturnType<typeof ChatGpt.layer>
    >()
    expect(OAuth.layerConfig({ authorizationLifetime: Config.Duration('LIFETIME') })).type.toBe<
      ReturnType<typeof OAuth.layerConfig>
    >()
    expect(
      ChatGpt.layerConfig({ appName: Config.String('APP'), refreshSkew: Config.Duration('SKEW') }),
    ).type.toBe<ReturnType<typeof ChatGpt.layerConfig>>()
  })
})
