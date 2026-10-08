import { inspect } from 'node:util'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as DateTime from 'effect/DateTime'
import * as Effect from 'effect/Effect'
import type * as Inspectable from 'effect/Inspectable'
import * as Redacted from 'effect/Redacted'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Pkce from 'effect-harness/auth/Pkce'
import * as OAuth from 'effect-harness/provider-anthropic/OAuth'
import * as ChatGpt from 'effect-harness/provider-openai/ChatGpt'
import * as IntentServer from 'effect-harness/provider-claude-code/IntentServer'
// effect-nit-allow P9-namespace-alias-equals-module: this interoperability fixture requires all three distinct owned Descriptor constructors and guards to prove native callback identity and cross-provider rejection; a single Catalog API cannot supply those brand contracts.
import * as AnthropicCatalog from 'effect-harness/provider-anthropic/Catalog'
import * as OpenAiCatalog from 'effect-harness/provider-openai/Catalog'
import * as ClaudeCodeCatalog from 'effect-harness/provider-claude-code/Catalog'
import * as Output from 'effect-harness/Output'
import * as LineScan from 'effect-harness/env/LineScan'
import * as Result from 'effect/Result'
import * as Exit from 'effect/Exit'
import * as Cause from 'effect/Cause'
import * as AnthropicLanguageModel from '@effect/ai-anthropic/AnthropicLanguageModel'
import * as OpenAiLanguageModel from '@effect/ai-openai/OpenAiLanguageModel'
import * as Usage from 'effect-harness/Usage'

const diagnostics = (value: Inspectable.Inspectable): ReadonlyArray<string> => [
  JSON.stringify(value),
  value.toString(),
  inspect(value),
]

describe('OwnedProtocols', () => {
  it('auth handles preserve descriptors, expiry identity, sparse extras and secrets without sampling getters', () => {
    let reads = 0
    const verifier = Redacted.make('private-verifier')
    const expiresAt = DateTime.makeUnsafe(123456)
    const holes: Array<string> = []
    holes.length = 4
    Object.defineProperty(holes, '2', {
      get: () => {
        reads++
        return 'private-sparse-value'
      },
      enumerable: true,
    })
    const opaque = Symbol('opaque')
    const input = Object.freeze({
      verifier,
      challenge: 'public-challenge',
      state: 'private-state',
      nonce: 'private-nonce',
      holes,
      ['__proto__']: { secret: 'private-prototype-data' },
      get extra(): string {
        reads++
        return 'private-extra'
      },
      get toJSON(): () => unknown {
        reads++
        return () => 'private-protocol-shadow'
      },
      [opaque]: verifier,
    })
    const first = Pkce.makeChallenge(input)
    const second = Pkce.makeChallenge(input)
    assert.isTrue(Pkce.isChallenge(first))
    assert.strictEqual(
      first.pipe((value) => value),
      first,
    )
    assert.strictEqual(Object.getPrototypeOf(first), Object.getPrototypeOf(second))
    assert.strictEqual(first.verifier, verifier)
    assert.strictEqual(Object.getOwnPropertyDescriptor(first, 'holes')?.value, holes)
    assert.strictEqual(0 in holes, false)
    assert.strictEqual(1 in holes, false)
    assert.deepStrictEqual(
      Object.getOwnPropertyDescriptor(first, 'extra'),
      Object.getOwnPropertyDescriptor(input, 'extra'),
    )
    assert.strictEqual(Object.getOwnPropertyDescriptor(first, opaque)?.value, verifier)
    assert.strictEqual(
      Object.getOwnPropertyDescriptor(first, '__proto__')?.value,
      Object.getOwnPropertyDescriptor(input, '__proto__')?.value,
    )
    assert.isUndefined(Object.getOwnPropertyDescriptor(first, 'toJSON'))
    assert.strictEqual(reads, 0)
    const secretUrl = Redacted.make('https://secret.example/authorize?secret=private-state')
    const anthropic = OAuth.makeAuthorization(
      Object.freeze({
        url: secretUrl,
        state: verifier,
        redirectUri: 'http://localhost:53692/callback',
        expiresAt,
      }),
    )
    const openai = ChatGpt.makeAuthorization(
      Object.freeze({
        url: secretUrl,
        state: 'private-state',
        redirectUri: 'http://localhost:1234/callback',
        expiresAt,
      }),
    )
    assert.strictEqual(anthropic.url, secretUrl)
    assert.strictEqual(anthropic.state, verifier)
    assert.strictEqual(anthropic.expiresAt, expiresAt)
    assert.strictEqual(openai.expiresAt, expiresAt)
    assert.strictEqual(openai.state, 'private-state')
    assert.isTrue(OAuth.isAuthorization(anthropic))
    assert.isTrue(ChatGpt.isAuthorization(openai))
    assert.isFalse(OAuth.isAuthorization(openai))
    assert.isFalse(ChatGpt.isAuthorization(anthropic))
    for (const handle of [first, anthropic, openai]) {
      assert.strictEqual(
        handle.pipe((value) => value),
        handle,
      )
      for (const text of diagnostics(handle)) {
        assert.include(text, 'effect-harness/')
        assert.isBelow(text.length, 700)
        for (const secret of [
          'private-verifier',
          'private-state',
          'private-nonce',
          'private-extra',
          'private-prototype-data',
          'private-sparse-value',
        ])
          assert.notInclude(text, secret)
      }
    }
    assert.strictEqual(reads, 0)
    for (const value of [undefined, null, 0, '', {}, input]) assert.isFalse(Pkce.isChallenge(value))
    const marker = Object.getOwnPropertyNames(first).find((name) =>
      name.startsWith('~effect-harness/'),
    )
    assert.isDefined(marker)
    if (marker !== undefined) {
      assert.strictEqual(Object.getOwnPropertyDescriptor(first, marker)?.enumerable, false)
      assert.isFalse(Pkce.isChallenge({ [marker]: undefined }))
    }
  })

  it('session handles keep the original insertion-ordered alias map without iterating it for inspection', () => {
    let visits = 0
    class AliasMap extends Map<string, string> {
      override entries(): MapIterator<[string, string]> {
        visits++
        return super.entries()
      }
      override [Symbol.iterator](): MapIterator<[string, string]> {
        visits++
        return super[Symbol.iterator]()
      }
      toJSON(): unknown {
        return 'private-alias-values'
      }
    }
    const aliases = new AliasMap([
      ['second', 'native.second'],
      ['first', 'native.first'],
    ])
    const input = Object.freeze({ url: 'http://localhost:1234/private-token', aliases })
    const session = IntentServer.makeSession(input)
    assert.strictEqual(session.aliases, aliases)
    assert.strictEqual(session.url, input.url)
    assert.isTrue(IntentServer.isSession(session))
    assert.strictEqual(
      session.pipe((value) => value),
      session,
    )
    for (const text of diagnostics(session)) {
      assert.notInclude(text, 'private-token')
      assert.notInclude(text, 'native.second')
      assert.notInclude(text, 'private-alias-values')
    }
    assert.strictEqual(visits, 0)
    assert.deepStrictEqual(
      [...session.aliases],
      [
        ['second', 'native.second'],
        ['first', 'native.first'],
      ],
    )
  })

  it.effect(
    'descriptor handles retain the exact native model and callback identities without traversing services',
    () =>
      Effect.gen(function* () {
        const model = yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.empty,
        })
        let reads = 0
        Object.defineProperty(model, 'nativeSecret', {
          get: () => {
            reads++
            throw new Error('opaque native service traversed')
          },
          enumerable: true,
        })
        const configure = () => Effect.succeed(Context.empty())
        const configureAnthropic = () =>
          Effect.succeed(
            Context.make(AnthropicLanguageModel.Config, { model: 'native-model', max_tokens: 10 }),
          )
        const configureOpenAi = () =>
          Effect.succeed(Context.make(OpenAiLanguageModel.Config, { model: 'native-model' }))
        const classify = () => ({ retryable: false, overflow: false })
        const ref = Object.freeze({ provider: 'declared', modelId: 'native-model' })
        const input = Object.freeze({
          ref,
          model,
          contextWindow: 20,
          maxOutputTokens: 10,
          configure,
          usage: Usage.make,
          classify,
        })
        const anth = AnthropicCatalog.makeDescriptor({ ...input, configure: configureAnthropic })
        const openai = OpenAiCatalog.makeDescriptor({ ...input, configure: configureOpenAi })
        const cli = ClaudeCodeCatalog.makeDescriptor(input)
        assert.isTrue(AnthropicCatalog.isDescriptor(anth))
        assert.isTrue(OpenAiCatalog.isDescriptor(openai))
        assert.isTrue(ClaudeCodeCatalog.isDescriptor(cli))
        assert.isFalse(OpenAiCatalog.isDescriptor(anth))
        for (const handle of [anth, openai, cli]) {
          assert.strictEqual(handle.model, model)
          let selected = configure
          if (handle === anth) selected = configureAnthropic
          else if (handle === openai) selected = configureOpenAi
          assert.strictEqual(handle.configure, selected)
          assert.strictEqual(handle.usage, Usage.make)
          assert.strictEqual(handle.classify, classify)
          assert.strictEqual(handle.ref, ref)
          assert.strictEqual(
            handle.pipe((value) => value),
            handle,
          )
          for (const text of diagnostics(handle)) {
            assert.notInclude(text, 'nativeSecret')
            assert.notInclude(text, 'native-model')
            assert.isBelow(text.length, 300)
          }
        }
        assert.strictEqual(reads, 0)
        const accessorInput = Object.freeze({
          ...input,
          configure: configureAnthropic,
          get model(): LanguageModel.LanguageModel {
            reads++
            throw new Error('model getter sampled')
          },
        })
        const opaque = AnthropicCatalog.makeDescriptor(accessorInput)
        assert.deepStrictEqual(
          Object.getOwnPropertyDescriptor(opaque, 'model'),
          Object.getOwnPropertyDescriptor(accessorInput, 'model'),
        )
        diagnostics(opaque)
        assert.strictEqual(reads, 0)
      }),
  )

  it.effect(
    'each Window owns one decoder; piping and inspection preserve split UTF-8 and independent state',
    () =>
      Effect.gen(function* () {
        let constructed = 0
        let decoded = 0
        const original = yield* Effect.acquireRelease(
          Effect.sync(() => {
            const Original = globalThis.TextDecoder
            globalThis.TextDecoder = class extends Original {
              constructor(...args: ConstructorParameters<typeof Original>) {
                super(...args)
                constructed++
              }
              override decode(
                ...args: Parameters<InstanceType<typeof Original>['decode']>
              ): string {
                decoded++
                return super.decode(...args)
              }
            }
            return Original
          }),
          (Original) =>
            Effect.sync(() => {
              globalThis.TextDecoder = Original
            }),
        )
        const first = yield* Output.makeWindow()
        const second = yield* Output.makeWindow()
        assert.strictEqual(constructed, 2)
        assert.strictEqual(Object.getPrototypeOf(first), Object.getPrototypeOf(second))
        assert.isTrue(Output.isWindow(first))
        assert.strictEqual(
          first.pipe((value) => value),
          first,
        )
        const push = first.push
        const snapshot = first.snapshot
        yield* first.push(new Uint8Array([0xe2]))
        const before = decoded
        diagnostics(first)
        assert.strictEqual(decoded, before)
        assert.strictEqual(first.push, push)
        assert.strictEqual(first.snapshot, snapshot)
        yield* first.push(new Uint8Array([0x82, 0xac]))
        yield* first.end
        assert.strictEqual((yield* first.snapshot).text, '€')
        yield* second.push('other')
        assert.strictEqual((yield* second.snapshot).text, 'other')
        yield* first.reset
        assert.strictEqual(constructed, 2)
        assert.strictEqual((yield* first.snapshot).text, '')
        assert.strictEqual((yield* second.snapshot).text, 'other')
        assert.notStrictEqual(globalThis.TextDecoder, original)
      }).pipe(Effect.scoped),
  )
  it.effect(
    'private Result siblings retain exact native fault causes at public mutation boundaries',
    () =>
      Effect.gen(function* () {
        const outputCause = new Error('native output getter')
        const buffer = Output.make()
        Object.defineProperty(buffer, 'full', {
          get: () => {
            throw outputCause
          },
        })
        const output = yield* Effect.exit(Output.push(buffer, 'x'))
        assert.isTrue(Exit.isFailure(output))
        if (Exit.isFailure(output)) {
          assert.strictEqual(Result.getOrThrow(Cause.findError(output.cause)).cause, outputCause)
        }
        const scanCause = new Error('native scanner getter')
        const scanner = Result.getOrThrow(LineScan.make(0))
        Object.defineProperty(scanner, 'selection', {
          get: () => {
            throw scanCause
          },
        })
        const scanned = yield* Effect.exit(
          LineScan.push(scanner, new TextEncoder().encode('abc\n')),
        )
        assert.isTrue(Exit.isFailure(scanned))
        if (Exit.isFailure(scanned)) {
          assert.strictEqual(Result.getOrThrow(Cause.findError(scanned.cause)).cause, scanCause)
        }
      }),
  )
})
