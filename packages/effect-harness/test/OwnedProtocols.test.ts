import { inspect } from 'node:util'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Inspectable from 'effect/Inspectable'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
// effect-nit-allow P9-namespace-alias-equals-module: this interoperability fixture requires both distinct owned Descriptor constructors and guards to prove native callback identity and cross-provider rejection; a single Catalog API cannot supply those brand contracts.
import * as AnthropicCatalog from 'effect-harness/provider-anthropic/Catalog'
import * as OpenAiCatalog from 'effect-harness/provider-openai/Catalog'
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
        assert.isTrue(AnthropicCatalog.isDescriptor(anth))
        assert.isTrue(OpenAiCatalog.isDescriptor(openai))
        assert.isFalse(OpenAiCatalog.isDescriptor(anth))
        assert.strictEqual(anth.configure, configureAnthropic)
        assert.strictEqual(openai.configure, configureOpenAi)
        for (const handle of [anth, openai]) {
          assert.strictEqual(handle.model, model)
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
