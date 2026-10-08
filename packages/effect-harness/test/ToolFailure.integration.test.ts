import { assertFailure } from '@effect/vitest/utils'
import { FileInvalidError, FileUnknownError } from 'effect-harness/FileError'
import * as NodeNativeFiles from 'effect-harness/NodeNativeFiles'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Cause from 'effect/Cause'

import * as Tool from 'effect/ai/Tool'

import * as Toolkit from 'effect/ai/Toolkit'

import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'

import * as Prompt from 'effect/ai/Prompt'

import * as PlatformError from 'effect/PlatformError'

import * as Model from 'effect-harness/Model'

import * as ToolRegistration from 'effect-harness/ToolRegistration'

import * as Transcript from 'effect-harness/Transcript'

import * as Compaction from 'effect-harness/Compaction'

import * as LineScan from 'effect-harness/env/LineScan'

import { fromPlatform, Env } from 'effect-harness/Env'
import { FileError } from 'effect-harness/FileError'
import { ExecutionError, ExecutionCallbackError } from 'effect-harness/ExecutionError'

import * as ModelError from 'effect-harness/ModelError'

import * as ToolError from 'effect-harness/ToolError'

import * as Registry from 'effect-harness/Registry'

import * as HookError from 'effect-harness/HookError'

import * as OutputError from 'effect-harness/OutputError'

import * as Serialization from 'effect-harness/Serialization'

import * as Bash from 'effect-harness/tools/Bash'

import * as Usage from 'effect-harness/Usage'

import { ToolCall } from 'effect-harness/Invocation'

import { withEnv } from './EnvFixture.ts'

import { recording } from './InvocationRecorder.ts'

describe('ToolFailure', () => {
  describe('FailureBoundaries', () => {
    it('line scanner validates ranges in the Result channel', () => {
      const result = LineScan.make(-1)
      assertFailure(
        result,
        new FileError({ reason: new FileInvalidError({ message: 'Invalid line range' }) }),
      )
    })
    it('foreign filesystem errors keep the exact caught value in a structured reason', () => {
      const cause = PlatformError.badArgument({
        module: 'FileSystem',
        method: 'open',
        description: 'invalid open',
      })
      const error = fromPlatform(cause, '/file')
      assert.strictEqual(error.reason._tag, 'FileInvalidError')
      assert.strictEqual(error.cause, cause)
      assert.strictEqual(error.reason.cause, cause)
      assert.strictEqual(error.path, '/file')
    })
    it.effect('foreign Error causes roundtrip with their message through the schema codec', () =>
      Effect.gen(function* () {
        const source = fromPlatform(
          PlatformError.systemError({
            _tag: 'Unknown',
            module: 'FileSystem',
            method: 'open',
            cause: new Error('root filesystem failure'),
          }),
        )
        const codec = Schema.toCodecJson(FileError)
        const wire = {
          _tag: 'FileError',
          reason: {
            _tag: 'FileUnknownError',
            message: 'Unknown: FileSystem.open',
            cause: {
              name: 'PlatformError',
              message: 'Unknown: FileSystem.open',
              cause: { name: 'Error', message: 'root filesystem failure' },
            },
          },
        }
        const expectedCause = new Error('Unknown: FileSystem.open', {
          cause: new Error('root filesystem failure'),
        })
        expectedCause.name = 'PlatformError'
        const expected = new FileError({
          reason: new FileUnknownError({
            message: 'Unknown: FileSystem.open',
            cause: expectedCause,
          }),
        })
        const assertions = new TestSchema.Asserts(codec)
        yield* assertions.encoding().succeedEffect(source, wire)
        yield* assertions.decoding().succeedEffect(wire, expected)
        const decoded = yield* Schema.decodeEffect(codec)(wire)
        assert.ok(decoded.cause instanceof Error)
        if (decoded.cause instanceof Error)
          assert.match(decoded.cause.message, /FileSystem|filesystem/)
        assert.strictEqual(decoded.code, source.code)
        assert.strictEqual(decoded.message, source.message)
      }),
    )
    for (const [name, value] of [
      ['bigint', 1n],
      [
        'circular',
        (() => {
          const value: { self?: unknown } = {}
          value.self = value
          return value
        })(),
      ],
      [
        'throwing toJSON',
        {
          // effect-nit-allow P7-v4-data-type-naming: JSON.stringify invokes the fixed toJSON protocol slot; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
          toJSON() {
            throw new Error('cannot encode')
          },
        },
      ],
      [
        'throwing coercion',
        {
          // effect-nit-allow P7-v4-data-type-naming: JSON.stringify invokes the fixed toJSON protocol slot; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
          toJSON() {
            throw new Error('cannot encode')
          },
          // effect-nit-allow P7-v4-data-type-naming: native string coercion invokes the fixed toString protocol slot; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
          toString() {
            throw new Error('cannot coerce')
          },
        },
      ],
      ['undefined', undefined],
    ] as const) {
      it(`safe formatting marks ${name} as unencodable without throwing`, () => {
        assert.match(Model.errorText(value), /unencodable/)
        const projected = ToolRegistration.defaultProject(value, value, false)
        const first = projected.content?.[0]
        assert.ok(first?.type === 'text')
        if (first?.type === 'text') assert.match(first.text, /unencodable/)
        const call = Prompt.assistantMessage({
          content: [
            Prompt.toolCallPart({
              id: 'call',
              name: 'tool',
              params: value,
              providerExecuted: false,
            }),
          ],
        })
        assert.ok(Number.isFinite(Transcript.estimateMessage(call)))
        assert.match(Compaction.serializeConversation([call]), /unencodable/)
      })
    }
    it('compaction arguments preserve JSON quoting for scalar strings and object values', () => {
      const call = (params: unknown) =>
        Prompt.assistantMessage({
          content: [
            Prompt.toolCallPart({ id: 'call', name: 'tool', params, providerExecuted: false }),
          ],
        })
      assert.strictEqual(
        Compaction.serializeConversation([call('foo')]),
        '[Assistant tool calls]: tool("foo")',
      )
      assert.strictEqual(
        Compaction.serializeConversation([call({ name: 'foo', values: ['bar'], count: 2 })]),
        '[Assistant tool calls]: tool(name="foo", values=["bar"], count=2)',
      )
    })
    it('strict serialization retains thrown causes while undefined has no invented cause', () => {
      const cause = new Error('exact conversion cause')
      const result = Serialization.stringify({
        // effect-nit-allow P7-v4-data-type-naming: JSON.stringify invokes the fixed toJSON protocol slot; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
        toJSON() {
          throw cause
        },
      })
      assertFailure(
        result,
        new Serialization.SerializationError({ message: 'Value cannot be encoded as JSON', cause }),
      )
      const unsupported = Serialization.stringify(undefined)
      assertFailure(
        unsupported,
        new Serialization.SerializationError({ message: 'Value has no JSON representation' }),
      )
    })
    it('foreign property getters and Error messages are guarded before display and estimation', () => {
      const failure = new Error('before')
      Object.defineProperty(failure, 'message', {
        // effect-nit-allow P7-v4-data-type-naming: PropertyDescriptor requires the exact get callback name; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
        get() {
          throw new Error('hostile message')
        },
      })
      assert.strictEqual(Model.errorText(failure), Serialization.unencodable)
      const hostile = new Proxy(
        {},
        {
          // effect-nit-allow P7-v4-data-type-naming: ProxyHandler requires the exact get trap name; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
          get() {
            throw new Error('hostile getter')
          },
        },
      )
      const tool = Prompt.toolMessage({
        content: [
          Prompt.toolResultPart({
            id: 'call',
            name: 'tool',
            result: hostile,
            isFailure: false,
            providerExecuted: false,
          }),
        ],
      })
      assert.ok(Number.isFinite(Transcript.estimateMessage(tool)))
      assert.match(Compaction.serializeConversation([tool]), /unencodable/)
      const args = Object.defineProperty({}, 'value', {
        enumerable: true,
        // effect-nit-allow P7-v4-data-type-naming: PropertyDescriptor requires the exact get callback name; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
        get() {
          throw new Error('hostile args')
        },
      })
      const call = Prompt.assistantMessage({
        content: [
          Prompt.toolCallPart({ id: 'call', name: 'tool', params: args, providerExecuted: false }),
        ],
      })
      assert.match(Compaction.serializeConversation([call]), /unencodable/)
      const foreign = Object.defineProperty({}, 'code', {
        enumerable: true,
        // effect-nit-allow P7-v4-data-type-naming: PropertyDescriptor requires the exact get callback name; this hostile fixture must throw the original foreign cause to exercise the guarded boundary.
        get() {
          throw new Error('hostile code')
        },
      })
      const error = NodeNativeFiles.fileError(foreign, '/file')
      assert.strictEqual(error.reason._tag, 'FileUnknownError')
      assert.strictEqual(error.cause, foreign)
      assert.strictEqual(error.message, Serialization.unencodable)
    })
    it.effect(
      'all owned wrappers delegate their tagged reason and preserve Error codec provenance',
      () =>
        Effect.gen(function* () {
          const cause = new Error('foreign provenance', { cause: new Error('nested provenance') })
          const usage = Usage.make()
          const model = new ModelError.ModelError({
            reason: new ModelError.ModelInvalidResponseError({
              message: 'exact caller message',
              cause,
              usage,
            }),
          })
          assert.strictEqual(model._tag, 'ModelError')
          assert.strictEqual(model.message, 'exact caller message')
          assert.strictEqual(model.usage, model.reason.usage)
          assert.deepStrictEqual(model.usage, usage)
          assert.strictEqual(model.isRetryable, false)
          const tool = new ToolError.ToolError({
            reason: new ToolError.ToolExecutionError({
              name: 'caller-tool',
              message: 'tool message',
              cause,
            }),
          })
          assert.strictEqual(tool.name, 'caller-tool')
          assert.strictEqual(tool._tag, 'ToolError')
          assert.strictEqual(tool.cause, cause)
          const codec = Schema.toCodecJson(
            Schema.Union([
              ModelError.ModelError,
              ToolError.ToolError,
              Registry.RegistryError,
              HookError.HookError,
              OutputError.OutputError,
            ]),
          )
          const defect = {
            name: 'Error',
            message: 'foreign provenance',
            cause: { name: 'Error', message: 'nested provenance' },
          }
          const expectedCause = new Error('foreign provenance', {
            cause: new Error('nested provenance'),
          })
          const measuredUsage = {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 0,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
          }
          const cases = [
            [
              model,
              {
                _tag: 'ModelError',
                reason: {
                  _tag: 'ModelInvalidResponseError',
                  message: 'exact caller message',
                  cause: defect,
                  usage: measuredUsage,
                },
              },
              new ModelError.ModelError({
                reason: new ModelError.ModelInvalidResponseError({
                  message: 'exact caller message',
                  cause: expectedCause,
                  usage: measuredUsage,
                }),
              }),
            ],
            [
              tool,
              {
                _tag: 'ToolError',
                reason: {
                  _tag: 'ToolExecutionError',
                  name: 'caller-tool',
                  message: 'tool message',
                  cause: defect,
                },
              },
              new ToolError.ToolError({
                reason: new ToolError.ToolExecutionError({
                  name: 'caller-tool',
                  message: 'tool message',
                  cause: expectedCause,
                }),
              }),
            ],
            [
              new Registry.RegistryError({
                reason: new Registry.RegistryFailureError({ message: 'registry', cause }),
              }),
              {
                _tag: 'RegistryError',
                reason: { _tag: 'RegistryFailureError', message: 'registry', cause: defect },
              },
              new Registry.RegistryError({
                reason: new Registry.RegistryFailureError({
                  message: 'registry',
                  cause: expectedCause,
                }),
              }),
            ],
            [
              new HookError.HookError({
                reason: new HookError.HookFailureError({ message: 'hook', cause }),
              }),
              {
                _tag: 'HookError',
                reason: { _tag: 'HookFailureError', message: 'hook', cause: defect },
              },
              new HookError.HookError({
                reason: new HookError.HookFailureError({ message: 'hook', cause: expectedCause }),
              }),
            ],
            [
              new OutputError.OutputError({
                reason: new OutputError.OutputFailureError({ message: 'output', cause }),
              }),
              {
                _tag: 'OutputError',
                reason: { _tag: 'OutputFailureError', message: 'output', cause: defect },
              },
              new OutputError.OutputError({
                reason: new OutputError.OutputFailureError({
                  message: 'output',
                  cause: expectedCause,
                }),
              }),
            ],
          ] as const
          const assertions = new TestSchema.Asserts(codec)
          for (const [value, wire, expected] of cases) {
            yield* assertions.encoding().succeedEffect(value, wire)
            yield* assertions.decoding().succeedEffect(wire, expected)
            const decoded = yield* Schema.decodeEffect(codec)(wire)
            assert.strictEqual(decoded.message, expected.message)
            assert.ok(decoded.cause instanceof Error)
            if (decoded.cause instanceof Error)
              assert.strictEqual(decoded.cause.message, 'foreign provenance')
          }
        }),
    )
    it.effect('native toolkit failures retain the exact caught handler value', () =>
      withEnv(
        Effect.gen(function* () {
          const cause = new Error('handler cause')
          const tool = Tool.make('fails', {
            parameters: Schema.Struct({}),
            success: Schema.String,
            failure: Schema.Unknown,
          })
          const toolkit = Toolkit.make(tool)
          const registrations = yield* ToolRegistration.bind(toolkit).pipe(
            Effect.provide(toolkit.toLayer({ fails: () => Effect.fail(cause) })),
          )
          const registration = registrations[0]
          assert.isDefined(registration)
          if (registration === undefined) return
          const captured = yield* recording
          const failure = yield* Effect.flip(registration.execute({}, 'call')).pipe(
            Effect.provideService(ToolCall, captured.api),
          )
          assert.strictEqual(failure.reason._tag, 'ToolExecutionError')
          assert.strictEqual(failure.cause, cause)
          assert.strictEqual(failure.message, 'handler cause')
        }),
      ),
    )
    // Native child-process spawn, pipe delivery and kill/join callbacks advance on the host loop; TestClock cannot drive them.
    it.live(
      'process callback failures retain Cause; Bash retains last failure and has no fabricated empty-command cause',
      () =>
        withEnv(
          Effect.gen(function* () {
            const env = yield* Env
            const source = new ExecutionError({
              reason: new ExecutionCallbackError({ message: 'callback cause' }),
            })
            const failed = yield* Effect.flip(
              env.exec('printf callback', { onOutput: () => Effect.fail(source) }),
            )
            assert.strictEqual(failed.code, 'callback_error')
            assert.deepStrictEqual(failed.cause, Cause.fail(source))
            const captured = yield* recording
            const custom = Env.of({ ...env, exec: () => Effect.fail(source) })
            const last = yield* Effect.flip(
              Bash.powerShellHandler({ programs: ['fails'] })({ command: 'x' }),
            ).pipe(
              Effect.provideService(Env, custom),
              Effect.provideService(ToolCall, captured.api),
            )
            assert.strictEqual(last.cause, source)
            const empty = yield* Effect.flip(
              Bash.powerShellHandler({ programs: [] })({ command: 'x' }),
            ).pipe(Effect.provideService(ToolCall, captured.api))
            assert.strictEqual(empty.cause, undefined)
          }),
        ),
    )
  })
})
