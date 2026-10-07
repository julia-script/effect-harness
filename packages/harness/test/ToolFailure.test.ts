import * as NodeNativeFiles from '@effect-harness/harness/NodeNativeFiles'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as AiTool from 'effect/ai/Tool'
import * as Toolkit from 'effect/ai/Toolkit'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as PlatformError from 'effect/PlatformError'
import * as Model from '@effect-harness/harness/Model'
import * as Tool from '@effect-harness/harness/Tool'
import * as Context from '@effect-harness/harness/Context'
import * as Compaction from '@effect-harness/harness/Compaction'
import * as LineScan from '@effect-harness/harness/env/LineScan'
import {
  fromPlatform,
  FileError,
  Env,
  ExecutionError,
  ExecutionCallbackError,
} from '@effect-harness/harness/Env'
import * as ModelError from '@effect-harness/harness/ModelError'
import * as ToolError from '@effect-harness/harness/ToolError'
import * as RegistryError from '@effect-harness/harness/RegistryError'
import * as HookError from '@effect-harness/harness/HookError'
import * as OutputError from '@effect-harness/harness/OutputError'
import * as Serialization from '@effect-harness/harness/Serialization'
import * as Bash from '@effect-harness/harness/tools/Bash'
import * as Usage from '@effect-harness/harness/Usage'
import { ToolCall } from '@effect-harness/harness/Invocation'
import { withEnv, recording } from './tools/Helpers.ts'

describe('ToolFailure', () => {
  describe('FailureBoundaries', () => {
    it('line scanner validates ranges in the Result channel', () => {
      const result = LineScan.make(-1)
      assert.strictEqual(Result.isFailure(result), true)
      if (Result.isFailure(result)) assert.strictEqual(result.failure.code, 'invalid')
    })
    it('foreign filesystem errors keep the exact caught value in a structured reason', () => {
      const cause = PlatformError.badArgument({
        module: 'FileSystem',
        method: 'open',
        description: 'invalid open',
      })
      const error = fromPlatform(cause, '/file')
      assert.strictEqual(error.reason._tag, 'FileInvalid')
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
        const decoded = yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(source))
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
          toJSON() {
            throw new Error('cannot encode')
          },
        },
      ],
      [
        'throwing coercion',
        {
          toJSON() {
            throw new Error('cannot encode')
          },
          toString() {
            throw new Error('cannot coerce')
          },
        },
      ],
      ['undefined', undefined],
    ] as const) {
      it(`safe formatting marks ${name} as unencodable without throwing`, () => {
        assert.match(Model.errorText(value), /unencodable/)
        const projected = Tool.defaultProject(value, value, false)
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
        assert.ok(Number.isFinite(Context.estimateMessage(call)))
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
        toJSON() {
          throw cause
        },
      })
      assert.strictEqual(Result.isFailure(result), true)
      if (Result.isFailure(result)) assert.strictEqual(result.failure.cause, cause)
      const unsupported = Serialization.stringify(undefined)
      assert.strictEqual(Result.isFailure(unsupported), true)
      if (Result.isFailure(unsupported)) assert.strictEqual(unsupported.failure.cause, undefined)
    })
    it('foreign property getters and Error messages are guarded before display and estimation', () => {
      const failure = new Error('before')
      Object.defineProperty(failure, 'message', {
        get() {
          throw new Error('hostile message')
        },
      })
      assert.strictEqual(Model.errorText(failure), Serialization.unencodable)
      const hostile = new Proxy(
        {},
        {
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
      assert.ok(Number.isFinite(Context.estimateMessage(tool)))
      assert.match(Compaction.serializeConversation([tool]), /unencodable/)
      const args = Object.defineProperty({}, 'value', {
        enumerable: true,
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
        get() {
          throw new Error('hostile code')
        },
      })
      const error = NodeNativeFiles.fileError(foreign, '/file')
      assert.strictEqual(error.reason._tag, 'FileUnknown')
      assert.strictEqual(error.cause, foreign)
      assert.strictEqual(error.message, Serialization.unencodable)
    })
    it.effect(
      'all owned wrappers delegate their tagged reason and preserve Error codec provenance',
      () =>
        Effect.gen(function* () {
          const cause = new Error('foreign provenance', { cause: new Error('nested provenance') })
          const usage = Usage.zero()
          const model = new ModelError.ModelError({
            reason: new ModelError.ModelInvalidResponse({
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
            reason: new ToolError.ToolExecution({
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
              RegistryError.RegistryError,
              HookError.HookError,
              OutputError.OutputError,
            ]),
          )
          for (const value of [
            model,
            tool,
            new RegistryError.RegistryError({
              reason: new RegistryError.RegistryFailure({ message: 'registry', cause }),
            }),
            new HookError.HookError({
              reason: new HookError.HookFailure({ message: 'hook', cause }),
            }),
            new OutputError.OutputError({
              reason: new OutputError.OutputFailure({ message: 'output', cause }),
            }),
          ]) {
            const decoded = yield* Schema.decodeEffect(codec)(
              yield* Schema.encodeEffect(codec)(value),
            )
            assert.strictEqual(decoded.message, value.message)
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
          const tool = AiTool.make('fails', {
            parameters: Schema.Struct({}),
            success: Schema.String,
            failure: Schema.Unknown,
          })
          const toolkit = Toolkit.make(tool)
          const registrations = yield* Tool.bind(toolkit).pipe(
            Effect.provide(toolkit.toLayer({ fails: () => Effect.fail(cause) })),
          )
          const registration = registrations[0]
          assert.isDefined(registration)
          if (registration === undefined) return
          const captured = yield* recording
          const failure = yield* Effect.flip(registration.execute({}, 'call')).pipe(
            Effect.provideService(ToolCall, captured.api),
          )
          assert.strictEqual(failure.reason._tag, 'ToolExecution')
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
            assert.ok(Cause.isCause(failed.cause))
            if (Cause.isCause(failed.cause)) assert.strictEqual(Cause.squash(failed.cause), source)
            const captured = yield* recording
            const custom: Env['Service'] = { ...env, exec: () => Effect.fail(source) }
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
