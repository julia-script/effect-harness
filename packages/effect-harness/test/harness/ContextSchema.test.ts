import * as Duration from 'effect/Duration'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Option from 'effect/Option'
import * as NativePrompt from 'effect/ai/Prompt'
import * as Agent from 'effect-harness/Agent'
import * as Identity from 'effect-harness/Identity'
import * as Context from 'effect-harness/Context'
import * as Managed from 'effect-harness/SystemPatch'
import * as Executor from 'effect-harness/Executor'
import * as Invocation from 'effect-harness/Invocation'
import * as Model from 'effect-harness/Model'
import * as Tool from 'effect-harness/Tool'
import * as Usage from 'effect-harness/Usage'
import * as ModelError from 'effect-harness/ModelError'
import * as Json from 'effect-harness/Json'
import * as Response from 'effect-harness/Response'
import * as Compaction from 'effect-harness/Compaction'
import * as Edit from 'effect-harness/tools/Edit'
import * as Bash from 'effect-harness/tools/Bash'
import * as Read from 'effect-harness/tools/Read'

describe('ContextSchema', () => {
  const serializedTool = (result: unknown) =>
    Compaction.serializeConversation([
      NativePrompt.toolMessage({
        content: [
          NativePrompt.toolResultPart({
            id: 'c',
            name: 't',
            result,
            isFailure: false,
            providerExecuted: false,
          }),
        ],
      }),
    ])

  describe('SchemaContracts', () => {
    it.effect(
      'canonical IDs validate exact numeric bounds, remain wire numbers and preserve nominal separation',
      () =>
        Effect.gen(function* () {
          const entry = yield* Schema.decodeEffect(Identity.EntryId)(Number.MAX_SAFE_INTEGER)
          const conversation = yield* Schema.decodeEffect(Identity.ConversationId)(1)
          assert.strictEqual(
            yield* Schema.encodeEffect(Identity.EntryId)(entry),
            Number.MAX_SAFE_INTEGER,
          )
          assert.strictEqual(yield* Schema.encodeEffect(Identity.ConversationId)(conversation), 1)
          const nominal: Identity.EntryId extends Identity.ConversationId ? true : false = false
          const encoded: typeof Identity.EntryId.Encoded extends number ? true : false = true
          void nominal
          void encoded
          for (const invalid of [0, -1, 1.2, Number.MAX_SAFE_INTEGER + 1, Infinity, '1']) {
            assert.strictEqual(
              Option.isNone(Schema.decodeUnknownOption(Identity.EntryId)(invalid)),
              true,
            )
            assert.strictEqual(
              Option.isNone(Schema.decodeUnknownOption(Identity.ConversationId)(invalid)),
              true,
            )
          }
          assert.deepStrictEqual(
            yield* Schema.decodeEffect(Context.Edit)({ action: 'omit', target: 3 }),
            { _tag: 'omit', target: yield* Schema.decodeEffect(Identity.EntryId)(3) },
          )
          assert.strictEqual(Executor.SummaryRequest.fields.firstKept, Identity.EntryId)
          assert.strictEqual(Executor.SummaryRequest.fields.tail, Identity.EntryId)
        }),
    )
    it.effect(
      'canonical managed patch preserves prior bytes, opaque JSON arguments and exact shared schema bindings',
      () =>
        Effect.gen(function* () {
          assert.strictEqual(Context.ToolDeclaration, Managed.ToolDeclaration)
          assert.strictEqual(Context.SystemPatch, Managed.SystemPatch)
          assert.strictEqual(Executor.Request.fields.tools.value, Managed.ToolDeclaration)
          const wire = {
            sections: { deleted: null, active: 'text' },
            toolsRemoved: ['old'],
            toolsAdded: [
              {
                name: 'search',
                parameters: { type: 'object', arbitrary: [1, null, true] },
                provider: {
                  id: 'custom.search',
                  name: 'remote',
                  args: { opaque: ['native', { extra: 7 }] },
                },
              },
            ],
          }
          const codec = Schema.fromJsonString(Schema.toCodecJson(Managed.SystemPatch))
          const decoded = yield* Schema.decodeEffect(codec)(JSON.stringify(wire))
          assert.deepStrictEqual(decoded, wire)
          assert.strictEqual(yield* Schema.encodeEffect(codec)(decoded), JSON.stringify(wire))
          assert.strictEqual(
            Option.isNone(
              Schema.decodeUnknownOption(Managed.ToolDeclaration)({
                name: 'bad',
                parameters: [],
                provider: { id: 'missingNamespace', name: 'bad', args: {} },
              }),
            ),
            true,
          )
          assert.strictEqual(
            Option.isNone(
              Schema.decodeUnknownOption(Managed.ToolDeclaration)({
                name: 'bad',
                parameters: {},
                provider: { id: 'custom.bad', name: 'bad', args: () => 'opaque function' },
              }),
            ),
            true,
          )
        }),
    )
    it.effect(
      'authoritative policy types preserve defaults, explicit undefined and opaque stream options',
      () =>
        Effect.gen(function* () {
          const opaque = { callback: () => 'SDK owned', bytes: new Uint8Array([1, 2]) }
          const settings = yield* Agent.settings({
            extensions: undefined,
            stream: opaque,
            retry: { enabled: false },
            compaction: { backgroundTokens: 0 },
            progress: { partialIntervalMs: undefined },
          })
          const decoded = yield* Schema.decodeEffect(Schema.toType(Agent.Settings))(settings)
          assert.strictEqual(decoded.stream['callback'], opaque.callback)
          assert.strictEqual(decoded.stream['bytes'], opaque.bytes)
          assert.strictEqual(decoded.retry.enabled, false)
          assert.strictEqual(Duration.toMillis(decoded.retry.baseDelayMs), 2000)
          assert.strictEqual(decoded.compaction.backgroundTokens, 0)
          assert.strictEqual(Duration.toMillis(decoded.progress.partialIntervalMs), 100)
          const policy: Agent.RetryPolicy = Agent.defaultRetry
          const settingsType: Agent.Settings = decoded
          void policy
          void settingsType
          assert.deepStrictEqual(
            yield* Schema.decodeEffect(Agent.State)({
              model: undefined,
              thinking: undefined,
              extensions: undefined,
              tools: undefined,
              instructions: undefined,
              cwd: undefined,
            }),
            {
              model: undefined,
              thinking: undefined,
              extensions: undefined,
              tools: undefined,
              instructions: undefined,
              cwd: undefined,
            },
          )
        }),
    )
    it.effect(
      'owned optional result, usage, error, intent and tool input fields admit undefined while JSON omits it',
      () =>
        Effect.gen(function* () {
          const result = yield* Schema.decodeEffect(Invocation.Result)({
            content: undefined,
            details: undefined,
            diagnostics: undefined,
            control: { terminate: undefined, reset: { note: undefined }, addTools: undefined },
            usage: undefined,
            isError: undefined,
          })
          assert.strictEqual(
            JSON.stringify(
              yield* Schema.encodeEffect(Schema.toCodecJson(Invocation.Result))(result),
            ),
            '{"control":{"reset":{}}}',
          )
          const usage = yield* Schema.decodeEffect(Usage.Usage)({
            ...Usage.zero(),
            reasoning: undefined,
            cacheWrite1h: undefined,
            cost: { ...Usage.zero().cost, known: undefined, totalKnown: undefined },
          })
          assert.strictEqual(usage.reasoning, undefined)
          const error = yield* Schema.decodeEffect(ModelError.ModelNoModel)({
            _tag: 'ModelNoModel',
            message: 'missing',
            cause: undefined,
            usage: undefined,
          })
          assert.strictEqual(error.usage, undefined)
          yield* Schema.decodeEffect(Model.RequestOptions)({
            thinking: 'off',
            options: {},
            sessionId: undefined,
            maxTokens: undefined,
            cache: undefined,
          })
          yield* Schema.decodeEffect(Model.DeferredDecision)({
            handle: {},
            pollAfterMs: undefined,
          })
          yield* Schema.decodeEffect(Tool.Intent)({
            id: 'c',
            name: 't',
            args: {},
            encodedArgs: undefined,
            replay: 'safe',
          })
          yield* Schema.decodeEffect(Bash.Parameters)({ command: 'true', timeout: undefined })
          yield* Schema.decodeEffect(Read.Parameters)({
            path: 'file',
            offset: undefined,
            limit: undefined,
          })
        }),
    )
    it.effect(
      'tolerant Edit repair preserves unrelated keys, arbitrary arrays and unchanged input fallback',
      () =>
        Effect.gen(function* () {
          const original = [{ oldText: 'a', newText: 'b' }]
          assert.strictEqual(yield* Edit.repair(original), original)
          const arbitrary = [1, null, { other: true }]
          const value = { path: 'file', edits: arbitrary, extra: { keep: true } }
          assert.deepStrictEqual(yield* Edit.repair(value), value)
          assert.deepStrictEqual(
            yield* Edit.repair({ path: 'file', edits: '[1,null,{"other":true}]', extra: 'kept' }),
            { path: 'file', edits: arbitrary, extra: 'kept' },
          )
          assert.deepStrictEqual(
            yield* Edit.repair({
              path: 'file',
              edits: '{"oldText":"a","newText":"b"}',
              extra: 'kept',
            }),
            { path: 'file', edits: original, extra: 'kept' },
          )
          assert.deepStrictEqual(
            yield* Edit.repair({
              path: 'file',
              edits: original[0],
              oldText: 'c',
              newText: 'd',
              extra: 9,
            }),
            { path: 'file', edits: [...original, { oldText: 'c', newText: 'd' }], extra: 9 },
          )
          const invalid = { path: 'file', edits: 'not JSON', oldText: 1, newText: 'd', extra: 9 }
          assert.deepStrictEqual(yield* Edit.repair(invalid), invalid)
          assert.strictEqual(yield* Edit.repair('unrelated'), 'unrelated')
        }),
    )
    it.effect(
      'compaction recognizes canonical and generic mixed blocks and retains unrelated serialized fallback',
      () =>
        Effect.gen(function* () {
          const canonical = yield* Schema.encodeEffect(Schema.toCodecJson(Invocation.Result))({
            content: [
              NativePrompt.textPart({ text: 'one' }),
              NativePrompt.filePart({ mediaType: 'image/png', data: new Uint8Array([1, 2]) }),
            ],
          })
          assert.strictEqual(
            serializedTool({
              _tag: '@effect-harness/ToolContent',
              ...(yield* Schema.decodeUnknownEffect(Schema.Record(Schema.String, Schema.Unknown))(
                canonical,
              )),
            }),
            '[Tool result]: one',
          )
          assert.strictEqual(
            serializedTool({
              content: [
                { type: 'text', text: 'a' },
                { type: 'unknown', value: 2 },
                null,
                { type: 'text', text: 7 },
                { type: 'text', text: 'b' },
              ],
            }),
            '[Tool result]: a\nb',
          )
          assert.strictEqual(serializedTool({ content: [{ type: 'image', bytes: [1, 2] }] }), '')
          assert.strictEqual(
            serializedTool({ native: { arbitrary: [true, null] } }),
            '[Tool result]: {"native":{"arbitrary":[true,null]}}',
          )
          assert.strictEqual(serializedTool('literal'), '[Tool result]: literal')
        }),
    )
    it('schema equivalence preserves JSON key order independence and native opaque parameters without encoding', () => {
      assert.strictEqual(
        Json.equals({ nested: { a: 1, b: [2, 3] } }, { nested: { b: [2, 3], a: 1 } }),
        true,
      )
      let encoded = 0
      const params = {
        toJSON: () => {
          encoded++
          throw new Error('must remain opaque')
        },
        payload: new Uint8Array([1, 2]),
      }
      const before = NativePrompt.assistantMessage({
        content: [
          NativePrompt.toolCallPart({ id: 'c', name: 'native', params, providerExecuted: true }),
        ],
        options: { native: { b: 2, a: 1 } },
      })
      const after = NativePrompt.assistantMessage({
        content: [
          NativePrompt.toolCallPart({ id: 'c', name: 'native', params, providerExecuted: true }),
        ],
        options: { native: { a: 1, b: 2 } },
      })
      assert.deepStrictEqual(Response.delta(before, after), [])
      assert.strictEqual(encoded, 0)
      assert.strictEqual(
        after.content[0]?.type === 'tool-call' ? after.content[0].params : undefined,
        params,
      )
      const changed = NativePrompt.assistantMessage({
        content: [
          NativePrompt.toolCallPart({
            id: 'c',
            name: 'native',
            params: new Date(0),
            providerExecuted: true,
          }),
        ],
      })
      assert.strictEqual(Response.delta(undefined, changed)[0]?.value, changed)
    })
  })
})
