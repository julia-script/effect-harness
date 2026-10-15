import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as TestClock from 'effect/testing/TestClock'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as StorageFormat from 'effect-harness/StorageFormat'
import * as Images from 'effect-harness/ImagePreparation'
import * as Record from 'effect-harness/Record'
import { ToolExecution } from 'effect-harness/ToolExecution'
import * as ToolImage from 'effect-harness/ToolImage'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Policy from 'effect-harness/coding/ImageReadPolicy'
import * as NodeImages from '../src/images-node/index.js'
import * as Screenshot from '../../../apps/docs/src/examples/ToolImages.js'
import { fixture, limits, rgba } from './fixtures/images-native/Conformance.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'
import * as RequestRecord from '../src/internal/RequestRecord.js'
import * as ResultEntry from '../src/internal/ToolResultEntry.js'

const vision: Policy.Policy = { vision: true, limits }
const execution = (imageReadPolicy?: Policy.Policy) =>
  Layer.succeed(ToolExecution, {
    conversationId: Schema.decodeSync(Record.ConversationId)(1),
    taskId: Schema.decodeSync(Record.TaskId)(1),
    callId: 'screenshot',
    imageReadPolicy,
    snapshot: () => Effect.die('Unexpected snapshot'),
    watch: () => Stream.die('Unexpected watch'),
    commit: () => Effect.die('Unexpected commit'),
    output: () => Effect.die('Unexpected output'),
    details: () => Effect.die('Unexpected details'),
    diagnostic: () => Effect.die('Unexpected diagnostic'),
  })
const bytesOf = (output: ToolResult.Output<ToolImage.Metadata>) => {
  const part = output.content.find((part) => part.type === 'file')
  assert.isDefined(part)
  assert.instanceOf(part?.data, Uint8Array)
  return part?.data instanceof Uint8Array ? part.data : new Uint8Array()
}
const prepare = (name = 'opaque.png', mime = 'image/png') => ToolImage.prepare(fixture(name), mime)

it.effect(
  'uses the pinned model limits over the fallback and exposes real resized coordinates',
  () =>
    Effect.gen(function* () {
      const result = yield* prepare('orientation-6.png')
      assert.deepStrictEqual(result.structuredOutput.original, { width: 3, height: 2 })
      assert.deepStrictEqual(result.structuredOutput.oriented, { width: 2, height: 3 })
      assert.deepStrictEqual(result.structuredOutput.resized, {
        from: { width: 2, height: 3 },
        to: { width: 1, height: 2 },
      })
      assert.strictEqual(result.structuredOutput.orientation, 6)
      const pixels = yield* rgba(bytesOf(result))
      assert.strictEqual(pixels.info.width, 1)
      assert.strictEqual(pixels.info.height, 2)
      const note = result.content.find((part) => part.type === 'text')?.text ?? ''
      assert.include(note, 'x by 2 and y by 1.5')
      assert.include(note, 'upright')
      assert.notProperty(result.structuredOutput, 'bytes')
      const codec = Schema.toCodecJson(ToolResult.strictOutputSchema(ToolImage.Metadata))
      const roundTrip = yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(result))
      assert.deepStrictEqual(bytesOf(roundTrip), bytesOf(result))
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          NodeImages.layer,
          Policy.layer({ vision: false }),
          execution({ vision: true, limits: { ...limits, maxWidth: 1 } }),
        ),
      ),
    ),
)

it.effect(
  'resolves direct-call fallback per invocation and uses the configured conversion service',
  () =>
    Effect.gen(function* () {
      let policy: Policy.Policy = vision
      let resolutions = 0
      const resolver = Layer.succeed(Policy.ImageReadPolicy, {
        current: Effect.sync(() => {
          resolutions++
          return policy
        }),
      })
      yield* Effect.gen(function* () {
        const output = yield* prepare('opaque.bmp', 'image/bmp')
        assert.strictEqual(output.structuredOutput.convertedFrom, 'image/bmp')
        assert.strictEqual(output.structuredOutput.mimeType, 'image/png')
        assert.deepStrictEqual((yield* rgba(bytesOf(output))).info.width, 3)
        policy = { vision: false }
        assert.strictEqual((yield* prepare().pipe(Effect.flip))._tag, 'ImageModelUnsupported')
        assert.strictEqual(resolutions, 2)
      }).pipe(Effect.provide(resolver))
    }).pipe(Effect.provide(Layer.merge(NodeImages.layer, execution()))),
)

it.effect('pinned unsupported vision fails before fallback resolution or codec execution', () =>
  Effect.gen(function* () {
    const error = yield* prepare().pipe(Effect.flip)
    assert.strictEqual(error._tag, 'ImageModelUnsupported')
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(Images.ImagePreparation, { prepare: () => Effect.die('Codec must not run') }),
        Layer.succeed(Policy.ImageReadPolicy, { current: Effect.die('Fallback must not run') }),
        execution({ vision: false }),
      ),
    ),
  ),
)

it.effect(
  'checks padded base64, not binary bytes, at the replacement-service output boundary',
  () =>
    Effect.gen(function* () {
      const source = fixture('opaque.png')
      const prepared = yield* Images.prepare(source, 'image/png', limits).pipe(
        Effect.provide(NodeImages.layer),
      )
      const encodedSize = Buffer.from(source).toString('base64').length
      assert.isAbove(encodedSize, source.byteLength)
      const replacement = Layer.succeed(Images.ImagePreparation, {
        prepare: () => Effect.succeed(prepared),
      })
      const attempt = (maxBytes: number) =>
        prepare().pipe(
          Effect.provide(
            Layer.mergeAll(
              replacement,
              Policy.layer(vision),
              execution({ vision: true, limits: { ...limits, maxBytes } }),
            ),
          ),
        )
      const output = yield* attempt(encodedSize)
      assert.strictEqual(output.structuredOutput.base64Bytes, encodedSize)
      assert.strictEqual(output.structuredOutput.outputBytes, source.byteLength)
      for (const maxBytes of [source.byteLength, encodedSize - 1]) {
        const error = yield* attempt(maxBytes).pipe(Effect.flip)
        assert.instanceOf(error, ToolImage.InvalidImageOutput)
        if (error._tag === 'InvalidImageOutput') assert.strictEqual(error.reason, 'outputBudget')
      }
    }),
)

it.effect('preserves semantic preparation failures without returning successful placeholders', () =>
  Effect.gen(function* () {
    const malformed = yield* ToolImage.prepare(new Uint8Array([1, 2, 3]), 'image/png').pipe(
      Effect.flip,
    )
    assert.strictEqual(malformed._tag, 'ImageDecodeFailed')
    const tiny = yield* prepare().pipe(
      Effect.provide(execution({ vision: true, limits: { ...limits, maxBytes: 1 } })),
      Effect.flip,
    )
    assert.strictEqual(tiny._tag, 'ImageCannotFit')
    const invalid = yield* prepare().pipe(
      Effect.provide(execution({ vision: true, limits: { ...limits, maxWidth: 0 } })),
      Effect.flip,
    )
    assert.strictEqual(invalid._tag, 'InvalidImageLimits')
  }).pipe(Effect.provide(Layer.mergeAll(NodeImages.layer, Policy.layer(vision), execution()))),
)

it.effect(
  'the documented custom screenshot tool returns native content and declared failures',
  () =>
    Effect.gen(function* () {
      const toolkit = yield* Screenshot.tools
      const success = yield* toolkit.invoke({ id: 'capture', name: 'screenshot', arguments: {} })
      assert.isFalse(success.isError)
      const metadata = yield* Schema.decodeUnknownEffect(ToolImage.Metadata)(
        success.structuredOutput,
      )
      assert.deepStrictEqual(metadata.dimensions, { width: 3, height: 2 })
      assert.deepStrictEqual(
        bytesOf({ ...success, structuredOutput: metadata }),
        fixture('opaque.png'),
      )
      const failure = yield* toolkit
        .invoke({ id: 'no-vision', name: 'screenshot', arguments: {} })
        .pipe(Effect.provide(execution({ vision: false })))
      assert.isTrue(failure.isError)
      assert.isFalse(failure.content.some((part) => part.type === 'file'))
      assert.include(
        yield* Schema.decodeUnknownEffect(Schema.String)(failure.structuredOutput),
        'does not support images',
      )
    }).pipe(
      Effect.provide(
        Layer.merge(
          execution(),
          Screenshot.ToolsLive.pipe(
            Layer.provideMerge(
              Layer.succeed(Screenshot.Webview, {
                screenshot: Effect.sync(() => fixture('opaque.png')),
              }),
            ),
          ),
        ),
      ),
    ),
)

it.effect(
  'custom screenshots keep accepted model limits after request eviction and reconfiguration',
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const storage = yield* Storage.Storage
        const format = yield* StorageFormat.StorageFormat
        let requestLoads = 0
        const counted: Storage.Storage['Service'] = {
          ...storage,
          document: (id, at) =>
            storage.document(id, at).pipe(
              Effect.tap((stored) =>
                Effect.sync(() => {
                  if (Option.isSome(stored) && stored.value.record.kind === 'harness.request')
                    requestLoads++
                }),
              ),
            ),
        }
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let calls = 0
        const finish = {
          type: 'finish',
          reason: 'stop',
          usage: {
            inputTokens: { total: 1, uncached: 1, cacheRead: undefined, cacheWrite: undefined },
            outputTokens: { total: 1, text: 1, reasoning: undefined },
          },
          response: undefined,
        } as const
        const native = yield* ScriptedStream.make({
          generateText: () =>
            Effect.sync(() => {
              calls++
              if (calls === 1)
                return [
                  {
                    type: 'tool-call' as const,
                    id: 'capture',
                    name: 'screenshot',
                    params: {},
                    providerExecuted: false,
                  },
                  finish,
                ]
              return [{ type: 'text' as const, text: 'done' }, finish]
            }),
        })
        const model = (modelId: string, maxWidth: number) =>
          Model.make({
            definition: {
              ref: { provider: 'synthetic', modelId },
              capabilities: {
                tools: true,
                images: true,
                reasoning: false,
                structuredOutput: false,
              },
              imageLimits: { ...limits, maxWidth },
            },
            languageModel: native,
            options: Schema.Struct({}),
            configure: () => Effect.succeed(Context.empty()),
          })
        const one = model('one-pixel', 1)
        const two = model('two-pixels', 2)
        const runtime = yield* HarnessRuntime.make({
          tools: Screenshot.tools,
          models: [one, two],
          documentRetentionMs: 100,
        }).pipe(
          Effect.provideService(Storage.Storage, counted),
          Effect.provideService(StorageFormat.StorageFormat, { ...format, storage: counted }),
          Effect.provide(
            Screenshot.handlers.pipe(
              Layer.provideMerge(NodeImages.layer),
              // Only the accepted request grants vision; falling back would fail the tool.
              Layer.provideMerge(Policy.layer({ vision: false })),
              Layer.provideMerge(
                Layer.succeed(Screenshot.Webview, {
                  screenshot: Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.as(fixture('opaque.png')),
                  ),
                }),
              ),
            ),
          ),
        )
        const root = Record.ROOT_CONVERSATION_ID
        yield* runtime.backend.root
        const job = yield* runtime.backend.submit({
          conversationId: root,
          draft: { type: 'input', content: 'synthetic screenshot' },
        })
        yield* Deferred.await(entered)
        const tasks = yield* storage.scanTasks({ conversationId: root }).pipe(Stream.runCollect)
        const requests = (yield* Effect.forEach(tasks, (task) =>
          storage
            .scanDocuments({ scope: { _tag: 'task', taskId: task.id }, kind: 'harness.request' })
            .pipe(Stream.runCollect),
        )).flat()
        assert.lengthOf(requests, 1)
        const read = Session.snapshot(runtime.session, RequestRecord.document, requests[0]!)
        const pinned = yield* read
        yield* runtime.backend.configure({
          conversationId: root,
          change: { model: { ref: two.definition.ref, options: {} } },
        })
        const before = requestLoads
        yield* TestClock.adjust(100)
        assert.deepStrictEqual(yield* read, pinned)
        assert.strictEqual(
          requestLoads,
          before + 1,
          'expired request reloaded from durable storage',
        )
        yield* Deferred.succeed(release, undefined)
        assert.strictEqual((yield* runtime.backend.wait(job.id)).status, 'done')
        const entries = yield* storage.scanEntries({ conversationId: root }).pipe(Stream.runCollect)
        const results = entries.filter((entry) => entry.kind === 'tool.result')
        assert.lengthOf(results, 1)
        assert.isUndefined(results[0]!.model)
        const result = yield* ResultEntry.read(results[0]!)
        assert.isFalse(result.isError)
        const metadata = yield* Schema.decodeUnknownEffect(ToolImage.Metadata)(
          result.structuredOutput,
        )
        assert.strictEqual(metadata.dimensions.width, 1, 'accepted model policy survives expiry')
        const file = result.content.find((part) => part.type === 'file')
        assert.instanceOf(file?.data, Uint8Array)
        if (file?.data instanceof Uint8Array)
          assert.strictEqual((yield* rgba(file.data)).info.width, 1)
        assert.isTrue(Option.isNone(yield* storage.document(requests[0]!.id)))
      }),
    ).pipe(Effect.provide(Storage.layerMemory)),
)
