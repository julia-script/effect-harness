import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Wire from 'effect-harness/PromptWire'
import * as Submission from 'effect-harness/Submission'
import * as ToolResult from 'effect-harness/ToolResult'
import * as Prepared from 'effect-harness/PreparedRequest'
import * as Settings from 'effect-harness/Settings'
import * as Record from 'effect-harness/Record'
import * as RunState from '../src/internal/RunState.js'
import * as Value from '../src/internal/DocumentValue.js'

const bytes = new Uint8Array([0, 1, 127, 128, 254, 255])
const parts = [
  Prompt.textPart({ text: 'before' }),
  Prompt.filePart({
    mediaType: 'image/png',
    data: bytes,
    fileName: 'capture.png',
    options: { openai: { detail: 'high' }, anthropic: { cacheControl: { type: 'ephemeral' } } },
  }),
  Prompt.filePart({
    mediaType: 'image/webp',
    data: new URL('https://example.test/image?x=%20#fragment'),
  }),
  Prompt.filePart({ mediaType: 'image/jpeg', data: 'AAH/gA==' }),
  Prompt.filePart({ mediaType: 'image/png', data: 'data:image/png;base64,AAH/gA==' }),
  Prompt.textPart({ text: 'after' }),
]

it.effect(
  'preserves native file types and arbitrary marker-shaped JSON through every prompt copy boundary',
  () =>
    Effect.gen(function* () {
      const marker = {
        _tag: '@effect-harness/Bytes',
        data: 'not bytes',
        nested: { _tag: '@effect-harness/URL', href: 'not a url' },
      }
      const prompt = Prompt.fromMessages([
        Prompt.userMessage({ content: parts }),
        Prompt.assistantMessage({
          content: [
            Prompt.toolCallPart({
              id: 'call',
              name: 'test',
              providerExecuted: false,
              params: marker,
            }),
          ],
        }),
        Prompt.toolMessage({
          content: [
            Prompt.toolResultPart({
              id: 'call',
              name: 'test',
              providerExecuted: false,
              result: marker,
              isFailure: true,
            }),
          ],
        }),
      ])
      const restored = yield* Value.copy(Schema.toCodecJson(Wire.Prompt), prompt)
      assert.isTrue(Prompt.isPrompt(restored))
      assert.deepEqual(restored.content, prompt.content)
      const file = restored.content[0]?.content[1]
      if (typeof file !== 'object' || file.type !== 'file' || !(file.data instanceof Uint8Array))
        throw new Error('Lost bytes')
      assert.notStrictEqual(file.data, bytes)
      file.data[0] = 42
      assert.strictEqual(bytes[0], 0)
      const draft = yield* Value.copy(Schema.toCodecJson(Submission.InputDraftSchema), {
        type: 'input',
        content: parts,
      })
      assert.deepEqual(draft.content, parts)
      const tool = yield* Value.copy(Schema.toCodecJson(ToolResult.ContentSchema), parts)
      assert.deepEqual(tool, parts)
    }),
)

it.effect(
  'does not decode wire controls outside typed file data and rejects forged file-data objects',
  () =>
    Effect.gen(function* () {
      const codec = Schema.toCodecJson(Wire.Message)
      const invalid = {
        role: 'user',
        content: [
          {
            type: 'file',
            mediaType: 'image/png',
            data: { _tag: '@effect-harness/URL', href: 'invalid' },
          },
        ],
      }
      assert.strictEqual(
        (yield* Schema.decodeEffect(codec)(invalid).pipe(Effect.result))._tag,
        'Failure',
      )
      const input = {
        role: 'assistant',
        content: [
          {
            type: 'tool-call',
            id: 'x',
            name: 'x',
            params: { _tag: '@effect-harness/URL', href: 'invalid' },
          },
        ],
      }
      const restored = yield* Schema.decodeEffect(codec)(input)
      if (restored.role !== 'assistant' || restored.content[0]?.type !== 'tool-call')
        throw new Error('Missing call')
      assert.deepEqual(restored.content[0].params, input.content[0]?.params)
    }),
)

it.effect(
  'restores streaming checkpoints without interpreting already hydrated native file data twice',
  () =>
    Effect.gen(function* () {
      const prompt = Prompt.fromMessages([Prompt.userMessage({ content: parts })])
      const preparation = yield* Schema.decodeUnknownEffect(Schema.toType(Prepared.SnapshotSchema))(
        {
          requestId: 'streaming-images',
          ordinal: 0,
          attempt: 1,
          prompt,
          agent: {},
          settings: Settings.defaults,
          affinity: 'test',
          model: { _tag: 'native', binding: { id: 'test', version: 1 } },
          options: {},
          configuration: {},
          configurationBinding: { id: 'test', version: 1 },
          registryGeneration: 0,
          requiredBindings: [],
          tools: [],
          allTools: [],
          translation: {
            prompt,
            positioning: { sections: 'leading', tools: 'latest' },
            tools: [],
            frames: [],
            toolPositions: [],
          },
        },
      )
      const checkpoint = yield* Schema.decodeUnknownEffect(Schema.toType(RunState.StoredSchema))({
        _tag: 'streaming',
        phase: 'streaming',
        submissionId: Record.SubmissionId.make(2),
        agent: {},
        attempt: preparation.attempt,
        retries: 0,
        revision: 1,
        partial: { message: prompt, parameters: [] },
        preparation: {
          ownerTaskId: Record.TaskId.make(3),
          requestId: preparation.requestId,
          ordinal: 0,
        },
      })
      const stored = yield* Value.copy(Schema.toCodecJson(RunState.StoredSchema), checkpoint)
      const restored = yield* RunState.hydrate(stored, () => Effect.succeed(preparation))
      assert.strictEqual(restored._tag, 'streaming')
      if (restored._tag !== 'streaming') return
      assert.deepEqual(restored.partial?.message.content, prompt.content)
      assert.deepEqual(restored.preparation.prompt.content, prompt.content)
    }),
)
