import { assert, it } from '@effect/vitest'
import { NodeServices } from '@effect/platform-node'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as CompactionTask from 'effect-harness/CompactionTask'
import { CompactionRequest } from 'effect-harness/CompactionRequest'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import * as Model from 'effect-harness/Model'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Settings from 'effect-harness/Settings'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'
import * as ToolResult from 'effect-harness/ToolResult'
import * as ToolResultRecord from 'effect-harness/ToolResultRecord'
import * as Storage from 'effect-harness/Storage'
import * as NativePrompt from './fixtures/NativePrompt.js'
import * as ScriptedStream from './fixtures/ScriptedStream.js'

const root = Record.ROOT_CONVERSATION_ID
const finish = (reason: 'stop' | 'tool-calls', input = 10): Response.PartEncoded => ({
  type: 'finish',
  reason,
  usage: {
    inputTokens: { total: input, uncached: input, cacheRead: undefined, cacheWrite: undefined },
    outputTokens: { total: 1, text: 1, reasoning: undefined },
  },
  response: undefined,
})
const answer = (text: string, input = 10): Response.PartEncoded[] => [
  { type: 'text', text },
  finish('stop', input),
]

for (const mode of ['manual', 'automatic'] as const) {
  it.live(
    `${mode}: cold image-first compaction preserves trailing captions and native fingerprints`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const directory = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped()
          const disk = Storage.layerJsonl({ filePath: `${directory}/media.jsonl` })
          const settings = yield* Settings.make({
            compaction: {
              enabled: false,
              reserveTokens: 2000,
              keepRecentTokens: 1,
              backgroundTokens: 0,
            },
          })
          const entered = yield* Deferred.make<void>()
          let generations = 0
          let summaries = 0
          let captures = 0
          const summaryPrompts: Prompt.Prompt[] = []
          const toolkit = Toolkit.make(
            Tool.makeResult('screenshot', { success: Schema.Struct({ width: Schema.Finite }) }),
          )
          const handlers = toolkit.toLayer({
            screenshot: () =>
              Effect.sync(() => {
                captures++
                return {
                  structuredOutput: { width: 2000 },
                  details: { panel: 'private screenshot controls' },
                  diagnostics: [
                    { severity: 'warning' as const, message: 'Coordinates need scaling' },
                  ],
                  content: [
                    Prompt.filePart({
                      mediaType: 'image/png',
                      fileName: 'bytes.png',
                      data: new Uint8Array(1024),
                    }),
                    Prompt.filePart({
                      mediaType: 'image/png',
                      fileName: 'url.png',
                      data: new URL('https://example.com/screenshot.png'),
                    }),
                    Prompt.filePart({
                      mediaType: 'image/png',
                      fileName: 'base64.png',
                      data: 'A'.repeat(4096),
                    }),
                    Prompt.textPart({
                      text: 'Click screenshot at x=200, y=100; source scales x=2 y=2.',
                    }),
                  ],
                }
              }),
          })
          const native = yield* ScriptedStream.make({
            generateText: ({ prompt }) =>
              Effect.gen(function* () {
                if (Option.isSome(yield* Effect.serviceOption(CompactionRequest))) {
                  summaries++
                  summaryPrompts.push(prompt)
                  const text = prompt.content
                    .flatMap((message) =>
                      message.role === 'user'
                        ? message.content.flatMap((part) =>
                            part.type === 'text' ? [part.text] : [],
                          )
                        : [],
                    )
                    .join('\n')
                  assert.include(text, 'Click screenshot at x=200, y=100; source scales x=2 y=2.')
                  assert.notInclude(text, 'AAAA')
                  assert.notInclude(text, 'https://example.com/screenshot.png')
                  assert.strictEqual(text.split('[warning] Coordinates need scaling').length - 1, 1)
                  assert.notInclude(text, 'private screenshot controls')
                  assert.notInclude(text, '"width"')
                  if (summaries === 1) {
                    yield* Deferred.succeed(entered, undefined)
                    return yield* Effect.never
                  }
                  return answer('Screenshot coordinates remembered.')
                }
                generations++
                if (generations === 1) {
                  NativePrompt.assertFiles(prompt)
                  return [
                    {
                      type: 'tool-call',
                      id: 'capture-1',
                      name: 'screenshot',
                      params: {},
                      providerExecuted: false,
                    },
                    finish('tool-calls'),
                  ]
                }
                if (generations === 2) {
                  const result = prompt.content
                    .flatMap((message) => (message.role === 'tool' ? message.content : []))
                    .find((part) => part.type === 'tool-result')
                  const envelope = yield* Schema.decodeUnknownEffect(
                    Schema.toCodecJson(ToolResult.Envelope),
                  )(result?.result).pipe(Effect.orDie)
                  assert.instanceOf(
                    envelope.content[0]?.type === 'file' ? envelope.content[0].data : undefined,
                    Uint8Array,
                  )
                  assert.instanceOf(
                    envelope.content[1]?.type === 'file' ? envelope.content[1].data : undefined,
                    URL,
                  )
                  const projected = JSON.stringify(result)
                  assert.strictEqual(
                    projected.split('[warning] Coordinates need scaling').length - 1,
                    1,
                  )
                  assert.notInclude(projected, 'private screenshot controls')
                  assert.notInclude(projected, '"width"')
                  return answer('Capture complete.', 9000)
                }
                assert.include(JSON.stringify(prompt), 'Screenshot coordinates remembered.')
                return answer('Continued after blocking compaction.')
              }),
          })
          const capability = yield* Model.makeCompaction({
            binding: { id: 'test/media-summary', version: 1 },
            configure: (context) => Effect.succeed(context),
          })
          const descriptor = Model.make({
            definition: {
              ref: { provider: 'fixture', modelId: 'vision' },
              contextWindow: 10000,
              capabilities: {
                images: true,
                tools: true,
                reasoning: false,
                structuredOutput: false,
              },
            },
            measurement: { identity: 'fixture/vision', overheadTokens: 0 },
            languageModel: native,
            options: Schema.Struct({}),
            configure: () => Effect.succeed(Context.empty()),
            compaction: capability,
          })
          const open = Effect.gen(function* () {
            const storage = yield* Layer.build(Layer.fresh(disk))
            return yield* HarnessRuntime.make({ models: [descriptor], tools: toolkit }).pipe(
              Effect.provideContext(storage),
              Effect.provideService(Settings.Settings, settings),
              Effect.provide(handlers),
            )
          })
          const seedScope = yield* Scope.make()
          const seeded = yield* open.pipe(Effect.provideService(Scope.Scope, seedScope))
          yield* seeded.backend.root
          const job = yield* seeded.backend.submit({
            conversationId: root,
            draft: { type: 'input', content: NativePrompt.content('Inspect screenshot') },
          })
          assert.strictEqual(
            (yield* seeded.backend.wait(job.id).pipe(Effect.timeout('5 seconds'))).status,
            'done',
          )
          assert.strictEqual(captures, 1)
          yield* Scope.close(seedScope, Exit.void)
          if (mode === 'automatic') yield* settings.update({ compaction: { enabled: true } })
          const summaryScope = yield* Scope.make()
          const before = yield* open.pipe(Effect.provideService(Scope.Scope, summaryScope))
          const entries = yield* Session.scanEntries(before.session, { conversationId: root }).pipe(
            Stream.runCollect,
          )
          const saved = entries.find((entry) => entry.kind === 'tool.result')
          const restored = yield* ToolResultRecord.read(saved!)
          assert.deepEqual(restored.structuredOutput, { width: 2000 })
          assert.deepEqual(restored.details, { panel: 'private screenshot controls' })
          assert.lengthOf(restored.content, 4)
          assert.deepEqual(restored.diagnostics, [
            { severity: 'warning', message: 'Coordinates need scaling' },
          ])
          const submission =
            mode === 'automatic'
              ? yield* before.backend.submit({
                  conversationId: root,
                  draft: { type: 'input', content: 'Continue' },
                })
              : undefined
          const manual = mode === 'manual' ? yield* before.backend.compact!(root) : undefined
          const summarizing = Deferred.await(entered)
          yield* (
            submission === undefined
              ? summarizing
              : Effect.raceFirst(
                  summarizing,
                  before.backend
                    .wait(submission.id)
                    .pipe(
                      Effect.andThen(() =>
                        Effect.die('Automatic generation settled without blocking compaction'),
                      ),
                    ),
                )
          ).pipe(Effect.timeout('5 seconds'))
          const tasks = yield* Session.scanTasks(before.session, {
            kind: CompactionTask.kind,
          }).pipe(Stream.runCollect)
          assert.lengthOf(tasks, 1)
          const task = tasks[0]!
          const input = yield* Schema.decodeUnknownEffect(CompactionTask.InputSchema)(task.input)
          assert.strictEqual(input.reason, mode === 'automatic' ? 'threshold' : 'manual')
          assert.strictEqual(input.blocking, mode === 'automatic')
          const checkpoint = yield* Schema.decodeUnknownEffect(
            Schema.toCodecJson(CompactionTask.CheckpointSchema),
          )(task.state.checkpoint)
          if (checkpoint.phase !== 'summarize') return assert.fail('Expected pinned summary')
          assert.lengthOf(checkpoint.envelope.omittedMedia, 6)
          assert.deepEqual(
            checkpoint.envelope.omittedMedia
              .filter((part) => part.callId === 'capture-1')
              .map((part) => [part.name, part.isFailure, part.contentPart]),
            [
              ['screenshot', false, 0],
              ['screenshot', false, 1],
              ['screenshot', false, 2],
            ],
          )
          NativePrompt.assertFiles(
            Prompt.fromMessages(
              checkpoint.envelope.fingerprint.selected.flatMap((item) => item.contribution),
            ),
          )
          yield* Scope.close(summaryScope, Exit.void)
          const after = yield* open
          if (submission !== undefined)
            assert.strictEqual(
              (yield* after.backend.wait(submission.id).pipe(Effect.timeout('5 seconds'))).status,
              'done',
            )
          if (manual !== undefined) {
            const outcome = yield* after.backend.waitCompaction!(manual).pipe(
              Effect.timeout('5 seconds'),
            )
            if (outcome.status !== 'completed' || outcome.result._tag !== 'Queued')
              return assert.fail(JSON.stringify(outcome))
            assert.strictEqual(
              (yield* after.backend.wait(outcome.result.submissionId)).status,
              'done',
            )
          }
          assert.strictEqual(summaries, 2)
          assert.deepEqual(summaryPrompts[1], summaryPrompts[0])
          assert.strictEqual(generations, mode === 'automatic' ? 3 : 2)
          assert.strictEqual(captures, 1)
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  )
}
