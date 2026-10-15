import { assert, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Option from 'effect/Option'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as Wire from '../src/PromptWire.js'
import * as Media from '../src/StorageMedia.js'
import * as Record from '../src/Record.js'
import * as ToolResult from '../src/ToolResult.js'
import * as Task from '../src/Task.js'
import * as Supervisor from '../src/internal/SubagentTask.js'
import * as Memory from '../src/internal/Memory.js'
import * as Kernel from '../src/internal/Kernel.js'
import * as Compaction from '../src/Compaction.js'
import * as Plan from '../src/internal/CompactionPlan.js'
import * as Protocol from '../src/internal/ContextProtocol.js'
import * as PromptUsage from '../src/internal/PromptUsage.js'
import * as LiveState from '../src/LiveState.js'
import * as CompactionTask from '../src/CompactionTask.js'
import * as RunControl from '../src/RunControl.js'

const root = Record.ROOT_CONVERSATION_ID
for (const shape of ['application', 'null-data', 'unknown-part'] as const)
  it.effect(`opaque ${shape} tool JSON survives commit and scan without media ownership`, () =>
    Effect.gen(function* () {
      const result = {
        _tag: shape === 'application' ? 'application' : '@effect-harness/ToolContent',
        content: [
          { type: 'file', mediaType: 'image/png', data: shape === 'unknown-part' ? 'AQID' : null },
          ...(shape === 'unknown-part' ? [{ type: 'custom', application: 'opaque' }] : []),
        ],
      }
      assert.isTrue(
        Option.isNone(Schema.decodeUnknownOption(Schema.toCodecJson(ToolResult.Envelope))(result)),
      )
      const model = yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Wire.Message)))([
        Prompt.toolMessage({
          content: [
            Prompt.toolResultPart({
              id: 'call',
              name: 'tool',
              result,
              isFailure: false,
              providerExecuted: false,
            }),
          ],
        }),
      ])
      const access = yield* Memory.makeAccess
      const storage = yield* Kernel.make(access)
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        {
          _tag: 'entry',
          value: { id: Record.EntryId.make(2), conversationId: root, kind: 'application', model },
        },
      ])
      const state = yield* access.snapshot
      assert.strictEqual(state.objects.size, 0)
      assert.strictEqual(state.rows.get(2)!.value.media.length, 0)
      const rows = yield* storage.scanEntries({ conversationId: root }).pipe(Stream.runCollect)
      assert.deepEqual(rows[0]!.model, model)
    }),
  )

it.effect(
  'runtime-owned image channels share objects and restore every fingerprint and draft copy',
  () =>
    Effect.gen(function* () {
      const file = Prompt.filePart({
        mediaType: 'image/png',
        data: new Uint8Array(65536).fill(123),
        fileName: 'shared.png',
      })
      const prompt = Prompt.fromMessages([Prompt.userMessage({ content: [file] })])
      const model = yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Wire.Message)))(
        prompt.content,
      )
      const first = { id: Record.EntryId.make(2), conversationId: root, kind: 'input', model }
      const entries = [
        first,
        {
          id: Record.EntryId.make(3),
          conversationId: root,
          kind: 'input',
          model: [{ role: 'user', content: 'keep' }],
        },
      ]
      const protocol: Protocol.Protocol = {
        version: 1,
        origin: { conversationId: root },
        legacyMode: 'honorAll',
      }
      const view = yield* Protocol.project(root, entries, protocol)
      const selection = yield* Compaction.select(
        view,
        { enabled: false, reserveTokens: 20, keepRecentTokens: 0, backgroundTokens: 0 },
        { reason: 'manual' },
      )
      if (selection._tag !== 'Selected') return assert.fail('Expected a compaction selection')
      const fingerprint = yield* Plan.capture(selection, { entries, view, protocol })
      const input = yield* Task.encode(Supervisor.InputSchema, {
        originTaskId: Record.TaskId.make(10),
        originConversationId: root,
        key: 'child',
        mode: 'background',
        draft: { type: 'input', content: [file] },
        inherited: {},
        restartPolicy: 'resume',
      })
      const writeInput = yield* Task.encode(Supervisor.InputSchema, {
        originTaskId: Record.TaskId.make(10),
        originConversationId: root,
        key: 'write',
        mode: 'background',
        draft: { type: 'write', entry: { kind: 'input', model } },
        inherited: {},
        restartPolicy: 'resume',
      })
      const partial = yield* Schema.encodeEffect(Schema.toCodecJson(LiveState.PartialSchema))({
        message: prompt,
        parameters: [],
      })
      const supplied = yield* Task.encode(CompactionTask.CheckpointSchema, {
        phase: 'supplied',
        fingerprint,
        summary: 'saved',
      })
      const encodedFingerprint = yield* Schema.encodeEffect(
        Schema.toCodecJson(Plan.FingerprintSchema),
      )(fingerprint)
      const encodedPrompt = yield* Schema.encodeEffect(Schema.toCodecJson(Wire.Prompt))(prompt)
      const inbox = yield* Schema.encodeEffect(Schema.toCodecJson(RunControl.InboxSchema))({
        items: [
          {
            submissionId: Record.SubmissionId.make(20),
            mode: 'write',
            payload: { kind: 'compaction' },
            compaction: fingerprint,
          },
        ],
      })
      const usage = yield* Schema.encodeEffect(
        Schema.toCodecJson(PromptUsage.document.definition.schema),
      )({ baseline: { identity: 'measured', prompt, tools: [], inputTokens: 9000 } })
      const cases: ReadonlyArray<{
        label: string
        body: Schema.Json
        context: Media.Context
        occurrences: number
      }> = [
        {
          label: 'subagent input',
          body: { kind: 'harness.subagent', input },
          context: { category: 'task' },
          occurrences: 1,
        },
        {
          label: 'subagent write',
          body: { kind: 'harness.subagent', input: writeInput },
          context: { category: 'task' },
          occurrences: 1,
        },
        {
          label: 'interrupted assistant',
          body: { entry: { kind: 'assistant.interrupted', data: partial } },
          context: { category: 'entry' },
          occurrences: 1,
        },
        {
          label: 'supplied compaction',
          body: { kind: 'harness.compaction', state: { checkpoint: supplied } },
          context: { category: 'task' },
          occurrences: 2,
        },
        {
          label: 'prepared compaction',
          body: {
            kind: 'harness.compaction',
            state: {
              checkpoint: {
                phase: 'summarize',
                envelope: {
                  fingerprint: encodedFingerprint,
                  preparation: { prompt: encodedPrompt, translation: { prompt: encodedPrompt } },
                },
              },
            },
          },
          context: { category: 'task' },
          occurrences: 4,
        },
        {
          label: 'inbox fingerprint',
          body: { _tag: 'base', version: 1, value: inbox },
          context: { category: 'document', kind: 'harness.inbox' },
          occurrences: 2,
        },
        {
          label: 'usage baseline',
          body: { _tag: 'base', version: 1, value: usage },
          context: { category: 'document', kind: 'harness.promptUsage' },
          occurrences: 1,
        },
      ]
      for (const item of cases) {
        const packed = yield* Media.pack(item.body, item.context)
        assert.strictEqual(packed.objects.length, 1, item.label)
        assert.strictEqual(packed.packed.media.length, item.occurrences, item.label)
        assert.isBelow(JSON.stringify(packed.packed).length, 8192, item.label)
        assert.deepEqual(
          yield* Media.hydrate(packed.packed, item.context, () =>
            Effect.succeed(packed.objects[0]!.bytes),
          ),
          item.body,
          item.label,
        )
        // Changing the owning protocol discriminant keeps the same nested JSON opaque.
        const opaque =
          item.context.category === 'document'
            ? { ...item.context, kind: 'application' }
            : item.context
        let body = item.body
        if (item.context.category === 'task')
          body = Object.assign({}, item.body, { kind: 'application' })
        else if (item.context.category === 'entry')
          body = { entry: { kind: 'application', data: partial } }
        const control = yield* Media.pack(body, opaque)
        assert.strictEqual(control.objects.length, 0, item.label)
      }
      const access = yield* Memory.makeAccess
      const storage = yield* Kernel.make(access)
      yield* storage.commit([
        { _tag: 'conversation', value: { id: root } },
        { _tag: 'entry', value: first },
        {
          _tag: 'entry',
          value: {
            id: Record.EntryId.make(4),
            conversationId: root,
            kind: 'assistant.interrupted',
            data: partial,
          },
        },
        {
          _tag: 'task',
          value: {
            id: Record.TaskId.make(10),
            conversationId: root,
            kind: 'harness.subagent',
            version: 1,
            input,
            background: true,
            abortRequested: false,
            state: { status: 'pending', checkpoint: { phase: 'create' } },
          },
        },
        {
          _tag: 'task',
          value: {
            id: Record.TaskId.make(11),
            conversationId: root,
            kind: 'harness.compaction',
            version: 1,
            input: {},
            background: false,
            abortRequested: false,
            state: { status: 'pending', checkpoint: supplied },
          },
        },
      ])
      const state = yield* access.snapshot
      assert.strictEqual(state.objects.size, 1)
      for (const id of [2, 4, 10, 11]) assert.isAbove(state.rows.get(id)!.value.media.length, 0)
      assert.deepEqual(Option.getOrThrow(yield* storage.task(Record.TaskId.make(10))).input, input)
      assert.deepEqual(
        Option.getOrThrow(yield* storage.task(Record.TaskId.make(11))).state.checkpoint,
        supplied,
      )
    }),
)

it.effect('packing separates shared JSON identities before replacing typed media leaves', () =>
  Effect.gen(function* () {
    const file = { type: 'file', mediaType: 'image/png', data: 'AQID' }
    const value = {
      entry: {
        kind: 'user',
        model: [{ role: 'user', content: [file, file] }],
        data: { opaque: file },
      },
    }
    const packed = yield* Media.pack(value, { category: 'entry' })
    assert.strictEqual(packed.packed.media.length, 2)
    assert.strictEqual(packed.objects.length, 1)
    const physical = yield* Schema.decodeUnknownEffect(Media.PackedBodySchema)(
      JSON.parse(JSON.stringify(packed.packed)),
    )
    const restored = yield* Media.hydrate(physical, { category: 'entry' }, () =>
      Effect.succeed(packed.objects[0]!.bytes),
    )
    assert.deepEqual(restored, value)
    assert.strictEqual(file.data, 'AQID')
  }),
)
