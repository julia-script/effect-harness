import * as Option from 'effect/Option'
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Entry from 'effect-harness/durable/Entry'
import * as Session from 'effect-harness/durable/Session'
import * as Memory from 'effect-harness/durable/storage/Memory'
import * as ToolExecutor from 'effect-harness/durable/workflow/ToolExecutor'
import * as GenerationExecutor from 'effect-harness/durable/workflow/GenerationExecutor'
import * as Inbox from 'effect-harness/durable/Inbox'
import * as Document from 'effect-harness/durable/Document'

const sessionLayer = Session.layer.pipe(Layer.provideMerge(Memory.layer))
const user = Prompt.userMessage({ content: [Prompt.textPart({ text: 'stored' })] })
const encodeUser = Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))

describe('Entry', () => {
  it.effect('decodes stored native role content and actual durable tool/partial writers', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const root = yield* session.root()
      const storedUser = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          return yield* tx.appendEntry(root.id, {
            kind: Entry.UserEntry.kind,
            model: [yield* encodeUser(user)],
            data: { timestamp: 3 },
          })
        }),
      )
      assert.isTrue(Entry.UserEntry.is(storedUser))
      const decodedUser = yield* Entry.UserEntry.decode(
        (yield* session.entry(storedUser.id).pipe(Effect.map(Option.getOrUndefined)))?.entry,
      )
      assert.strictEqual(decodedUser.model[0].role, 'user')
      assert.strictEqual(decodedUser.model[0].content[0]?.type, 'text')
      const assistant = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.AssistantMessage))(
            Prompt.assistantMessage({ content: [Prompt.textPart({ text: 'partial' })] }),
          )
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          live.generation = { attempt: 1, message: Document.copyUnsafe(encoded) }
          yield* GenerationExecutor.convertPartial(tx, live, root.id)
        }),
      )
      assert.strictEqual(assistant, undefined)
      const entries = (yield* session.scanEntries({ conversationId: root.id }, 10)).items
      const partial = entries.find(Entry.AssistantEntry.is)
      assert.ok(partial)
      const decodedPartial = yield* Entry.AssistantEntry.decode(partial)
      assert.strictEqual(decodedPartial.data.harness.status, 'aborted')
      assert.strictEqual(decodedPartial.model[0].role, 'assistant')
      const tool = yield* session.transaction((tx) =>
        ToolExecutor.appendResult(
          tx,
          {
            conversationId: root.id,
            assistantId: partial.id,
            callId: 'call-1',
            name: 'custom',
          },
          {
            outcome: 'unavailable',
            result: {
              content: [Prompt.textPart({ text: 'unavailable' })],
              diagnostics: [{ kind: 'tool_unavailable', message: 'not offered' }],
              isError: true,
            },
          },
        ),
      )
      const decodedTool = yield* Entry.ToolResultEntry.decode(
        (yield* session.entry(tool.id).pipe(Effect.map(Option.getOrUndefined)))?.entry,
      )
      assert.strictEqual(decodedTool.data.execution.outcome, 'unavailable')
      assert.strictEqual(
        decodedTool.data.execution.result.diagnostics?.[0]?.kind,
        'tool_unavailable',
      )
      assert.strictEqual(decodedTool.model[0].content[0]?.type, 'tool-result')
    }).pipe(Effect.provide(sessionLayer)),
  )

  it.effect('validates system metadata, reset heads and native compaction reason data', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const root = yield* session.root()
      const entries = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const system = yield* tx.appendEntry(root.id, {
            kind: Entry.SystemEntry.kind,
            data: {
              harness: {
                system: { sections: { policy: 'new', removed: null }, toolsRemoved: ['old'] },
              },
            },
          })
          const reset = yield* tx.appendEntry(root.id, {
            kind: Entry.ResetEntry.kind,
            head: 'self',
          })
          const compact = yield* tx.appendEntry(root.id, {
            kind: Entry.CompactionEntry.kind,
            head: reset.id,
            model: [yield* encodeUser(user)],
            data: { reason: 'manual' },
          })
          return { system, reset, compact }
        }),
      )
      assert.strictEqual(
        (yield* Entry.SystemEntry.decode(entries.system)).data.harness.system.sections?.policy,
        'new',
      )
      assert.strictEqual((yield* Entry.ResetEntry.decode(entries.reset)).head, entries.reset.id)
      const invalidHead = yield* Entry.ResetEntry.decode({
        ...entries.reset,
        head: entries.system.id,
      }).pipe(Effect.flip)
      assert.strictEqual(invalidHead._tag, 'SchemaError')
      assert.include(invalidHead.message, 'filter')
      assert.strictEqual(
        (yield* Entry.CompactionEntry.decode(entries.compact)).data.reason,
        'manual',
      )
      assert.isFalse(Entry.ToolResultEntry.is(entries.compact))
      assert.isFalse(Entry.UserEntry.is(undefined))
      const invalidKind = yield* Entry.UserEntry.decode(entries.system).pipe(Effect.flip)
      assert.strictEqual(invalidKind._tag, 'SchemaError')
      assert.include(invalidKind.message, 'harness.user')
    }).pipe(Effect.provide(sessionLayer)),
  )

  it.effect('kind guards never promise unvalidated JSON metadata or native messages', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const root = yield* session.root()
      const malformed = yield* session.transaction((tx) =>
        tx.appendEntry(root.id, {
          kind: Entry.ToolResultEntry.kind,
          data: { execution: false },
          model: [false],
        }),
      )
      assert.isTrue(Entry.ToolResultEntry.is(malformed))
      const invalidTool = yield* Entry.ToolResultEntry.decode(malformed).pipe(Effect.flip)
      assert.strictEqual(invalidTool._tag, 'SchemaError')
      assert.strictEqual(invalidTool.message, 'Expected ToolMessage\n  at ["model"][0]')
      const invalidCompaction = yield* Entry.CompactionEntry.decode({
        id: malformed.id,
        conversationId: malformed.conversationId,
        kind: Entry.CompactionEntry.kind,
        head: malformed.id,
        model: [yield* encodeUser(user)],
        data: { reason: 'bogus' },
      }).pipe(Effect.flip)
      assert.strictEqual(invalidCompaction._tag, 'SchemaError')
      assert.strictEqual(
        invalidCompaction.message,
        'Expected "manual" | "threshold" | "overflow"\n  at ["data"]["reason"]',
      )
      if (Entry.ToolResultEntry.is(malformed)) {
        const raw: Schema.Json | undefined = malformed.data
        assert.deepStrictEqual(raw, { execution: false })
      }
    }).pipe(Effect.provide(sessionLayer)),
  )
})
