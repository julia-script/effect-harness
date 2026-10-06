import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Prompt from 'effect/ai/Prompt'
import * as Entry from '../../src/Entry.ts'
import * as Session from '../../src/Session.ts'
import * as Memory from '../../src/storage/Memory.ts'
import * as ToolExecutor from '../../src/workflow/ToolExecutor.ts'
import * as GenerationExecutor from '../../src/workflow/GenerationExecutor.ts'
import * as Inbox from '../../src/Inbox.ts'
import * as Document from '../../src/Document.ts'

const sessionLayer = Session.layer.pipe(Layer.provideMerge(Memory.layer))
const user = Prompt.userMessage({ content: [Prompt.textPart({ text: 'stored' })] })
const encodeUser = Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))

describe('public native built-in entry tokens', () => {
  it.effect('decodes stored native role content and actual durable tool/partial writers', () =>
    Effect.scoped(
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
          (yield* session.entry(storedUser.id))?.entry,
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
          (yield* session.entry(tool.id))?.entry,
        )
        assert.strictEqual(decodedTool.data.execution.outcome, 'unavailable')
        assert.strictEqual(
          decodedTool.data.execution.result.diagnostics?.[0]?.kind,
          'tool_unavailable',
        )
        assert.strictEqual(decodedTool.model[0].content[0]?.type, 'tool-result')
      }).pipe(Effect.provide(sessionLayer)),
    ),
  )

  it.effect('validates system metadata, reset heads and native compaction reason data', () =>
    Effect.scoped(
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
        assert.isTrue(
          Exit.isFailure(
            yield* Entry.ResetEntry.decode({ ...entries.reset, head: entries.system.id }).pipe(
              Effect.exit,
            ),
          ),
        )
        assert.strictEqual(
          (yield* Entry.CompactionEntry.decode(entries.compact)).data.reason,
          'manual',
        )
        assert.isFalse(Entry.ToolResultEntry.is(entries.compact))
        assert.isFalse(Entry.UserEntry.is(undefined))
        assert.isTrue(
          Exit.isFailure(yield* Entry.UserEntry.decode(entries.system).pipe(Effect.exit)),
        )
      }).pipe(Effect.provide(sessionLayer)),
    ),
  )

  it.effect('kind guards never promise unvalidated JSON metadata or native messages', () =>
    Effect.scoped(
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
        assert.isTrue(
          Exit.isFailure(yield* Entry.ToolResultEntry.decode(malformed).pipe(Effect.exit)),
        )
        assert.isTrue(
          Exit.isFailure(
            yield* Entry.CompactionEntry.decode({
              ...malformed,
              kind: Entry.CompactionEntry.kind,
              data: { reason: 'bogus' },
            }).pipe(Effect.exit),
          ),
        )
        if (Entry.ToolResultEntry.is(malformed)) {
          const raw: Schema.Json | undefined = malformed.data
          assert.deepStrictEqual(raw, { execution: false })
          // @ts-expect-error Kind alone cannot establish an execution's metadata shape.
          const execution: { readonly outcome: string } = malformed.data.execution
          void execution
        }
      }).pipe(Effect.provide(sessionLayer)),
    ),
  )
})
