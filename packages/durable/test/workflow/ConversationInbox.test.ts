import * as Option from 'effect/Option'
import * as DateTime from 'effect/DateTime'
import * as Serialization from '../../src/Serialization.ts'
import * as Entry from '../../src/Entry.ts'
import { assert, describe, it } from '@effect/vitest'
import * as BunCrypto from '@effect/platform-bun/BunCrypto'
import * as Agent from '@effect-harness/harness/Agent'
import * as Totals from '@effect-harness/harness/Usage'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Conversation from '../../src/Conversation.ts'
import * as Document from '../../src/Document.ts'
import * as Inbox from '../../src/Inbox.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import * as Usage from '../../src/Usage.ts'
import * as Memory from '../../src/storage/Memory.ts'

const services = Session.layer.pipe(
  Layer.provideMerge(Memory.layer),
  Layer.provide(
    Conversation.layerCreation.pipe(
      Layer.provide(Conversation.layerConfiguration()),
      Layer.provide(BunCrypto.layer),
    ),
  ),
)
const user = (text: string) => Prompt.userMessage({ content: [Prompt.textPart({ text })] })
const encode = Schema.encodeEffect(Schema.toCodecJson(Prompt.UserMessage))

describe('conversation and inbox domain integration', () => {
  it.effect(
    'configuration results remain detached after subsequent transactions and resets exclude malformed cut history',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(Conversation.layer.pipe(Layer.provideMerge(services)))
          const session = Context.get(context, Session.Session)
          const conversation = Context.get(context, Conversation.Conversation)
          const root = yield* session.root()
          yield* conversation.configure(root.id, { model: { provider: 'fake', modelId: 'test' } })
          const result = yield* conversation.configure(root.id, { instructions: 'later' })
          yield* conversation.configure(root.id, { instructions: 'latest' })
          assert.strictEqual(result.model?.modelId, 'test')
          assert.strictEqual(result.instructions, 'later')
          const malformed = yield* session.transaction((tx) =>
            tx.appendEntry(root.id, { kind: 'old', model: [false] }),
          )
          assert.strictEqual(
            (yield* conversation.context(root.id).pipe(Effect.flip)).reason._tag,
            'InvalidState',
          )
          yield* session.transaction((tx) =>
            tx.appendEntry(root.id, { kind: 'reset', head: 'self' }),
          )
          assert.deepStrictEqual((yield* conversation.context(root.id)).messages, [])
          assert.strictEqual(
            (yield* conversation.context(root.id, malformed.id).pipe(Effect.flip)).reason._tag,
            'InvalidState',
          )
          const other = yield* conversation.create()
          const invisible = yield* session.transaction((tx) =>
            tx.appendEntry(other.id, { kind: 'unrelated' }),
          )
          assert.strictEqual(
            (yield* conversation.context(root.id, invisible.id).pipe(Effect.flip)).reason._tag,
            'InvalidArguments',
          )
          const unknown = yield* Schema.decodeEffect(Record.EntryId)(999)
          assert.strictEqual(
            (yield* conversation.context(root.id, unknown).pipe(Effect.flip)).reason._tag,
            'InvalidArguments',
          )
        }),
      ),
  )
  it.effect(
    'initializes lazy root once and creates fresh provider and usage state on historical forks',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = Context.get(yield* Layer.build(services), Session.Session)
          const root = yield* session.root()
          const initial = yield* session
            .snapshot(Conversation.ProviderDoc, { owner: root.id })
            .pipe(Effect.map(Option.getOrUndefined))
          const first = yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
              agent.instructions = 'historical instructions'
              yield* Usage.record(tx, root.id, 'models', 'fake/model', {
                ...Totals.zero(),
                input: 3,
                totalTokens: 3,
              })
              return yield* tx.appendEntry(root.id, { kind: 'marker' })
            }),
          )
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
              agent.instructions = 'current instructions'
              return null
            }),
          )
          const fork = yield* session.transaction((tx) =>
            tx.forkConversation(root.id, first.id, { ownership: { kind: 'ownerless' } }),
          )
          assert.strictEqual(
            (yield* session
              .snapshot(Conversation.AgentDoc, { owner: fork.id })
              .pipe(Effect.map(Option.getOrUndefined)))?.value.instructions,
            'historical instructions',
          )
          assert.deepStrictEqual(
            (yield* session
              .snapshot(Usage.UsageDoc, { owner: fork.id })
              .pipe(Effect.map(Option.getOrUndefined)))?.value,
            Totals.empty(),
          )
          const forkProvider = yield* session
            .snapshot(Conversation.ProviderDoc, { owner: fork.id })
            .pipe(Effect.map(Option.getOrUndefined))
          assert.isDefined(initial)
          assert.isDefined(forkProvider)
          assert.notStrictEqual(initial?.value.sessionId, forkProvider?.value.sessionId)
          assert.match(
            forkProvider?.value.sessionId ?? '',
            /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-/,
          )
          yield* session.root(() => Effect.die('Existing root must not run creation initializer'))
          assert.deepStrictEqual(
            (yield* session
              .snapshot(Conversation.ProviderDoc, { owner: root.id })
              .pipe(Effect.map(Option.getOrUndefined)))?.value,
            initial?.value,
          )
          const reset = yield* Conversation.resetDraft('handoff note')
          yield* session.transaction((tx) => tx.appendEntry(root.id, reset))
          assert.strictEqual(
            (yield* session
              .snapshot(Conversation.AgentDoc, { owner: root.id })
              .pipe(Effect.map(Option.getOrUndefined)))?.value.instructions,
            'current instructions',
          )
          assert.strictEqual((yield* Usage.sessionTotals(session)).models['fake/model']?.input, 3)
          const view = yield* Conversation.context(session, root.id)
          assert.strictEqual(view.entries.length, 1)
          assert.strictEqual(view.messages[0]?.role, 'user')
        }),
      ),
  )

  it.effect('task-owned conversation copies the stored owner agent atomically', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const session = Context.get(yield* Layer.build(services), Session.Session)
        const root = yield* session.root()
        const owned = yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
            agent.model = { provider: 'fake', modelId: 'owner' }
            const taskId = yield* tx.createTask({
              conversationId: root.id,
              kind: 'test',
              version: 1,
              input: null,
              background: false,
              abortRequested: false,
              state: { status: 'running' },
            })
            return yield* tx.createConversation({ ownership: { kind: 'task', taskId } })
          }),
        )
        assert.deepStrictEqual(
          (yield* session
            .snapshot(Conversation.AgentDoc, { owner: owned.id })
            .pipe(Effect.map(Option.getOrUndefined)))?.value.model,
          { provider: 'fake', modelId: 'owner' },
        )
        assert.strictEqual(owned.owner?.conversationId, root.id)
      }),
    ),
  )

  it.effect(
    'post-tools reset places writes before older queued inputs, upgrades boundary and retains unselected order',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = Context.get(yield* Layer.build(services), Session.Session)
          const root = yield* session.root()
          const first = yield* encode(user('first follow up'))
          const second = yield* encode(user('second follow up'))
          const steer = yield* encode(user('steering input'))
          const reset = yield* Conversation.resetDraft()
          const ids = yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const inbox = yield* tx.doc(Inbox.InboxDoc, { owner: root.id })
              const firstId = (yield* tx.createSubmission({
                conversationId: root.id,
                type: 'input',
                status: 'queued',
              })).id
              const secondId = (yield* tx.createSubmission({
                conversationId: root.id,
                type: 'input',
                status: 'queued',
              })).id
              const writeId = (yield* tx.createSubmission({
                conversationId: root.id,
                type: 'write',
                status: 'queued',
              })).id
              const steerId = (yield* tx.createSubmission({
                conversationId: root.id,
                type: 'input',
                status: 'queued',
              })).id
              inbox.items.push(
                { id: firstId, mode: 'followUp', message: Document.copyUnsafe(first) },
                { id: secondId, mode: 'followUp', message: Document.copyUnsafe(second) },
                { id: writeId, mode: 'write', entry: Document.copyUnsafe(reset) },
                { id: steerId, mode: 'steer', message: Document.copyUnsafe(steer) },
              )
              return { firstId, secondId, writeId, steerId }
            }),
          )
          const boundary = yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const prepared = yield* Inbox.prepare(tx, root.id, yield* Agent.settings())
              return yield* Inbox.apply(tx, prepared, 'postTools', DateTime.fromEpochSeconds(0.123))
            }),
          )
          assert.deepStrictEqual(boundary, {
            users: [ids.firstId, ids.steerId],
            settled: [ids.writeId],
            reset: true,
          })
          const entries = (yield* session.scanEntries(
            { conversationId: root.id },
            10,
          )).items.toReversed()
          assert.deepStrictEqual(
            entries.map((entry) => entry.kind),
            ['harness.reset', 'harness.user', 'harness.user'],
          )
          assert.strictEqual(entries[0]?.head, entries[0]?.id)
          assert.deepStrictEqual(
            (yield* session
              .snapshot(Inbox.InboxDoc, { owner: root.id })
              .pipe(Effect.map(Option.getOrUndefined)))?.value.items.map((item) => item.id),
            [ids.secondId],
          )
          assert.strictEqual(
            (yield* session.submission(ids.writeId).pipe(Effect.map(Option.getOrUndefined)))
              ?.status,
            'done',
          )
          assert.strictEqual(
            (yield* session.submission(ids.firstId).pipe(Effect.map(Option.getOrUndefined)))
              ?.status,
            'placed',
          )
          assert.strictEqual(
            (yield* session.submission(ids.secondId).pipe(Effect.map(Option.getOrUndefined)))
              ?.status,
            'queued',
          )
        }),
      ),
  )

  it.effect(
    'stale head writes settle unanswered; abort removes queued inputs while preserving passive writes',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = Context.get(yield* Layer.build(services), Session.Session)
          const root = yield* session.root()
          const first = yield* session.transaction((tx) => tx.appendEntry(root.id, { kind: 'old' }))
          yield* session.transaction((tx) =>
            tx.appendEntry(root.id, { kind: 'reset', head: 'self' }),
          )
          const input = yield* encode(user('queued'))
          const ids = yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const inbox = yield* tx.doc(Inbox.InboxDoc, { owner: root.id })
              const stale = (yield* tx.createSubmission({
                conversationId: root.id,
                type: 'write',
                status: 'queued',
              })).id
              const userId = (yield* tx.createSubmission({
                conversationId: root.id,
                type: 'input',
                status: 'queued',
              })).id
              inbox.items.push(
                { id: stale, mode: 'write', entry: { kind: 'summary', head: first.id } },
                { id: userId, mode: 'followUp', message: Document.copyUnsafe(input) },
              )
              return { stale, userId }
            }),
          )
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const prepared = yield* Inbox.prepare(tx, root.id, yield* Agent.settings())
              return yield* Inbox.apply(tx, prepared, 'postTools', DateTime.fromEpochSeconds(0))
            }),
          )
          assert.strictEqual(
            (yield* session.submission(ids.stale).pipe(Effect.map(Option.getOrUndefined)))?.reason,
            'stale',
          )
          const passiveId = yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const inbox = yield* tx.doc(Inbox.InboxDoc, { owner: root.id })
              const submission = yield* tx.createSubmission({
                conversationId: root.id,
                type: 'write',
                status: 'queued',
              })
              inbox.items.push({ id: submission.id, mode: 'write', entry: { kind: 'passive' } })
              return submission.id
            }),
          )
          assert.deepStrictEqual(yield* session.transaction((tx) => Inbox.withdraw(tx, root.id)), [
            ids.userId,
          ])
          assert.strictEqual(
            (yield* session.submission(ids.userId).pipe(Effect.map(Option.getOrUndefined)))?.reason,
            'aborted',
          )
          assert.deepStrictEqual(
            (yield* session
              .snapshot(Inbox.InboxDoc, { owner: root.id })
              .pipe(Effect.map(Option.getOrUndefined)))?.value.items.map((item) => item.id),
            [passiveId],
          )
        }),
      ),
  )

  it.effect(
    'usage keys cannot mutate prototypes and partial assistant spend does not restore aborted context',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = Context.get(yield* Layer.build(services), Session.Session)
          const root = yield* session.root()
          const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.AssistantMessage))(
            Prompt.assistantMessage({ content: [Prompt.textPart({ text: 'aborted response' })] }),
          )
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              yield* Usage.record(tx, root.id, 'tools', '__proto__', {
                ...Totals.zero(),
                output: 2,
                totalTokens: 2,
              })
              yield* Usage.record(tx, root.id, 'tools', '__proto__', {
                ...Totals.zero(),
                output: 3,
                totalTokens: 3,
              })
              return yield* tx.appendEntry(root.id, {
                kind: 'harness.assistant',
                model: [encoded],
                data: yield* Schema.encodeEffect(Serialization.json(Entry.AssistantData))({
                  harness: { status: 'aborted', usage: Totals.zero() },
                }),
              })
            }),
          )
          const ledger = (yield* session
            .snapshot(Usage.UsageDoc, { owner: root.id })
            .pipe(Effect.map(Option.getOrUndefined)))?.value
          assert.isTrue(Object.hasOwn(ledger?.tools ?? {}, '__proto__'))
          assert.strictEqual(ledger?.tools['__proto__']?.output, 5)
          assert.deepStrictEqual((yield* Conversation.context(session, root.id)).messages, [])
          const invalid = {
            id: yield* Schema.decodeEffect(Record.EntryId)(99),
            conversationId: root.id,
            kind: 'malformed',
            model: [false],
          }
          const invalidContext = yield* Conversation.projectEntry(invalid).pipe(Effect.flip)
          assert.strictEqual(invalidContext.reason._tag, 'InvalidState')
          assert.ok(invalidContext.cause instanceof Schema.SchemaError)
        }),
      ),
  )
})
