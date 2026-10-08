import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Stream from 'effect/Stream'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import type * as Response from 'effect/ai/Response'
import * as Agent from '../src/Agent.ts'
import * as Executor from '../src/Executor.ts'
import * as Model from '../src/Model.ts'
import * as Registry from '../src/Registry.ts'
import type * as Record from '../src/Record.ts'
import * as CompactionTask from '../src/internal/CompactionTask.ts'
import * as ConversationState from '../src/internal/ConversationState.ts'
import * as Scheduler from '../src/internal/Scheduler.ts'
import * as Session from '../src/internal/Session.ts'
import * as Memory from '../src/storage/Memory.ts'

const sessionLayer = Session.layer.pipe(Layer.provide(Memory.layer))
const ref = { provider: 'test', modelId: 'summary' }
const finish: Response.PartEncoded = {
  type: 'finish',
  reason: 'stop',
  usage: { inputTokens: { total: 5 }, outputTokens: { total: 2 } },
}
const response: ReadonlyArray<Response.PartEncoded> = [
  { type: 'text', text: 'A concise saved summary.' },
  finish,
]
const fixture = Effect.fn('CompactionTask.test.fixture')(function* (blocked = false) {
  const session = yield* Session.Session
  const conversation = yield* session.root
  const entries = yield* session.transaction(
    Effect.fn('CompactionTask.test.history')(function* (tx) {
      const agent = yield* tx.doc(ConversationState.AgentDoc, { owner: conversation.id })
      agent.model = ref
      const history: Array<Record.Entry> = []
      for (const text of ['first question', 'second question', 'third question']) {
        history.push(
          yield* tx.appendEntry(conversation.id, {
            kind: 'harness.user',
            model: yield* ConversationState.encodeMessages([
              Prompt.userMessage({ content: [Prompt.textPart({ text })] }),
            ]),
          }),
        )
      }
      return history
    }),
  )
  const started = yield* Deferred.make<void>()
  const released = yield* Deferred.make<void>()
  let calls = 0
  let savedRequest = false
  const native = yield* LanguageModel.make({
    generateText: () =>
      Effect.gen(function* () {
        calls++
        const tasks = yield* session
          .scanTasks({ conversationId: conversation.id, kind: 'harness.compaction' })
          .pipe(Stream.runCollect)
        savedRequest = tasks.some((task) => {
          const checkpoint = task.state.checkpoint
          return (
            checkpoint !== null &&
            typeof checkpoint === 'object' &&
            'phase' in checkpoint &&
            checkpoint.phase === 'summarize' &&
            'request' in checkpoint
          )
        })
        yield* Deferred.succeed(started, undefined)
        if (blocked) yield* Deferred.await(released)
        return [...response]
      }).pipe(Effect.orDie),
    streamText: () => Stream.empty,
  })
  const executor = yield* Executor.Executor.pipe(
    Effect.provide(
      Executor.layer.pipe(
        Layer.provide(
          Layer.merge(
            Registry.layer(),
            Model.layer([
              {
                ref,
                model: native,
                contextWindow: 1000,
                maxOutputTokens: 100,
                configure: () => Effect.succeed(Context.empty()),
              },
            ]),
          ),
        ),
      ),
    ),
  )
  const definition = yield* CompactionTask.make(session, executor, {
    settings: yield* Agent.settings({
      compaction: { keepRecentTokens: 1, reserveTokens: 100 },
    }),
    cwd: '.',
    report: () => Effect.void,
  })
  const scheduler = yield* Scheduler.make(session, [definition])
  return {
    session,
    conversation,
    entries,
    scheduler,
    definition,
    started,
    released,
    calls: () => calls,
    savedRequest: () => savedRequest,
  }
})
const taskResult = (task: Record.Task) => task.state.outcome

describe('checkpoint compaction', () => {
  it.live(
    'pins the summary before inference and atomically places the summary with its outcome',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const f = yield* fixture()
          const observed = yield* f.session.observe(f.session.metadata)
          const id = yield* f.scheduler.create(
            f.definition,
            { reason: 'manual' },
            { conversationId: f.conversation.id },
          )
          const terminal = yield* f.scheduler.await(id)
          assert.isTrue(f.savedRequest())
          assert.strictEqual(f.calls(), 1)
          assert.strictEqual(terminal.state.status, 'terminal')
          const history = yield* f.session
            .scanEntries({ conversationId: f.conversation.id })
            .pipe(Stream.runCollect)
          assert.lengthOf(history, 4)
          const marker = history.find((entry) => entry.kind === 'harness.compaction')
          assert.isDefined(marker)
          if (marker === undefined) return yield* Effect.die('Missing summary entry')
          assert.strictEqual(marker.head, f.entries[2]?.id)
          assert.deepStrictEqual(taskResult(terminal), {
            status: 'completed',
            result: { placed: true, entryId: marker.id },
          })
          const frames = yield* observed.frames.pipe(
            Stream.takeUntil((frame) =>
              frame.writes.some((write) => write._tag === 'entry' && write.value.id === marker.id),
            ),
            Stream.runCollect,
          )
          const placement = frames[frames.length - 1]
          assert.isTrue(
            placement?.writes.some(
              (write) =>
                write._tag === 'task' &&
                write.value.id === id &&
                write.value.state.status === 'terminal',
            ),
          )
        }).pipe(Effect.provide(sessionLayer)),
      ),
  )

  it.live('keeps ordinary entries appended while summarization runs', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture(true)
        const id = yield* f.scheduler.create(
          f.definition,
          { reason: 'threshold' },
          { conversationId: f.conversation.id, background: true },
        )
        yield* Deferred.await(f.started)
        const appended = yield* f.session.transaction((tx) =>
          tx.appendEntry(f.conversation.id, { kind: 'harness.user' }),
        )
        yield* Deferred.succeed(f.released, undefined)
        const terminal = yield* f.scheduler.await(id)
        assert.strictEqual(terminal.state.status, 'terminal')
        const view = yield* ConversationState.context(f.session, f.conversation.id)
        assert.strictEqual(view.head?.kind, 'harness.compaction')
        assert.isTrue(view.entries.some((entry) => entry.id === appended.id))
      }).pipe(Effect.provide(sessionLayer)),
    ),
  )

  it.live('discards a stale summary when a reset replaces its context head', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture(true)
        const id = yield* f.scheduler.create(
          f.definition,
          { reason: 'threshold' },
          { conversationId: f.conversation.id },
        )
        yield* Deferred.await(f.started)
        const last = f.entries[2]
        if (last === undefined) return yield* Effect.die('Missing retained entry')
        const reset = yield* f.session.transaction((tx) =>
          tx.appendEntry(f.conversation.id, { kind: 'harness.reset', head: last.id }),
        )
        yield* Deferred.succeed(f.released, undefined)
        assert.deepStrictEqual(taskResult(yield* f.scheduler.await(id)), {
          status: 'completed',
          result: { placed: false, reason: 'stale' },
        })
        const view = yield* ConversationState.context(f.session, f.conversation.id)
        assert.strictEqual(view.head?.id, reset.id)
        assert.lengthOf(
          yield* f.session
            .scanEntries({ conversationId: f.conversation.id })
            .pipe(Stream.runCollect),
          4,
        )
      }).pipe(Effect.provide(sessionLayer)),
    ),
  )

  it.live('aborts an in-flight summary and retires task progress without placing an entry', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture(true)
        const id = yield* f.scheduler.create(
          f.definition,
          { reason: 'manual' },
          { conversationId: f.conversation.id },
        )
        yield* Deferred.await(f.started)
        yield* f.session.transaction(
          Effect.fn(function* (tx) {
            const progress = yield* tx.doc(ConversationState.ProgressDoc, { owner: id })
            progress.output = 'in progress'
          }),
        )
        yield* f.scheduler.abort(id)
        assert.deepStrictEqual(taskResult(yield* f.scheduler.await(id)), { status: 'aborted' })
        assert.isTrue(
          Option.isNone(yield* f.session.snapshot(ConversationState.ProgressDoc, { owner: id })),
        )
        assert.lengthOf(
          yield* f.session
            .scanEntries({ conversationId: f.conversation.id })
            .pipe(Stream.runCollect),
          3,
        )
      }).pipe(Effect.provide(sessionLayer)),
    ),
  )

  it.live('resumes a saved summary at placement without calling the model again', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const f = yield* fixture()
        const last = f.entries[2]
        if (last === undefined) return yield* Effect.die('Missing retained entry')
        const id = yield* f.session.transaction((tx) =>
          tx.createTask({
            conversationId: f.conversation.id,
            kind: f.definition.name,
            version: f.definition.version,
            input: { reason: 'manual' },
            background: false,
            abortRequested: false,
            state: {
              status: 'pending',
              checkpoint: {
                phase: 'place',
                firstKept: last.id,
                tail: last.id,
                summary: 'Previously committed summary.',
              },
            },
          }),
        )
        yield* f.scheduler.resume
        const terminal = yield* f.scheduler.await(id)
        assert.strictEqual(terminal.state.status, 'terminal')
        assert.strictEqual(f.calls(), 0)
        const view = yield* ConversationState.context(f.session, f.conversation.id)
        assert.strictEqual(view.head?.kind, 'harness.compaction')
        assert.isTrue(
          view.messages.some(
            (message) =>
              message.role === 'user' &&
              message.content.some(
                (part) =>
                  part.type === 'text' && part.text.includes('Previously committed summary.'),
              ),
          ),
        )
      }).pipe(Effect.provide(sessionLayer)),
    ),
  )
})
