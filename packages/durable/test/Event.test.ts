import * as TestClock from 'effect/testing/TestClock'
import * as Option from 'effect/Option'
import { ResourceScope, withLayer } from '@effect-harness/durable/testing/Storage'
import * as Exit from 'effect/Exit'
import * as Scope from 'effect/Scope'
import { assert, describe, it } from '@effect/vitest'
import * as Totals from '@effect-harness/harness/Usage'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import * as Layer from 'effect/Layer'
import * as Prompt from 'effect/ai/Prompt'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as Document from '@effect-harness/durable/Document'
import * as Event from '@effect-harness/durable/Event'
import * as Inbox from '@effect-harness/durable/Inbox'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Usage from '@effect-harness/durable/Usage'
import * as View from '@effect-harness/durable/View'
import * as Memory from '@effect-harness/durable/storage/Memory'

const layers = Event.layer.pipe(
  Layer.provideMerge(View.layer),
  Layer.provideMerge(Session.layer),
  Layer.provideMerge(Memory.layer),
)
const encode = Schema.encodeEffect(Schema.toCodecJson(Prompt.Message))
const assistant = (text: string) =>
  Prompt.assistantMessage({ content: [Prompt.textPart({ text })] })
const generationKind = '@effect-harness/durable/Generation/v1'
const initialize = Effect.gen(function* () {
  const session = yield* Session.Session
  const events = yield* Event.Event
  const views = yield* View.View
  const root = yield* session.root(
    Effect.fnUntraced(function* (tx) {
      yield* tx.doc(Conversation.AgentDoc, { owner: Record.ROOT_CONVERSATION_ID })
      yield* tx.doc(Inbox.LiveDoc, { owner: Record.ROOT_CONVERSATION_ID })
      yield* tx.doc(Inbox.InboxDoc, { owner: Record.ROOT_CONVERSATION_ID })
      yield* tx.doc(Usage.UsageDoc, { owner: Record.ROOT_CONVERSATION_ID })
    }),
  )
  return { session, events, views, root }
})
const collect = (watch: Event.Watch, count: number) =>
  Effect.gen(function* () {
    const consumer = yield* Stream.runCollect(watch.changes.pipe(Stream.take(count))).pipe(
      Effect.timeout('3 seconds'),
      Effect.forkScoped,
    )
    // The fork admits the consumer before modeled journal polls and mount monitoring advance.
    yield* TestClock.adjust('100 millis')
    return yield* Fiber.join(consumer)
  })
const task = (
  tx: Session.Transaction,
  id: Record.ConversationId,
  kind = generationKind,
  input: Record.Json = {},
) =>
  tx.createTask({
    conversationId: id,
    kind,
    input,
    version: 1,
    background: false,
    abortRequested: false,
    state: { status: 'running' },
  })
describe('Event', () => {
  it.effect('keeps an earlier partial batch across101 attempt-only document mutations', () =>
    Effect.gen(function* () {
      const { session, events, root } = yield* initialize
      const watch = yield* events.watch(root.id)
      const partial = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.AssistantMessage))(
        assistant('retained'),
      )
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          live.generation = { attempt: 1, message: Document.copyUnsafe(partial) }
        }),
      )
      for (let index = 0; index < 110; index++)
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            assert.ok(live.generation)
            live.generation.attempt = index + 2
          }),
        )
      const batch = (yield* collect(watch, 1))[0]
      assert.deepStrictEqual(
        batch?.map((event) => event.type),
        ['message_start'],
      )
      assert.deepStrictEqual(
        batch?.[0]?.type === 'message_start' && batch[0].message,
        assistant('retained'),
      )
      const late = yield* events.watch(root.id)
      assert.strictEqual(late.snapshot.generation?.attempt, 111)
    }).pipe(Effect.provide(layers)),
  )

  it.effect('retains one meaningful event across same-conversation eventless task noise', () =>
    Effect.gen(function* () {
      const { session, events, views, root } = yield* initialize
      const semantic = yield* events.watch(root.id)
      const structural = yield* views.watch(root.id)
      yield* session.transaction((tx) =>
        tx.createTask({
          conversationId: root.id,
          kind: 'rare-failure',
          version: 1,
          input: null,
          background: false,
          abortRequested: false,
          state: {
            status: 'terminal',
            outcome: { status: 'faulted', error: { message: 'rare' } },
          },
        }),
      )
      for (let index = 0; index < 110; index++)
        yield* session.transaction((tx) => task(tx, root.id, 'eventless'))
      assert.deepStrictEqual(
        (yield* collect(semantic, 1))[0]?.map((event) => event.type),
        ['task_failed'],
      )
      yield* session.transaction((tx) => tx.appendEntry(root.id, { kind: 'after-noise' }))
      const collected = yield* Stream.runCollect(structural.changes.pipe(Stream.take(1))).pipe(
        Effect.forkScoped,
      )
      yield* TestClock.adjust('100 millis')
      const frame = (yield* Fiber.join(collected))[0]
      assert.strictEqual(frame?.reset, false)
      assert.deepStrictEqual(
        frame?.value.entries.map((entry) => entry.kind),
        ['after-noise'],
      )
    }).pipe(Effect.provide(layers)),
  )

  it.effect('counts submission batches independently from structural view frames', () =>
    Effect.gen(function* () {
      const { session, events, views, root } = yield* initialize
      const semantic = yield* events.watch(root.id)
      const structural = yield* views.watch(root.id)
      yield* session.transaction((tx) =>
        tx.appendEntry(root.id, { kind: 'one-before-submissions' }),
      )
      for (let index = 0; index < 101; index++)
        yield* session.transaction((tx) =>
          tx.createSubmission({
            _tag: 'WriteQueued' as const,
            conversationId: root.id,
            type: 'write',
            status: 'queued',
          }),
        )
      yield* TestClock.adjust('60 millis')
      assert.strictEqual((yield* collect(semantic, 1))[0]?.[0]?.type, 'snapshot')
      yield* session.transaction((tx) => tx.appendEntry(root.id, { kind: 'only-structural-frame' }))
      const collected = yield* Stream.runCollect(structural.changes.pipe(Stream.take(2))).pipe(
        Effect.forkScoped,
      )
      yield* TestClock.adjust('100 millis')
      const frames = yield* Fiber.join(collected)
      const frame = frames[1]
      assert.strictEqual(frame?.reset, false)
      assert.deepStrictEqual(
        frames.map((value) => value.reset),
        [false, false],
      )
      assert.strictEqual(frame?.value.entries.length, 2)
    }).pipe(Effect.provide(layers)),
  )

  it.effect('reports usage-only partial changes and exact object replacement noops', () =>
    Effect.gen(function* () {
      const { session, events, root } = yield* initialize
      const partial = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.AssistantMessage))(
        assistant('same'),
      )
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          live.generation = { attempt: 1, message: Document.copyUnsafe(partial) }
          live.tools = [
            { callId: 'c', name: 'tool', status: 'running', details: { unchanged: true } },
          ]
        }),
      )
      const watch = yield* events.watch(root.id)
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          assert.ok(live.generation)
          live.generation.usage = { ...Totals.zero(), input: 42, totalTokens: 42 }
        }),
      )
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          assert.ok(live.tools?.[0])
          live.tools[0].details = { unchanged: true }
        }),
      )
      const batches = yield* collect(watch, 2)
      assert.deepStrictEqual(batches[0], [
        {
          _tag: 'message_update' as const,
          type: 'message_update',
          usage: { ...Totals.zero(), input: 42, totalTokens: 42 },
          changes: [],
        },
      ])
      assert.deepStrictEqual(batches[1], [
        {
          _tag: 'tool_execution_update' as const,
          type: 'tool_execution_update',
          toolCallId: 'c',
          toolName: 'tool',
          details: { unchanged: true },
        },
      ])
    }).pipe(Effect.provide(layers)),
  )

  it.effect('orders user admission, partials, answer settlement and lifecycle events exactly', () =>
    Effect.gen(function* () {
      const { session, events, root } = yield* initialize
      const watch = yield* events.watch(root.id)
      assert.deepStrictEqual(watch.snapshot, {
        _tag: 'snapshot' as const,
        type: 'snapshot',
        entries: [],
        tools: [],
        compactions: [],
        inbox: [],
        agent: {},
        usage: Totals.empty(),
      })
      const user = yield* encode(Prompt.userMessage({ content: [Prompt.textPart({ text: 'hi' })] }))
      const partial = yield* Schema.encodeEffect(Schema.toCodecJson(Prompt.AssistantMessage))(
        assistant('hel'),
      )
      const final = yield* encode(assistant('hello'))
      const ids = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          const taskId = yield* task(tx, root.id)
          const input = yield* tx.createSubmission({
            _tag: 'InputQueued' as const,
            conversationId: root.id,
            type: 'input',
            status: 'queued',
          })
          const entry = yield* tx.appendEntry(root.id, { kind: 'harness.user', model: [user] })
          yield* tx.placeSubmission(input.id, entry.id)
          live.run = { taskId, inputs: [input.id] }
          live.generation = { attempt: 1 }
          return { taskId, inputId: input.id }
        }),
      )
      yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          live.generation = { attempt: 1, message: Document.copyUnsafe(partial) }
        }),
      )
      const answer = yield* session.transaction(
        Effect.fnUntraced(function* (tx) {
          const record = yield* tx.task(ids.taskId).pipe(Effect.map(Option.getOrUndefined))
          assert.ok(record)
          const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
          const entry = yield* tx.appendEntry(root.id, {
            kind: 'harness.assistant',
            model: [final],
          })
          yield* Inbox.endRun(tx, live, ids.taskId, { status: 'done', answer: entry.id })
          yield* Usage.record(tx, root.id, 'models', 'fake/model', {
            ...Totals.zero(),
            output: 2,
            totalTokens: 2,
          })
          yield* tx.write({
            _tag: 'task' as const,
            type: 'task',
            value: { ...record, state: { status: 'terminal', outcome: { status: 'done' } } },
          })
          return entry
        }),
      )
      const batches = yield* collect(watch, 3)
      assert.deepStrictEqual(
        batches.map((batch) => batch.map((event) => event.type)),
        [
          ['message_start', 'message_end', 'submission', 'run_start', 'turn_start'],
          ['message_start'],
          ['message_end', 'turn_end', 'run_end', 'submission', 'usage_changed'],
        ],
      )
      assert.strictEqual(
        batches[2]?.[0]?.type === 'message_end' && batches[2][0].entry.id,
        answer.id,
      )
      const late = yield* events.watch(root.id)
      assert.strictEqual(late.snapshot.entries.length, 2)
      assert.strictEqual(late.snapshot.run, undefined)
      assert.strictEqual(late.snapshot.usage.models['fake/model']?.output, 2)
    }).pipe(Effect.provide(layers)),
  )

  it.effect(
    'orders tools, retries, deferred polls, failures, compactions and new runs in one batch',
    () =>
      Effect.gen(function* () {
        const { session, events, root } = yield* initialize
        const toolMessage = yield* encode(
          Prompt.toolMessage({
            content: [
              Prompt.toolResultPart({
                id: 'call',
                name: 'read',
                isFailure: false,
                providerExecuted: false,
                result: 'ok',
              }),
            ],
          }),
        )
        const initial = yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            const generation = yield* task(tx, root.id)
            const toolId = yield* task(tx, root.id, 'tool', { arguments: { path: 'a' } })
            const compaction = yield* task(tx, root.id, 'compact')
            const submission = yield* tx.createSubmission({
              _tag: 'InputPlaced' as const,
              conversationId: root.id,
              type: 'input',
              status: 'placed',
              entry: (yield* tx.appendEntry(root.id, { kind: 'input' })).id,
            })
            live.run = { taskId: generation, inputs: [submission.id] }
            live.generation = { attempt: 2, retry: { at: 200, error: 'retry' } }
            live.tools = [
              { callId: 'call', name: 'read', taskId: toolId, status: 'running', output: 'abc' },
            ]
            live.compactions = [
              { taskId: compaction, reason: 'manual', blocking: true, attempt: 1 },
            ]
            return { generation, toolId, compaction, submission: submission.id }
          }),
        )
        const watch = yield* events.watch(root.id)
        assert.strictEqual(watch.snapshot.tools[0]?.output, 'abc')
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const old = yield* tx.task(initial.generation).pipe(Effect.map(Option.getOrUndefined))
            assert.ok(old)
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            const next = yield* task(tx, root.id)
            const compaction = yield* task(tx, root.id, 'compact')
            const input = yield* tx.createSubmission({
              _tag: 'InputQueued' as const,
              conversationId: root.id,
              type: 'input',
              status: 'queued',
            })
            const result = yield* tx.appendEntry(root.id, {
              kind: 'harness.tool',
              model: [toolMessage],
            })
            yield* tx.settleSubmission(initial.submission, {
              status: 'unanswered',
              reason: 'faulted',
            })
            yield* tx.write({
              _tag: 'task' as const,
              type: 'task',
              value: {
                ...old,
                state: {
                  status: 'terminal',
                  outcome: { status: 'faulted', error: { message: 'broken' } },
                },
              },
            })
            live.tools = [
              {
                callId: 'call',
                name: 'read',
                taskId: initial.toolId,
                status: 'done',
                entry: result.id,
              },
              { callId: 'new', name: 'write', taskId: next, status: 'running' },
            ]
            live.generation = { attempt: 3, deferred: { pollAt: 500 } }
            live.compactions = [
              { taskId: compaction, reason: 'background', blocking: false, attempt: 1 },
            ]
            live.run = { taskId: next, inputs: [input.id] }
            const agent = yield* tx.doc(Conversation.AgentDoc, { owner: root.id })
            agent.instructions = 'new'
          }),
        )
        const batch = (yield* collect(watch, 1))[0]
        assert.deepStrictEqual(
          batch?.map((event) => event.type),
          [
            'tool_execution_start',
            'auto_retry_end',
            'deferred_poll',
            'tool_execution_end',
            'message_start',
            'message_end',
            'compaction_end',
            'task_failed',
            'turn_end',
            'run_end',
            'submission',
            'submission',
            'agent_changed',
            'compaction_start',
            'run_start',
            'turn_start',
          ],
        )
        assert.deepStrictEqual(
          batch?.find((event) => event.type === 'task_failed'),
          {
            _tag: 'task_failed' as const,
            type: 'task_failed',
            taskId: initial.generation,
            kind: generationKind,
            message: 'broken',
          },
        )
      }).pipe(Effect.provide(layers)),
  )

  it.effect(
    'updates retained output/details/diagnostics and reports vanished or created-done tool calls',
    () =>
      Effect.gen(function* () {
        const { session, events, root } = yield* initialize
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            live.tools = [
              {
                callId: 'run',
                name: 'tool',
                status: 'running',
                output: 'old\nkept\n',
                details: { progress: 1 },
                diagnostics: [],
              },
              { callId: 'pending', name: 'missing', status: 'pending' },
            ]
          }),
        )
        const watch = yield* events.watch(root.id)
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            assert.ok(live.tools?.[0])
            live.tools[0].output = 'kept\nnew\n'
            delete live.tools[0].details
            delete live.tools[0].diagnostics
          }),
        )
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            live.tools = [{ callId: 'done-now', name: 'unoffered', status: 'done' }]
          }),
        )
        const batches = yield* collect(watch, 2)
        assert.deepStrictEqual(batches[0], [
          {
            _tag: 'tool_execution_update' as const,
            type: 'tool_execution_update',
            toolCallId: 'run',
            toolName: 'tool',
            output: { trimStart: 4, append: 'new\n' },
            details: null,
            diagnostics: [],
          },
        ])
        assert.deepStrictEqual(
          batches[1]?.map((event) => event.type),
          ['tool_execution_end', 'tool_execution_end', 'tool_execution_end'],
        )
        assert.deepStrictEqual(
          batches[1]?.map((event) => event.type === 'tool_execution_end' && event.toolCallId),
          ['run', 'pending', 'done-now'],
        )
      }).pipe(Effect.provide(layers)),
  )

  it.effect(
    'ends held turns once, including late joins, and starts successor turns without restarting a run',
    () =>
      Effect.gen(function* () {
        const { session, events, root } = yield* initialize
        const ids = yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            const id = yield* task(tx, root.id)
            const input = yield* tx.createSubmission({
              _tag: 'InputQueued' as const,
              conversationId: root.id,
              type: 'input',
              status: 'queued',
            })
            live.run = { taskId: id, inputs: [input.id] }
            return { id, input: input.id }
          }),
        )
        const first = yield* events.watch(root.id)
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const record = yield* tx.task(ids.id).pipe(Effect.map(Option.getOrUndefined))
            assert.ok(record)
            yield* tx.write({
              _tag: 'task' as const,
              type: 'task',
              value: { ...record, state: { status: 'completing', outcome: { status: 'done' } } },
            })
          }),
        )
        assert.deepStrictEqual((yield* collect(first, 1))[0], [
          { _tag: 'turn_end' as const, type: 'turn_end' },
        ])
        const acquiring = yield* events.watch(root.id).pipe(Effect.forkScoped)
        yield* TestClock.adjust('60 millis')
        const late = yield* Fiber.join(acquiring)
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const record = yield* tx.task(ids.id).pipe(Effect.map(Option.getOrUndefined))
            assert.ok(record)
            const live = yield* tx.doc(Inbox.LiveDoc, { owner: root.id })
            const next = yield* task(tx, root.id)
            yield* tx.write({
              _tag: 'task' as const,
              type: 'task',
              value: { ...record, state: { status: 'terminal', outcome: { status: 'done' } } },
            })
            live.run = { taskId: next, inputs: [ids.input] }
          }),
        )
        assert.deepStrictEqual((yield* collect(late, 1))[0], [
          { _tag: 'turn_start' as const, type: 'turn_start' },
        ])
      }).pipe(Effect.provide(layers)),
  )

  it.effect(
    'preserves task and submission events despite an unrelated global journal overflow',
    () =>
      Effect.gen(function* () {
        const { session, events, root } = yield* initialize
        const other = yield* session.transaction((tx) =>
          tx.createConversation({ ownership: { _tag: 'ownerless' as const, kind: 'ownerless' } }),
        )
        const watch = yield* events.watch(root.id)
        const own = yield* session.transaction((tx) =>
          tx.createSubmission({
            _tag: 'WriteQueued' as const,
            conversationId: root.id,
            type: 'write',
            status: 'queued',
          }),
        )
        for (let index = 0; index < 110; index++)
          yield* session.transaction((tx) => task(tx, other.id, 'noise'))
        yield* session.transaction((tx) =>
          tx.settleSubmission(own.id, { status: 'unanswered', reason: 'withdrawn' }),
        )
        const batches = yield* collect(watch, 2)
        assert.deepStrictEqual(
          batches.map((batch) => batch.map((event) => event.type)),
          [['submission'], ['submission']],
        )
      }).pipe(Effect.provide(layers)),
  )

  it.effect(
    'replaces101 pending semantic batches with a current snapshot then resumes with exact changes',
    () =>
      Effect.gen(function* () {
        const { session, events, root } = yield* initialize
        const watch = yield* events.watch(root.id)
        for (let index = 0; index < 101; index++)
          yield* session.transaction((tx) => tx.appendEntry(root.id, { kind: `entry-${index}` }))
        yield* TestClock.adjust('60 millis')
        const batch = (yield* collect(watch, 1))[0]
        assert.strictEqual(batch?.length, 1)
        assert.strictEqual(batch?.[0]?.type, 'snapshot')
        assert.strictEqual(batch?.[0]?.type === 'snapshot' && batch[0].entries.length, 101)
      }).pipe(Effect.provide(layers)),
  )

  it.effect(
    'overflows only pending batches while a listener is in flight and closes at Session shutdown',
    () =>
      Effect.gen(function* () {
        const { session, events, root } = yield* initialize
        const watch = yield* events.watch(root.id)
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const batches: Event.Batch[] = []
        const listening = yield* watch
          .listen((batch) =>
            Effect.gen(function* () {
              batches.push(batch)
              if (batches.length === 1) {
                yield* Deferred.succeed(entered, undefined)
                yield* Deferred.await(release)
              }
            }),
          )
          .pipe(Effect.forkScoped)
        yield* session.transaction((tx) => tx.appendEntry(root.id, { kind: 'inflight' }))
        yield* TestClock.adjust('60 millis')
        yield* Deferred.await(entered)
        for (let index = 0; index < 101; index++)
          yield* session.transaction((tx) => tx.appendEntry(root.id, { kind: 'pending' }))
        yield* TestClock.adjust('60 millis')
        yield* Deferred.succeed(release, undefined)
        yield* TestClock.adjust('60 millis')
        assert.strictEqual(batches.length, 2)
        assert.strictEqual(batches[1]?.[0]?.type, 'snapshot')
        assert.strictEqual(
          batches[1]?.[0]?.type === 'snapshot' && batches[1][0].entries.length,
          102,
        )
        yield* Scope.close(yield* ResourceScope, Exit.void)
        yield* TestClock.adjust('60 millis')
        assert.strictEqual(yield* watch.closed, 'session_closed')
        yield* Fiber.join(listening)
      }).pipe((effect) =>
        withLayer(
          effect.pipe(Effect.provide(Event.layer.pipe(Layer.provideMerge(View.layer)))),
          Session.layer.pipe(Layer.provideMerge(Memory.layer)),
        ),
      ),
  )

  it.effect(
    'translates native text, reasoning and tool arguments deltas and falls back on replacement',
    () =>
      Effect.sync(() => {
        const before = Prompt.assistantMessage({
          content: [
            Prompt.textPart({ text: 'hel' }),
            Prompt.reasoningPart({ text: 'think' }),
            Prompt.toolCallPart({
              id: 'c',
              name: 'tool',
              params: { path: 'a/' },
              providerExecuted: false,
            }),
          ],
        })
        const after = Prompt.assistantMessage({
          content: [
            Prompt.textPart({ text: 'hello' }),
            Prompt.reasoningPart({ text: 'thinking' }),
            Prompt.toolCallPart({
              id: 'c',
              name: 'tool',
              params: { path: 'a/b' },
              providerExecuted: false,
            }),
          ],
        })
        const base = ['docs', 'harness.live', 'generation', 'message', 'content']
        assert.deepStrictEqual(
          Event.messageChanges(
            [
              ['set', [...base, 0, 'text'], 'hello'],
              ['set', [...base, 1, 'text'], 'thinking'],
              ['set', [...base, 2, 'params', 'path'], 'a/b'],
            ],
            before,
            after,
          ),
          [
            { _tag: 'text_delta' as const, type: 'text_delta', contentIndex: 0, delta: 'lo' },
            {
              _tag: 'thinking_delta' as const,
              type: 'thinking_delta',
              contentIndex: 1,
              delta: 'ing',
            },
            {
              _tag: 'toolcall_delta' as const,
              type: 'toolcall_delta',
              contentIndex: 2,
              path: ['path'],
              delta: 'b',
            },
          ],
        )
        assert.deepStrictEqual(
          Event.messageChanges(
            [['set', ['docs', 'harness.live', 'generation'], {}]],
            before,
            after,
          ),
          [{ _tag: 'message' as const, type: 'message', message: after }],
        )
        assert.deepStrictEqual(Option.getOrUndefined(Event.outputChange('abc', 'abcXYZ')), {
          append: 'XYZ',
        })
        assert.deepStrictEqual(Option.getOrUndefined(Event.outputChange('abc', 'z')), { set: 'z' })
      }),
  )
})
