/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Conversation views, agent events, and the task graph through whole runs. Ported in spirit from
 * pi's `harness-view.test.ts`, `harness-events.test.ts` and `harness-task-graph.test.ts`.
 */
import { describe as group, expect, test } from 'bun:test'
import { Effect, Fiber, Stream } from 'effect'
import { Ops } from '../../delta/index.ts'
import { Faux } from '../../model/index.ts'
import { SessionClosed } from '../../session/SessionError.ts'
import * as Feed from '../internal/views/feed.ts'
import type { AgentEvent } from '../AgentEvent.ts'
import type { Conversation } from '../Conversation.ts'
import type { Change } from '../ViewState.ts'
import { open, ownerless, runTest } from './generationSupport.ts'
import { openClosable } from './support.ts'

const ask = (conversation: Conversation, content: string) =>
  Effect.orDie(Effect.flatMap(conversation.submit({ type: 'input', content }), (s) => s.wait))

/** Event batches until one ends the run, flattened to their types. */
const runEvents = (batches: Stream.Stream<ReadonlyArray<AgentEvent>, unknown>) =>
  Effect.forkChild(
    batches.pipe(
      Stream.takeUntil((batch) => batch.some((event) => event.type === 'run_end')),
      Stream.runCollect,
      // Up to the run's end; the same commit settles its inputs after it.
      Effect.map((all) => {
        const flat = all.flat()
        return flat.slice(0, flat.findIndex((event) => event.type === 'run_end') + 1)
      }),
      Effect.orDie,
    ),
  )

group('Conversation.view', () => {
  test('starts from the committed view and replays every change with its ops', () =>
    runTest((storage) =>
      Effect.scoped(
        Effect.gen(function* () {
          const { harness, faux } = yield* open(storage, { agent: () => ({ tools: [] }) })
          yield* faux.setResponses([Faux.message('hello')])
          const conversation = yield* ownerless(harness)
          const watch = yield* Effect.orDie(conversation.view)
          expect(watch.value.conversation.id).toBe(conversation.id)
          expect(watch.value.entries).toEqual([])
          expect(Object.keys(watch.value.docs).toSorted()).toEqual(['$agent', '$inbox', '$live'])
          const changes = yield* Effect.forkChild(
            watch.changes.pipe(
              Stream.takeUntil(
                (change: Change<typeof watch.value>) =>
                  change.value.entries.length === 2 &&
                  change.value.docs['$live']?.['run'] === undefined,
              ),
              Stream.runCollect,
              Effect.orDie,
            ),
          )
          yield* ask(conversation, 'hi')
          const all = yield* Fiber.join(changes)
          let value = watch.value
          for (const change of all) {
            value = Ops.apply(value, change.ops)
            expect(value).toEqual(change.value)
          }
          const last = all.at(-1)!.value
          expect(last.entries.map((entry) => entry.kind)).toEqual(['$user', '$assistant'])
          expect(Object.keys(last.docs)).toContain('$usage')
        }),
      ),
    ))
})

group('Conversation.events', () => {
  test('a run without tools', () =>
    runTest((storage) =>
      Effect.scoped(
        Effect.gen(function* () {
          const { harness, faux } = yield* open(storage, { agent: () => ({ tools: [] }) })
          yield* faux.setResponses([Faux.message('hello')])
          const conversation = yield* ownerless(harness)
          const events = yield* Effect.orDie(conversation.events)
          expect(events.snapshot).toMatchObject({
            type: 'snapshot',
            entries: [],
            tools: [],
            inbox: [],
          })
          const collected = yield* runEvents(events.batches)
          yield* ask(conversation, 'hi')
          const types = (yield* Fiber.join(collected)).map((event) => event.type)
          // Entries come before the submission that placed them; the run settles its input last.
          expect(types).toEqual([
            'message_start',
            'message_end',
            'submission',
            'run_start',
            'turn_start',
            'message_start',
            'message_end',
            'turn_end',
            'run_end',
          ])
        }),
      ),
    ))

  test('tool calls start and end around their results', () =>
    runTest((storage) =>
      Effect.scoped(
        Effect.gen(function* () {
          const { harness, faux } = yield* open(storage)
          yield* faux.setResponses([
            Faux.message([Faux.toolCall('read', { path: 'a' }, 'c1')], { stopReason: 'toolUse' }),
            Faux.message('done'),
          ])
          const conversation = yield* ownerless(harness)
          const events = yield* Effect.orDie(conversation.events)
          const collected = yield* runEvents(events.batches)
          yield* ask(conversation, 'go')
          const all = yield* Fiber.join(collected)
          const start = all.findIndex((event) => event.type === 'tool_execution_start')
          const end = all.findIndex((event) => event.type === 'tool_execution_end')
          expect(all[start]).toMatchObject({
            toolCallId: 'c1',
            toolName: 'read',
            args: { path: 'a' },
          })
          expect(all[end]).toMatchObject({ toolCallId: 'c1', entry: { kind: '$tool-result' } })
          expect(all[end + 1]?.type).toBe('message_start')
          expect(start).toBeLessThan(end)
        }),
      ),
    ))
})

group('Harness.taskGraph', () => {
  test('shows each live task until it ends', () =>
    runTest((storage) =>
      Effect.scoped(
        Effect.gen(function* () {
          const { harness, faux } = yield* open(storage, { agent: () => ({ tools: [] }) })
          yield* faux.setResponses([Faux.message('hello')])
          const conversation = yield* ownerless(harness)
          const watch = yield* Effect.orDie(harness.taskGraph)
          expect(watch.value).toEqual({ tasks: {} })
          const changes = yield* Effect.forkChild(
            watch.changes.pipe(
              Stream.map((change) => Object.values(change.value.tasks)),
              Stream.takeUntil((tasks) => tasks.length === 0),
              Stream.runCollect,
              Effect.orDie,
            ),
          )
          yield* ask(conversation, 'hi')
          const states = yield* Fiber.join(changes)
          expect(states[0]?.[0]).toMatchObject({
            kind: '$generation',
            conversationId: conversation.id,
            background: false,
            abortRequested: false,
            state: { status: 'pending' },
            conversations: [],
          })
          expect(states.at(-1)).toEqual([])
        }),
      ),
    ))
})

group('closing', () => {
  test('a closing loop ends every watch, and later watches fail', () =>
    runTest((storage) =>
      Effect.scoped(
        Effect.gen(function* () {
          const { harness, close } = yield* openClosable(storage)
          const conversation = yield* ownerless(harness)
          const view = yield* Effect.orDie(conversation.view)
          const events = yield* Effect.orDie(conversation.events)
          const graph = yield* Effect.orDie(harness.taskGraph)
          const ended = yield* Effect.forkChild(
            Effect.all([
              Stream.runDrain(view.changes),
              Stream.runDrain(events.batches),
              Stream.runDrain(graph.changes),
            ]),
          )
          yield* close
          yield* Effect.orDie(Fiber.join(ended))
          const refused = yield* Effect.flip(Effect.scoped(conversation.view))
          expect(refused).toBeInstanceOf(SessionClosed)
        }),
      ),
    ))
})

group('overflow', () => {
  test('a reader that falls behind gets one collapsed item for what it missed', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        let latest = 0
        const feed = yield* Feed.make(() => -latest)
        for (latest = 1; latest <= 105; latest++) yield* feed.offer(latest)
        const items = yield* Stream.runCollect(Stream.take(feed.stream, 5))
        // The first hundred collapsed when the 101st arrived; the rest followed it.
        expect(items).toEqual([-101, 102, 103, 104, 105])
      }),
    ))
})
