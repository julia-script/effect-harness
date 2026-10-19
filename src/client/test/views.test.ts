/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Views across the transport: the client replays the loop's frames into the same values.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Fiber, Stream } from 'effect'
import { HarnessTransport } from '../index.ts'
import { open, runTest } from './support.ts'

const transports = [
  ['memory', HarnessTransport.makeMemory],
  ['encoded memory', HarnessTransport.makeMemoryEncoded],
] as const

for (const [name, transport] of transports) {
  describe(`views over ${name}`, () => {
    test('a view, its events and the task graph follow a turn', () =>
      runTest((storage) =>
        Effect.scoped(
          Effect.gen(function* () {
            const { client, answer } = yield* open(storage, {}, transport)
            const root = yield* client.root()
            const view = yield* root.view
            const events = yield* root.events
            const graph = yield* client.taskGraph
            expect(view.value.entries).toEqual([])
            expect(events.snapshot.type).toBe('snapshot')
            expect(graph.value).toEqual({ tasks: {} })
            const entries = yield* Effect.forkChild(
              view.changes.pipe(
                Stream.map((change) => change.value.entries.map((entry) => entry.kind)),
                Stream.takeUntil((kinds) => kinds.length === 2),
                Stream.runLast,
              ),
            )
            const types = yield* Effect.forkChild(
              events.batches.pipe(
                Stream.flattenIterable,
                Stream.map((event) => event.type),
                Stream.takeUntil((type) => type === 'run_end'),
                Stream.runCollect,
              ),
            )
            const tasks = yield* Effect.forkChild(
              graph.changes.pipe(
                Stream.map((change) => Object.values(change.value.tasks).map((task) => task.kind)),
                Stream.takeUntil((kinds) => kinds.length === 0),
                Stream.runCollect,
              ),
            )
            const submission = yield* root.submit({ type: 'input', content: 'hi' })
            yield* answer('hello')
            yield* submission.wait
            expect(yield* Fiber.join(entries)).toMatchObject({ value: ['pi.user', 'pi.assistant'] })
            expect(yield* Fiber.join(types)).toContain('run_start')
            expect((yield* Fiber.join(tasks))[0]).toEqual(['pi.generation'])
            // The replayed view is the loop's view.
            const fresh = yield* root.view
            expect(fresh.value.entries.length).toBe(2)
          }),
        ),
      ))
  })
}
