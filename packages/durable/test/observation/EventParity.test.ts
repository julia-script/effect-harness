import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Stream from 'effect/Stream'
import * as Event from '../../src/Event.ts'
import * as View from '../../src/View.ts'
import * as Session from '../../src/Session.ts'
import * as Memory from '../../src/storage/Memory.ts'

describe('expected task failure event semantics', () => {
  it.live(
    'ordinary failed receipts do not emit task_failed; faulted and orphaned receipts do',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const session = yield* Session.Session
          const root = yield* session.root()
          const watch = yield* (yield* Event.Event).watch(root.id)
          for (const status of ['failed', 'faulted', 'orphaned'])
            yield* session.transaction(
              Effect.fnUntraced(function* (tx) {
                yield* tx.createTask({
                  conversationId: root.id,
                  kind: 'custom',
                  version: 1,
                  input: null,
                  background: false,
                  abortRequested: false,
                  state: { status: 'terminal', outcome: { status, detail: status } },
                })
                yield* tx.appendEntry(root.id, { kind: 'boundary', data: { status } })
              }),
            )
          const batches = yield* Stream.runCollect(watch.changes.pipe(Stream.take(3))).pipe(
            Effect.timeout('3 seconds'),
          )
          assert.strictEqual(
            batches[0]?.some((event) => event.type === 'task_failed'),
            false,
          )
          assert.deepStrictEqual(
            batches
              .slice(1)
              .flatMap((batch) =>
                batch.filter((event) => event.type === 'task_failed').map((event) => event.message),
              ),
            ['faulted', 'orphaned'],
          )
        }).pipe(
          Effect.provide(
            Event.layer.pipe(
              Layer.provideMerge(View.layer),
              Layer.provideMerge(Session.layer),
              Layer.provide(Memory.layer),
            ),
          ),
        ),
      ),
  )
})
