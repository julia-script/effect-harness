import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Document from '../../src/Document.ts'
import * as Entry from '../../src/Entry.ts'
import * as Record from '../../src/Record.ts'
import * as Store from '../../src/Store.ts'
import * as Observation from '../../src/Observation.ts'
import * as View from '../../src/View.ts'
import * as State from '../../src/storage/State.ts'

const record: Record.Document = {
  id: Record.DocumentId.make(2),
  kind: 'counter',
  scope: { kind: 'session' },
  createdAt: Record.Seq.make(1),
}
const definition = Object.freeze({
  kind: 'counter',
  version: 1,
  scope: 'session' as const,
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
})

describe('constructed carrier boundaries', () => {
  it.effect('accepts frozen inputs, leaves config untouched and preserves serialized bytes', () =>
    Effect.sync(() => {
      const token = Document.defineUnsafe(definition)
      assert.isTrue(Document.isDocument(token))
      assert.isFalse(Document.isDocument(definition))
      assert.isTrue(Record.isEntryToken(Entry.UserEntry))
      assert.isFalse(Record.isEntryToken({ kind: Entry.UserEntry.kind }))
      const input = Object.freeze({ record, version: 1, value: { count: 0 }, deltasSinceBase: 0 })
      const snapshot = Document.makeSnapshot(input)
      assert.notStrictEqual<object>(snapshot, input)
      assert.strictEqual(JSON.stringify(snapshot), JSON.stringify(input))
      assert.strictEqual(Reflect.ownKeys(input).length, 4)
      const pageInput = Object.freeze({ items: [record], next: { after: 2 } })
      const page = Record.makePage(pageInput)
      assert.isTrue(Record.isPage(page))
      assert.notStrictEqual<object>(page, pageInput)
      assert.strictEqual(JSON.stringify(page), JSON.stringify(pageInput))
      const candidateInput = Object.freeze({ state: Record.emptyState(), writes: [], result: null })
      const candidate = Store.makeCandidate(candidateInput)
      assert.isTrue(Store.isCandidate(candidate))
      assert.notStrictEqual<object>(candidate, candidateInput)
      assert.strictEqual(JSON.stringify(candidate), JSON.stringify(candidateInput))
    }),
  )
  it.effect('forwards lazy watch and state getters without any construction sampling', () =>
    Effect.sync(() => {
      let reads = 0
      let count = 0
      const watchInput = Object.freeze({
        get value() {
          reads++
          return { count }
        },
        record,
        changes: Stream.empty,
        closed: Effect.succeed('stopped' as const),
        stop: Effect.void,
        listen: () => Effect.void,
      })
      const watch = Observation.makeWatch(watchInput)
      const state = Observation.makeState(
        Object.freeze({
          get value() {
            reads++
            return { count }
          },
          record,
          get cursor() {
            return count
          },
          closed: Effect.succeed('stopped' as const),
        }),
      )
      const projected = View.makeProjectionWatch(
        Object.freeze({
          get value() {
            reads++
            return count
          },
          changes: Stream.empty,
          closed: Effect.succeed('stopped' as const),
          stop: Effect.void,
          listen: () => Effect.void,
        }),
      )
      assert.strictEqual(reads, 0)
      assert.isTrue(Observation.isWatch(watch))
      assert.isTrue(Observation.isState(state))
      assert.isTrue(View.isProjectionWatch(projected))
      assert.isFalse(Observation.isWatch(watchInput))
      count = 9
      assert.strictEqual(watch.value?.count, 9)
      assert.strictEqual(state.value?.count, 9)
      assert.strictEqual(state.cursor, 9)
      assert.strictEqual(projected.value, 9)
      assert.strictEqual(reads, 3)
    }),
  )
  it.effect('rejects malformed empty persisted paths and permits root View replacement', () =>
    Effect.gen(function* () {
      const malformed: unknown = [['delete', []]]
      assert.isFalse(Schema.is(Schema.Array(Record.Op))(malformed))
      const error = yield* State.applyOps({ count: 1 }, malformed as ReadonlyArray<Record.Op>).pipe(
        Effect.flip,
      )
      assert.strictEqual(error._tag, 'StorageError')
      assert.strictEqual(error.reason._tag, 'Corrupt')
      assert.strictEqual(error.message, 'Invalid document operation')
      const view: View.Value = {
        conversation: { id: Record.ROOT_CONVERSATION_ID },
        entries: [],
        docs: {},
      }
      const rejected = View.apply(view, malformed as ReadonlyArray<View.Op>)
      assert.isTrue(Result.isFailure(rejected))
      if (Result.isFailure(rejected))
        assert.strictEqual(rejected.failure.message, 'Invalid view operation')
      const replacement = { ...view, docs: {} }
      assert.strictEqual(
        Result.getOrThrow(View.apply(view, [['set', [], replacement]])),
        replacement,
      )
      assert.isTrue(
        Option.isNone(
          State.findDocument(
            Record.emptyState(),
            { kind: 'missing', scope: { kind: 'session' } },
            'current',
          ),
        ),
      )
    }),
  )
})
