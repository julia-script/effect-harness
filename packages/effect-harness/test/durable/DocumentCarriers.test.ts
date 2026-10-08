import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Document from 'effect-harness/durable/Document'

import * as Entry from 'effect-harness/durable/Entry'

import * as Record from 'effect-harness/durable/Record'

import * as Store from 'effect-harness/durable/Store'

const record: Record.Document = {
  id: Record.DocumentId.make(2),
  kind: 'counter',
  scope: { _tag: 'session' as const },
  createdAt: Record.Seq.make(1),
}

const definition = Object.freeze({
  kind: 'counter',
  version: 1,
  scope: 'session' as const,
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
})

describe('DocumentCarriers', () => {
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
      assert.deepStrictEqual(snapshot.toJSON(), {
        _id: '@effect-harness/durable/Document/Snapshot',
        record: '[Opaque]',
        version: 1,
        value: '[Opaque]',
        deltasSinceBase: 0,
      })
      assert.strictEqual(snapshot.record, input.record)
      assert.deepStrictEqual(snapshot.value, input.value)
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
})
