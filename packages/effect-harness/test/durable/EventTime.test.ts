import { assert, describe, it } from '@effect/vitest'

import * as Time from 'effect-harness/Time'

import * as DateTime from 'effect/DateTime'

import * as Duration from 'effect/Duration'

import * as Effect from 'effect/Effect'

import * as HashSet from 'effect/HashSet'

import * as Ref from 'effect/Ref'

import * as Schema from 'effect/Schema'
import * as TestSchema from 'effect/testing/TestSchema'

import * as Entry from 'effect-harness/durable/Entry'

import * as Event from 'effect-harness/durable/Event'

import * as Inbox from 'effect-harness/durable/Inbox'

import * as Record from 'effect-harness/durable/Record'

describe('EventTime', () => {
  it.effect(
    'keeps fractional Entry/Event time bytes and suppresses unchanged decoded deferred deadlines',
    () =>
      Effect.gen(function* () {
        const data = { timestamp: Time.fromEpochMillis(1000.5) }
        const entry = new TestSchema.Asserts(Entry.UserData)
        yield* entry.decoding().succeedEffect({ timestamp: 1000.5 }, data)
        yield* entry.encoding().succeedEffect(data, { timestamp: 1000.5 })
        assert.strictEqual(
          DateTime.toEpochMillis(data.timestamp ?? Time.fromEpochMillis(0)),
          1000.5,
        )
        assert.deepStrictEqual(yield* Schema.encodeEffect(Entry.UserData)(data), {
          timestamp: 1000.5,
        })
        assert.strictEqual(
          DateTime.toEpochMillis(
            DateTime.addDuration(data.timestamp ?? Time.fromEpochMillis(0), Duration.millis(0.5)),
          ),
          1001,
        )
        const value = {
          conversation: { id: Record.ROOT_CONVERSATION_ID },
          entries: [],
          docs: { 'harness.live': { generation: { attempt: 1, deferred: { pollAt: 1000.5 } } } },
        }
        const domain = Inbox.domain(value.docs['harness.live'])
        assert.strictEqual(
          DateTime.toEpochMillis(domain.generation?.deferred?.pollAt ?? Time.fromEpochMillis(0)),
          1000.5,
        )
        const snap = yield* Event.snapshot(value)
        // effect-nit-allow P8-testschema-asserts: exact fractional-time JSON representation is an additional wire contract; Event.snapshot and translate are semantic runtime subjects rather than schema round-trip oracles.
        const wire = yield* Schema.encodeEffect(Event.BatchJson)([snap])
        assert.include(JSON.stringify(wire), '"pollAt":1000.5')
        const next = {
          ...value,
          docs: { 'harness.live': { generation: { attempt: 2, deferred: { pollAt: 1000.5 } } } },
        }
        const events = yield* Event.translate(
          Record.ROOT_CONVERSATION_ID,
          {
            seq: Record.Seq.make(2),
            before: value,
            value: next,
            ops: [],
            reset: false,
            publication: { seq: Record.Seq.make(2), writes: [], documents: [] },
          },
          yield* Ref.make(HashSet.empty<Record.TaskId>()),
        )
        assert.isFalse(events.some((event) => event._tag === 'deferred_poll'))
      }),
  )
})
