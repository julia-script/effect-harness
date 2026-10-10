import { expect, test } from 'tstyche'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Document from 'effect-harness/Document'
import type * as Session from 'effect-harness/Session'
import { SessionError } from 'effect-harness/SessionError'
import * as Transaction from 'effect-harness/Transaction'

const schema = Schema.Struct({ count: Schema.Natural })
const document = Document.define({
  kind: 'typed.migration',
  version: 3,
  scope: 'session',
  schema,
  initial: () => ({ count: 0 }),
  migrations: {
    1: (value) => {
      expect(value).type.toBe<Schema.JsonObject>()
      return Schema.decodeUnknownEffect(Schema.Struct({ legacy: Schema.Natural }))(value).pipe(
        Effect.map(({ legacy }) => ({ count: legacy })),
      )
    },
    2: () =>
      Effect.fail(
        new SessionError({ reason: 'conflict', operation: 'migration', message: 'unsupported' }),
      ),
  },
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target
declare const tx: Transaction.Transaction

test('migration acquisition and replacement retain decoded schema types and typed failures', () => {
  expect(document).type.toBe<Document.Document<typeof schema>>()
  expect(Transaction.ensureDocument(tx, document, target)).type.toBe<
    Effect.Effect<Document.Snapshot<typeof schema>, Session.Failure>
  >()
  expect(tx.pipe(Transaction.ensureDocument(document, target))).type.toBe<
    Effect.Effect<Document.Snapshot<typeof schema>, Session.Failure>
  >()
  expect(
    Transaction.updateDocument(tx, document, target, (value) => {
      expect(value).type.toBe<{ readonly count: number }>()
      return { count: value.count + 1 }
    }),
  ).type.toBe<Effect.Effect<void, Session.Failure>>()
})

test('migration callbacks consume and return encoded JSON objects', () => {
  expect<
    (_: Schema.JsonObject) => Effect.Effect<Schema.JsonObject>
  >().type.toBeAssignableTo<Document.Migration>()
  expect<
    (_: Schema.JsonObject) => Effect.Effect<string>
  >().type.not.toBeAssignableTo<Document.Migration>()
  expect<
    (_: Schema.JsonObject) => Effect.Effect<Schema.JsonObject, string>
  >().type.not.toBeAssignableTo<Document.Migration>()
})
