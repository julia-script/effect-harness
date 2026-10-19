import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Document from 'effect-harness/Document'
import type * as Record from 'effect-harness/Record'
import type * as Session from 'effect-harness/Session'
import * as Transaction from 'effect-harness/Transaction'

class Decode extends Context.Service<Decode, string>()('typetest/DocumentPatch/Decode') {}
class Encode extends Context.Service<Encode, string>()('typetest/DocumentPatch/Encode') {}
const codec = Schema.Struct({ stored: Schema.Natural }).pipe(
  Schema.decodeTo(Schema.Struct({ count: Schema.Natural, label: Schema.String }), {
    decode: SchemaGetter.transformEffect(({ stored }) =>
      Decode.pipe(Effect.map((label) => ({ count: stored, label }))),
    ),
    encode: SchemaGetter.transformEffect(({ count }) => Encode.pipe(Effect.as({ stored: count }))),
  }),
)
const document = Document.define({
  kind: 'typed.document.patch',
  version: 1,
  scope: 'session',
  schema: codec,
  initial: () => ({ count: 0, label: '' }),
  checkpointWhen: (value, operations, info) => {
    expect(value).type.toBe<{ readonly count: number; readonly label: string }>()
    expect(operations).type.toBe<ReadonlyArray<Record.Op>>()
    expect(info).type.toBe<{ readonly deltasSinceBase: number }>()
    return info.deltasSinceBase >= 2
  },
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target
declare const tx: Transaction.Transaction
const operations = [['set', ['stored'], 1]] satisfies ReadonlyArray<Record.Op>

test('both data-first forms retain transformed codec decoding and encoding services', () => {
  expect(Transaction.patchDocument(tx, document, target, operations)).type.toBe<
    Effect.Effect<void, Session.Failure, Decode | Encode>
  >()
  expect(tx.pipe(Transaction.patchDocument(document, target, operations))).type.toBe<
    Effect.Effect<void, Session.Failure, Decode | Encode>
  >()
  expect(
    Transaction.updateDocument(tx, document, target, (value) => ({ ...value, count: 2 })),
  ).type.toBe<Effect.Effect<void, Session.Failure, Decode | Encode>>()
  const patch = Transaction.patchDocument(tx, document, target, operations)
  expect(patch).type.not.toBeAssignableTo<Effect.Effect<void, Session.Failure>>()
  expect(patch.pipe(Effect.provideService(Decode, 'decode'))).type.toBe<
    Effect.Effect<void, Session.Failure, Encode>
  >()
  expect(
    patch.pipe(Effect.provideService(Decode, 'decode'), Effect.provideService(Encode, 'encode')),
  ).type.toBe<Effect.Effect<void, Session.Failure>>()
})

test('splice tuples carry nonempty encoded paths and compact inserted JSON items', () => {
  expect<
    readonly ['splice', readonly ['items'], number, number, readonly [number]]
  >().type.toBeAssignableTo<Record.Op>()
  expect<
    readonly ['splice', readonly [], number, number, readonly [number]]
  >().type.not.toBeAssignableTo<Record.Op>()
  expect<
    readonly ['splice', readonly ['items'], number, number, readonly [Date]]
  >().type.not.toBeAssignableTo<Record.Op>()
})
