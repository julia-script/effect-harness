import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Option from 'effect/Option'
import type * as Storage from 'effect-harness/Storage'
import * as SchemaGetter from 'effect/SchemaGetter'
import * as Document from 'effect-harness/Document'
import * as DocumentRetirement from 'effect-harness/DocumentRetirement'
import type * as Record from 'effect-harness/Record'
import type * as Session from 'effect-harness/Session'
import * as Transaction from 'effect-harness/Transaction'

class OldCodec extends Context.Service<OldCodec, string>()('typetest/obsolete/OldCodec') {}
const encoded = Schema.Struct({ count: Schema.Natural })
const codec = encoded.pipe(
  Schema.decodeTo(Schema.Struct({ count: Schema.Natural, label: Schema.String }), {
    decode: SchemaGetter.transformEffect(({ count }) =>
      OldCodec.pipe(Effect.map((label) => ({ count, label }))),
    ),
    encode: SchemaGetter.transformEffect(({ count }) => OldCodec.pipe(Effect.as({ count }))),
  }),
)
const obsolete = Document.define({
  kind: 'typed.obsolete',
  version: 3,
  scope: 'session',
  schema: codec,
  initial: () => ({ count: 0, label: '' }),
})
declare const tx: Transaction.Transaction
declare const raw: Record.StoredDocument
const target = DocumentRetirement.target(raw)

test('metadata retirement needs neither the obsolete decoder nor encoder service', () => {
  expect(target).type.toBe<DocumentRetirement.Target>()
  expect(Transaction.retireDocumentMetadata(tx, obsolete.definition, target)).type.toBe<
    Effect.Effect<void, Session.Failure>
  >()
  expect(tx.pipe(Transaction.retireDocumentMetadata(obsolete.definition, target))).type.toBe<
    Effect.Effect<void, Session.Failure>
  >()
  expect<typeof DocumentRetirement.TargetSchema.DecodingServices>().type.toBe<never>()
  expect(
    Transaction.snapshot(tx, obsolete, { scope: { _tag: 'session' } }),
  ).type.not.toBeAssignableTo<Effect.Effect<unknown, Session.Failure>>()
  expect(
    Transaction.ensureDocument(tx, obsolete, { scope: { _tag: 'session' } }),
  ).type.not.toBeAssignableTo<Effect.Effect<unknown, Session.Failure>>()
  expect(
    Transaction.retireDocumentMetadata(
      tx,
      { kind: 'typed.obsolete', scope: 'session', version: 3 },
      target,
    ),
  ).type.toBe<Effect.Effect<void, Session.Failure>>()
})

test('retirement targets require an incarnation and address, not old values', () => {
  expect<{ readonly record: Record.DocumentRecord }>().type.toBeAssignableTo<
    Parameters<typeof DocumentRetirement.target>[0]
  >()
  expect<{
    readonly address: Record.DocumentAddress
  }>().type.not.toBeAssignableTo<DocumentRetirement.Target>()
  expect<{
    readonly id: Record.DocumentId
  }>().type.not.toBeAssignableTo<DocumentRetirement.Target>()
})

declare const storage: Storage.Storage['Service']
test('fresh native metadata remains optional and needs no obsolete codec service', () => {
  const lookup = storage.documentMetadata
  expect(lookup).type.toBe<
    | ((
        id: Record.DocumentId,
      ) => Effect.Effect<Option.Option<Storage.DocumentMetadata>, Storage.StorageError>)
    | undefined
  >()
  if (lookup !== undefined)
    expect(lookup(target.id)).type.toBe<
      Effect.Effect<Option.Option<Storage.DocumentMetadata>, Storage.StorageError>
    >()
})
