/** Document policy and schema boundaries shared by Session and Transaction. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Document from '../Document.js'
import * as Record from '../Record.js'
import { SessionError, type Failure } from '../SessionError.js'

/** A schema round trip supplies an owned JSON representation without sharing caller objects. */
export const copy = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  schema: S,
  value: S['Type'],
) {
  const json = Schema.fromJsonString(schema)
  return yield* Schema.encodeEffect(json)(value).pipe(Effect.flatMap(Schema.decodeEffect(json)))
})

export const address = Effect.fnUntraced(function* <S extends Document.Codec>(
  document: Document.Document<S>,
  input: Document.Target,
): Effect.fn.Return<Record.DocumentAddress, Failure> {
  const definition = yield* Schema.decodeEffect(Document.MetadataSchema)(document.definition)
  const target = yield* copy(Document.TargetSchema, input)
  if (definition.scope !== target.scope._tag || document.family !== (target.key !== undefined))
    return yield* new SessionError({
      reason: 'invalid',
      operation: 'document.address',
      message: 'Document scope or family key does not match its definition',
    })
  return { kind: definition.kind, ...target }
})

const AddressJson = Schema.fromJsonString(Record.DocumentAddressSchema)
export const key = Schema.encodeEffect(AddressJson)

export const compatible = Effect.fnUntraced(function* <S extends Document.Codec>(
  document: Document.Document<S>,
  stored: Record.StoredDocument,
) {
  const definition = document.definition
  if (
    stored.record.kind !== definition.kind ||
    stored.record.scope._tag !== definition.scope ||
    stored.version !== definition.version ||
    (definition.scope === 'conversation' &&
      (stored.record.history !== definition.history || stored.record.fork !== definition.fork))
  )
    return yield* new SessionError({
      reason: 'conflict',
      operation: 'document.read',
      message: 'Persisted document version or policies differ from its definition',
    })
})

export const snapshot = Effect.fnUntraced(function* <S extends Document.Codec>(
  document: Document.Document<S>,
  stored: Record.StoredDocument,
) {
  yield* compatible(document, stored)
  const owned = yield* copy(Record.StoredDocumentSchema, stored)
  const value = yield* Schema.decodeEffect(document.definition.schema)(owned.value)
  return { ...owned, value }
})

export const encode = Effect.fnUntraced(function* <S extends Document.Codec>(
  document: Document.Document<S>,
  value: S['Type'],
) {
  const encoded = yield* Schema.encodeEffect(document.definition.schema)(value)
  return yield* copy(Schema.JsonObject, encoded)
})
