/** Document policy and schema boundaries shared by Session and Transaction. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Document from '../Document.js'
import type * as DocumentMaterialization from '../DocumentMaterialization.js'
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
  stored: Pick<Record.StoredDocument, 'record' | 'version'>,
  allowMigration = false,
) {
  const definition = document.definition
  if (
    stored.record.kind !== definition.kind ||
    stored.record.scope._tag !== definition.scope ||
    (stored.version !== definition.version &&
      (!allowMigration ||
        stored.version >= definition.version ||
        definition.migrations?.[stored.version] === undefined)) ||
    (definition.scope === 'conversation' &&
      (stored.record.history !== definition.history || stored.record.fork !== definition.fork))
  )
    return yield* new SessionError({
      reason: 'conflict',
      operation: 'document.read',
      message: 'Persisted document version or policies differ from its definition',
    })
})

/** Prepares an upgrade without changing the overlay, including target-schema validation. */
export const migrate = Effect.fnUntraced(function* <S extends Document.Codec>(
  document: Document.Document<S>,
  stored: Record.StoredDocument,
) {
  yield* compatible(document, stored, true)
  if (stored.version === document.definition.version) return stored
  const migration = document.definition.migrations?.[stored.version]
  if (migration === undefined)
    return yield* new SessionError({
      reason: 'conflict',
      operation: 'document.migrate',
      message: 'Persisted document version has no direct migration',
    })
  const owned = yield* copy(Record.StoredDocumentSchema, stored)
  const result = yield* Effect.try({
    try: () => migration(owned.value),
    catch: (cause) =>
      new SessionError({
        reason: 'invalid',
        operation: 'document.migrate',
        message: 'Document migration callback failed',
        cause,
      }),
  })
  const value = yield* copy(Schema.JsonObject, yield* result)
  const migrated = { ...owned, version: document.definition.version, value, deltasSinceBase: 0 }
  yield* snapshot(document, migrated)
  return migrated
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

/** Read-side upgrades own both callback input and result without adopting either in storage. */
export const materialize = Effect.fnUntraced(function* <S extends Document.Codec>(
  document: Document.Document<S>,
  stored: Record.StoredDocument,
): Effect.fn.Return<DocumentMaterialization.Snapshot<S>, Failure, S['DecodingServices']> {
  yield* Schema.decodeEffect(Document.MetadataSchema)(document.definition)
  yield* compatible(document, stored, true)
  const owned = yield* copy(Record.StoredDocumentSchema, stored)
  let encoded = owned.value
  if (owned.version !== document.definition.version) {
    const migration = document.definition.migrations?.[owned.version]
    if (migration === undefined)
      return yield* new SessionError({
        reason: 'conflict',
        operation: 'document.materialize',
        message: 'Persisted document version has no direct migration',
      })
    const migrated = yield* Effect.try({
      try: () => migration(owned.value),
      catch: (cause) =>
        new SessionError({
          reason: 'invalid',
          operation: 'document.materialize',
          message: 'Document migration callback failed',
          cause,
        }),
    }).pipe(
      Effect.flatten,
      Effect.catchDefect((cause) =>
        Effect.fail(
          new SessionError({
            reason: 'invalid',
            operation: 'document.materialize',
            message: 'Document migration callback failed',
            cause,
          }),
        ),
      ),
    )
    encoded = yield* copy(Schema.JsonObject, migrated)
  }
  const value = yield* Schema.decodeEffect(document.definition.schema)(encoded)
  return {
    record: owned.record,
    storedVersion: owned.version,
    effectiveVersion: document.definition.version,
    value,
    deltasSinceBase: owned.deltasSinceBase,
  }
})

export const encode = Effect.fnUntraced(function* <S extends Document.Codec>(
  document: Document.Document<S>,
  value: S['Type'],
) {
  const encoded = yield* Schema.encodeEffect(document.definition.schema)(value)
  return yield* copy(Schema.JsonObject, encoded)
})
