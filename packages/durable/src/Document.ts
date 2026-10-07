import * as Serialization from './Serialization.ts'
import { identity } from 'effect/Function'
import * as Predicate from 'effect/Predicate'
import type * as Types from 'effect/Types'
import * as Cache from 'effect/Cache'
import * as Duration from 'effect/Duration'
import * as Exit from 'effect/Exit'
import * as Option from 'effect/Option'
import * as Semaphore from 'effect/Semaphore'
import * as Schema from 'effect/Schema'
import * as Record from './Record.ts'
import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import { rejected, StorageError, Invalid } from './StorageError.ts'
import {
  detached,
  detachedUnsafe,
  detachedEffect,
  validate,
  type CloneError,
} from './storage/State.ts'
export { CloneError } from './storage/State.ts'

const DefinitionTypeId = '~@effect-harness/durable/Document/Definition'
const TypeId = '~@effect-harness/durable/Document'
const SnapshotTypeId = '~@effect-harness/durable/Document/Snapshot'
const MigrationCacheTypeId = '~@effect-harness/durable/Document/MigrationCache'
export interface Definition<in out T extends object> {
  readonly [DefinitionTypeId]: { readonly _T: Types.Invariant<T> }
  readonly kind: string
  readonly version: number
  readonly scope: Record.Scope['kind']
  readonly history?: 'latest' | 'rewindable'
  readonly fork?: 'asOf' | 'current' | 'initial'
  readonly schema: Schema.Codec<T, Record.JsonObject>
  readonly initial: (seed?: Record.Json) => T
  readonly migrate?: (value: Record.JsonObject, fromVersion: number) => T
  readonly checkpointWhen?: (
    value: Readonly<T>,
    ops: ReadonlyArray<Record.Op>,
    info: { readonly deltasSinceBase: number },
  ) => boolean
}
export type DefinitionInput<T extends object> = Omit<Definition<T>, typeof DefinitionTypeId>
export interface Document<in out T extends object> {
  readonly [TypeId]: { readonly _T: Types.Invariant<T> }
  readonly definition: Definition<T>
  readonly family: boolean
}
export const isDocument = (input: unknown): input is Document<object> =>
  Predicate.hasProperty(input, TypeId)
const construct = <T extends object>(input: DefinitionInput<T>, family: boolean): Document<T> => {
  const definition: Definition<T> = { ...input, [DefinitionTypeId]: { _T: identity } }
  Object.defineProperty(definition, DefinitionTypeId, { enumerable: false })
  const token: Document<T> = { definition, family, [TypeId]: { _T: identity } }
  return Object.defineProperty(token, TypeId, { enumerable: false })
}
export class DocumentDefinitionError extends Schema.TaggedError<DocumentDefinitionError>(
  '@effect-harness/durable/Document/DocumentDefinitionError',
)('DocumentDefinitionError', { message: Schema.String }) {}
const checkDefinition = <T extends object>(
  definition: DefinitionInput<T>,
): Result.Result<void, DocumentDefinitionError> => {
  if (
    definition.kind.length === 0 ||
    !Number.isSafeInteger(definition.version) ||
    definition.version < 1
  )
    return Result.fail(
      new DocumentDefinitionError({
        message: 'Document kind/version must be nonempty and a positive safe integer',
      }),
    )
  if (definition.scope === 'conversation') {
    if (
      definition.history === undefined ||
      definition.fork === undefined ||
      (definition.history === 'latest' && definition.fork === 'asOf')
    )
      return Result.fail(
        new DocumentDefinitionError({
          message: 'Conversation documents require compatible history/fork policies',
        }),
      )
  } else if (definition.history !== undefined || definition.fork !== undefined)
    return Result.fail(
      new DocumentDefinitionError({
        message: 'Only conversation documents specify history/fork policies',
      }),
    )
  return Result.succeed(undefined)
}
export const define = <T extends object>(
  definition: DefinitionInput<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => construct(definition, false))
export const family = <T extends object>(
  definition: DefinitionInput<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => construct(definition, true))
export const defineUnsafe = <T extends object>(definition: DefinitionInput<T>): Document<T> =>
  Result.getOrThrow(define(definition))
export const familyUnsafe = <T extends object>(definition: DefinitionInput<T>): Document<T> =>
  Result.getOrThrow(family(definition))
export interface Target {
  readonly owner?: Record.ConversationId | Record.TaskId
  readonly key?: string
  readonly seed?: Record.Json
}
export interface Snapshot<out T extends object = Record.JsonObject> {
  readonly [SnapshotTypeId]: { readonly _T: Types.Covariant<T> }
  readonly record: Record.Document
  readonly version: number
  readonly value: Readonly<T>
  readonly deltasSinceBase: number
}

export type SnapshotInput<T extends object = Record.JsonObject> = Omit<
  Snapshot<T>,
  typeof SnapshotTypeId
>
export const makeSnapshot = <T extends object>(input: SnapshotInput<T>): Snapshot<T> => {
  const value: Snapshot<T> = { ...input, [SnapshotTypeId]: { _T: identity } }
  return Object.defineProperty(value, SnapshotTypeId, { enumerable: false })
}

export const address = Effect.fnUntraced(function* <T extends object>(
  token: Document<T>,
  target: Target = {},
): Effect.fn.Return<Record.Address, StorageError> {
  const definition = token.definition
  if (
    !Number.isSafeInteger(definition.version) ||
    definition.version < 1 ||
    definition.kind.length === 0
  )
    return yield* rejected('Invalid document definition')
  if (token.family !== (target.key !== undefined))
    return yield* rejected('Document family requires an explicit key; singleton excludes it')
  let scope: Record.Scope
  if (definition.scope === 'session') scope = { kind: 'session' }
  else if (definition.scope === 'conversation')
    scope = {
      kind: 'conversation',
      conversationId: yield* validate(Record.ConversationId, target.owner),
    }
  else scope = { kind: 'task', taskId: yield* validate(Record.TaskId, target.owner) }
  if (definition.scope === 'conversation') {
    if (
      definition.history === undefined ||
      definition.fork === undefined ||
      (definition.history === 'latest' && definition.fork === 'asOf')
    )
      return yield* rejected('Invalid conversation document semantics')
  } else if (definition.history !== undefined || definition.fork !== undefined)
    return yield* rejected('Only conversation documents specify history and fork')
  return { kind: definition.kind, scope, ...(target.key === undefined ? {} : { key: target.key }) }
})

export interface MigrationCache {
  readonly [MigrationCacheTypeId]: typeof MigrationCacheTypeId
  readonly values: WeakMap<object, Cache.Cache<string, Record.JsonObject, StorageError>>
  readonly permit: Semaphore.Semaphore
}
export const isMigrationCache = (input: unknown): input is MigrationCache =>
  Predicate.hasProperty(input, MigrationCacheTypeId)
export const makeMigrationCache: Effect.Effect<MigrationCache> = Effect.gen(function* () {
  const cache: MigrationCache = {
    [MigrationCacheTypeId]: MigrationCacheTypeId,
    values: new WeakMap(),
    permit: yield* Semaphore.make(1),
  }
  return Object.defineProperty(cache, MigrationCacheTypeId, { enumerable: false })
})
export type Draft<T> = T extends string | number | boolean | null | undefined
  ? T
  : T extends ReadonlyArray<infer A>
    ? Array<Draft<A>>
    : T extends object
      ? { -readonly [K in keyof T]: Draft<T[K]> }
      : T

export const typed = Effect.fnUntraced(function* <T extends object>(
  token: Document<T>,
  snapshot: Snapshot,
  cache?: MigrationCache,
): Effect.fn.Return<Snapshot<T>, StorageError> {
  const definition = token.definition
  if (
    snapshot.record.scope.kind !== definition.scope ||
    snapshot.record.history !== definition.history ||
    snapshot.record.fork !== definition.fork
  )
    return yield* rejected('Document token semantics differ from persisted incarnation')
  if (
    snapshot.version > definition.version ||
    (snapshot.version < definition.version && definition.migrate === undefined)
  )
    return yield* rejected('Document stored version is incompatible')
  let value = snapshot.value
  if (snapshot.version < definition.version && definition.migrate !== undefined) {
    const key = JSON.stringify([snapshot.record.id, snapshot.version, snapshot.value])
    const migrate = Effect.fnUntraced(function* (key: string) {
      const [_id, version, storedValue] = yield* Schema.decodeEffect(
        Schema.fromJsonString(Schema.Tuple([Record.DocumentId, Schema.Int, Schema.JsonObject])),
      )(key).pipe(
        Effect.mapError((cause) => rejected('Invalid migration cache snapshot', Invalid, cause)),
      )
      const input = yield* detachedEffect(storedValue)
      const migrated = yield* Effect.try({
        try: () => definition.migrate?.(input, version) ?? storedValue,
        catch: (cause) => rejected('Document migration failed', Invalid, cause),
      })
      const domain = yield* validate(
        Schema.toType(definition.schema),
        yield* detachedEffect(migrated),
      )
      return yield* detachedEffect(yield* encode(token, domain))
    })
    if (cache === undefined) value = yield* migrate(key)
    else {
      const memo = yield* cache.permit.withPermit(
        Effect.gen(function* () {
          const previous = Option.fromUndefinedOr(cache.values.get(token))
          if (Option.isSome(previous)) return previous.value
          // Native Cache shares concurrent lookup fibers and captures schema services at construction.
          // Success survives indefinitely; failures expire immediately so later reads can retry.
          const memo = yield* Cache.makeWith(migrate, {
            capacity: Infinity,
            timeToLive: (exit) => (Exit.isSuccess(exit) ? Duration.infinity : Duration.zero),
            requireServicesAt: 'construction',
          })
          cache.values.set(token, memo)
          return memo
        }),
      )
      value = yield* detachedEffect(yield* Cache.get(memo, key))
    }
  }
  const decoded = yield* validate(definition.schema, value)
  yield* encode(token, decoded)
  return makeSnapshot({
    ...snapshot,
    version: definition.version,
    value: yield* detachedEffect(decoded),
  })
})

/** Canonical domain schemas may decode undefined-friendly fields; their storage form remains an object. */
export const jsonObjectCodec: typeof Serialization.object = Serialization.object

/** Encode the decoded domain model into the separately validated JSON storage representation. */
export const encode = <T extends object>(
  token: Document<T>,
  value: T,
): Effect.Effect<Record.JsonObject, StorageError> =>
  Schema.encodeEffect(token.definition.schema)(value).pipe(
    Effect.mapError((cause) => rejected('Document cannot be encoded', Invalid, cause)),
    Effect.flatMap((encoded) => validate(Schema.JsonObject, encoded)),
  )

/** Detaches validated JSON values into mutable data while preserving primitive brands. */
export const copy = <T>(value: T): Result.Result<Draft<T>, CloneError> =>
  Result.map(detached(value), (value) => value as Draft<T>)
/** Synchronous copy for validated static data or documented synchronous callbacks. */
export const copyUnsafe = <T>(value: T): Draft<T> => detachedUnsafe(value) as Draft<T>
/** Copies active drafts into the typed storage channel inside Effect transactions. */
export const copyEffect = <T>(value: T): Effect.Effect<Draft<T>, StorageError> =>
  detachedEffect(value).pipe(Effect.map((value) => value as Draft<T>))
