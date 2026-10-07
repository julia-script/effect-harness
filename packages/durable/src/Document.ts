/**
 * Validated document definitions, scoped draft values and detached snapshots.
 *
 * @since 0.0.0
 */
import { dual } from 'effect/Function'
import * as handle from './internal/handle.ts'
const DefinitionProto = handle.prototype('@effect-harness/durable/Document/Definition')
const DocumentProto = handle.prototype('@effect-harness/durable/Document/Document')
const SnapshotProto = handle.prototype('@effect-harness/durable/Document/Snapshot')
import type * as Pipeable from 'effect/Pipeable'
import type * as Inspectable from 'effect/Inspectable'
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
} from './storage/internal/state.ts'
/**
 * Canonical document clone failure.
 *
 * @category errors
 * @since 0.0.0
 */
export { CloneError } from './storage/internal/state.ts'

const DefinitionTypeId = '~@effect-harness/durable/Document/Definition'
const TypeId = '~@effect-harness/durable/Document'
const SnapshotTypeId = '~@effect-harness/durable/Document/Snapshot'
const MigrationCacheTypeId = '~@effect-harness/durable/Document/MigrationCache'
/**
 * Compatibility alias for Document.Definition.
 *
 * @category models
 * @since 0.0.0
 */
export type Definition<T extends object> = Document.Definition<T>
/**
 * Compatibility alias for Document.DefinitionInput.
 *
 * @category models
 * @since 0.0.0
 */
export type DefinitionInput<T extends object> = Document.DefinitionInput<T>
/**
 * Document contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface Document<in out T extends object>
  extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [TypeId]: { readonly _T: Types.Invariant<T> }
  readonly definition: Definition<T>
  readonly family: boolean
}
/**
 * Returns whether the value satisfies Document.
 *
 * @category guards
 * @since 0.0.0
 */
export const isDocument = (input: unknown): input is Document<object> =>
  Predicate.hasProperty(input, TypeId)
const construct = <T extends object>(input: DefinitionInput<T>, family: boolean): Document<T> => {
  const definition = handle.make(DefinitionProto, {
    ...input,
    [DefinitionTypeId]: { _T: identity },
  })
  Object.defineProperty(definition, DefinitionTypeId, { enumerable: false })
  const token = handle.make(DocumentProto, { definition, family, [TypeId]: { _T: identity } })
  return Object.defineProperty(token, TypeId, { enumerable: false })
}
/**
 * DocumentDefinitionError schema.
 *
 * @category errors
 * @since 0.0.0
 */
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
/**
 * Validates a document definition and creates its singleton token.
 *
 * @category constructors
 * @since 0.0.0
 */
export const define = <T extends object>(
  definition: DefinitionInput<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => construct(definition, false))
/**
 * Validates a document definition and creates its keyed token factory.
 *
 * @category constructors
 * @since 0.0.0
 */
export const family = <T extends object>(
  definition: DefinitionInput<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => construct(definition, true))
/**
 * Creates a singleton document token or throws for an invalid definition.
 *
 * @category constructors
 * @since 0.0.0
 */
export const defineUnsafe = <T extends object>(definition: DefinitionInput<T>): Document<T> =>
  Result.getOrThrow(define(definition))
/**
 * Creates a keyed document token factory or throws for an invalid definition.
 *
 * @category constructors
 * @since 0.0.0
 */
export const familyUnsafe = <T extends object>(definition: DefinitionInput<T>): Document<T> =>
  Result.getOrThrow(family(definition))
/**
 * Ownership traversal target constructors.
 *
 * @category models
 * @since 0.0.0
 */
export type Target = Document.Target
/**
 * Compatibility alias for Document.Snapshot.
 *
 * @category models
 * @since 0.0.0
 */
export type Snapshot<T extends object = Record.JsonObject> = Document.Snapshot<T>

/**
 * Compatibility alias for Document.SnapshotInput.
 *
 * @category models
 * @since 0.0.0
 */
export type SnapshotInput<T extends object = Record.JsonObject> = Document.SnapshotInput<T>
/**
 * Creates a detached snapshot carrier.
 *
 * @category constructors
 * @since 0.0.0
 */
export const makeSnapshot = <T extends object>(input: SnapshotInput<T>): Snapshot<T> => {
  const value = handle.make(SnapshotProto, handle.marked(input, SnapshotTypeId, { _T: identity }))
  return Object.defineProperty(value, SnapshotTypeId, { enumerable: false })
}

const addressImpl = Effect.fnUntraced(function* <T extends object>(
  self: Document<T>,
  target: Target = {},
): Effect.fn.Return<Record.Address, StorageError> {
  const definition = self.definition
  if (
    !Number.isSafeInteger(definition.version) ||
    definition.version < 1 ||
    definition.kind.length === 0
  )
    return yield* rejected('Invalid document definition')
  if (self.family !== (target.key !== undefined))
    return yield* rejected('Document family requires an explicit key; singleton excludes it')
  let scope: Record.Scope
  if (definition.scope === 'session') scope = { _tag: 'session', kind: 'session' }
  else if (definition.scope === 'conversation')
    scope = {
      _tag: 'conversation',
      kind: 'conversation',
      conversationId: yield* validate(Record.ConversationId, target.owner),
    }
  else scope = { _tag: 'task', kind: 'task', taskId: yield* validate(Record.TaskId, target.owner) }
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

/**
 * MigrationCache contract.
 *
 * @category models
 * @since 0.0.0
 */
export interface MigrationCache {
  readonly [MigrationCacheTypeId]: typeof MigrationCacheTypeId
  readonly values: WeakMap<object, Cache.Cache<string, Record.JsonObject, StorageError>>
  readonly permit: Semaphore.Semaphore
}
/**
 * Returns whether the value satisfies MigrationCache.
 *
 * @category guards
 * @since 0.0.0
 */
export const isMigrationCache = (input: unknown): input is MigrationCache =>
  Predicate.hasProperty(input, MigrationCacheTypeId)
/**
 * Creates a token-local migration cache with captured decoding services.
 *
 * @category constructors
 * @since 0.0.0
 */
export const makeMigrationCache: Effect.Effect<MigrationCache> = Effect.gen(function* () {
  const cache: MigrationCache = {
    [MigrationCacheTypeId]: MigrationCacheTypeId,
    values: new WeakMap(),
    permit: yield* Semaphore.make(1),
  }
  return Object.defineProperty(cache, MigrationCacheTypeId, { enumerable: false })
})
/**
 * Compatibility alias for Document.Draft.
 *
 * @category models
 * @since 0.0.0
 */
export type Draft<T> = Document.Draft<T>

const typedImpl = Effect.fnUntraced(function* <T extends object>(
  self: Document<T>,
  snapshot: Snapshot,
  cache?: MigrationCache,
): Effect.fn.Return<Snapshot<T>, StorageError> {
  const definition = self.definition
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
      return yield* detachedEffect(yield* encode(self, domain))
    })
    if (cache === undefined) value = yield* migrate(key)
    else {
      const memo = yield* cache.permit.withPermit(
        Effect.gen(function* () {
          const previous = Option.fromUndefinedOr(cache.values.get(self))
          if (Option.isSome(previous)) return previous.value
          // Native Cache shares concurrent lookup fibers and captures schema services at construction.
          // Success survives indefinitely; failures expire immediately so later reads can retry.
          const memo = yield* Cache.makeWith(migrate, {
            capacity: Infinity,
            timeToLive: (exit) => (Exit.isSuccess(exit) ? Duration.infinity : Duration.zero),
            requireServicesAt: 'construction',
          })
          cache.values.set(self, memo)
          return memo
        }),
      )
      value = yield* detachedEffect(yield* Cache.get(memo, key))
    }
  }
  const decoded = yield* validate(definition.schema, value)
  yield* encode(self, decoded)
  return makeSnapshot({
    ...snapshot,
    version: definition.version,
    value: yield* detachedEffect(decoded),
  })
})

/**
 * Canonical domain schemas may decode undefined-friendly fields; their storage form remains an object.
 *
 * @category combinators
 * @since 0.0.0
 */
export const jsonObjectCodec: typeof Serialization.object = Serialization.object

/**
 * Encodes the decoded domain model into the separately validated JSON storage representation.
 *
 * @category schemas
 * @since 0.0.0
 */
export const encode = <T extends object>(
  token: Document<T>,
  value: T,
): Effect.Effect<Record.JsonObject, StorageError> =>
  Schema.encodeEffect(token.definition.schema)(value).pipe(
    Effect.mapError((cause) => rejected('Document cannot be encoded', Invalid, cause)),
    Effect.flatMap((encoded) => validate(Schema.JsonObject, encoded)),
  )

/**
 * Detaches validated JSON values into mutable data while preserving primitive brands.
 *
 * @category combinators
 * @since 0.0.0
 */
export const copy = <T>(self: T): Result.Result<Draft<T>, CloneError> =>
  Result.map(detached(self), (self) => self as Draft<T>)
/**
 * Synchronous copy for validated static data or documented synchronous callbacks.
 *
 * @category combinators
 * @since 0.0.0
 */
export const copyUnsafe = <T>(self: T): Draft<T> => detachedUnsafe(self) as Draft<T>
/**
 * Copies active drafts into the typed storage channel inside Effect transactions.
 *
 * @category combinators
 * @since 0.0.0
 */
export const copyEffect = <T>(self: T): Effect.Effect<Draft<T>, StorageError> =>
  detachedEffect(self).pipe(Effect.map((self) => self as Draft<T>))

/**
 * Resolves a document token and target into its durable address.
 *
 * @category combinators
 * @since 0.0.0
 */
export const address: {
  (
    target?: Target,
  ): <T extends object>(self: Document<T>) => Effect.Effect<Record.Address, StorageError>
  <T extends object>(
    self: Document<T>,
    target?: Target,
  ): Effect.Effect<Record.Address, StorageError>
} = dual((args) => isDocument(args[0]), addressImpl)

/**
 * Decodes and detaches a stored snapshot through its document definition.
 *
 * @category combinators
 * @since 0.0.0
 */
export const typed: {
  (
    snapshot: Snapshot,
    cache?: MigrationCache,
  ): <T extends object>(self: Document<T>) => Effect.Effect<Snapshot<T>, StorageError>
  <T extends object>(
    self: Document<T>,
    snapshot: Snapshot,
    cache?: MigrationCache,
  ): Effect.Effect<Snapshot<T>, StorageError>
} = dual((args) => isDocument(args[0]), typedImpl)

/**
 * Document contract.
 *
 * @category models
 * @since 0.0.0
 */
export declare namespace Document {
  /**
   * Definition contract.
   *
   * @category models
   * @since 0.0.0
   */
  export interface Definition<in out T extends object>
    extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly [DefinitionTypeId]: { readonly _T: Types.Invariant<T> }
    readonly kind: string
    readonly version: number
    readonly scope: Record.Scope['kind']
    readonly history?: 'latest' | 'rewindable' | undefined
    readonly fork?: 'asOf' | 'current' | 'initial' | undefined
    readonly schema: Schema.Codec<T, Record.JsonObject>
    /**
     * Returns the initial decoded value.
     *
     * **Details**
     *
     * Seed decoding may throw. Session.transaction maps initializer failures to
     * rejected StorageError before commit.
     */
    readonly initial: (seed?: Record.Json) => T
    readonly migrate?: ((value: Record.JsonObject, fromVersion: number) => T) | undefined
    readonly checkpointWhen?:
      | ((
          value: Readonly<T>,
          ops: ReadonlyArray<Record.Op>,
          info: { readonly deltasSinceBase: number },
        ) => boolean)
      | undefined
  }
  /**
   * DefinitionInput contract.
   *
   * @category models
   * @since 0.0.0
   */
  export type DefinitionInput<T extends object> = handle.Input<
    Definition<T>,
    typeof DefinitionTypeId
  >
  /**
   * Ownership traversal target constructors.
   *
   * @category models
   * @since 0.0.0
   */
  export interface Target {
    readonly owner?: Record.ConversationId | Record.TaskId | undefined
    readonly key?: string | undefined
    readonly seed?: Record.Json | undefined
  }
  /**
   * Snapshot contract.
   *
   * @category models
   * @since 0.0.0
   */
  export interface Snapshot<out T extends object = Record.JsonObject>
    extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly [SnapshotTypeId]: { readonly _T: Types.Covariant<T> }
    readonly record: Record.Document
    readonly version: number
    readonly value: Readonly<T>
    readonly deltasSinceBase: number
  }
  /**
   * SnapshotInput contract.
   *
   * @category models
   * @since 0.0.0
   */
  export type SnapshotInput<T extends object = Record.JsonObject> = Omit<
    Snapshot<T>,
    typeof SnapshotTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >
  /**
   * Draft contract.
   *
   * @category models
   * @since 0.0.0
   */
  export type Draft<T> = T extends string | number | boolean | null | undefined
    ? T
    : T extends ReadonlyArray<infer A>
      ? Array<Draft<A>>
      : T extends object
        ? { -readonly [K in keyof T]: Draft<T[K]> }
        : T
}
