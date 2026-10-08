/**
 * Validated document definitions, scoped draft values and detached snapshots.
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
 * Public APIs from `./storage/internal/state.ts`.
 *
 * @category re-exports
 */
export { CloneError } from './storage/internal/state.ts'

const DefinitionTypeId = '~@effect-harness/durable/Document/Definition'
const TypeId = '~@effect-harness/durable/Document'
const SnapshotTypeId = '~@effect-harness/durable/Document/Snapshot'
const MigrationCacheTypeId = '~@effect-harness/durable/Document/MigrationCache'
/**
 * Schema, initialization, migration and history policies for a document kind.
 *
 * @category models
 */
export type Definition<T extends object> = Document.Definition<T>
/**
 * Document definition supplied before the library adds its nominal identity.
 *
 * @category models
 */
export type DefinitionInput<T extends object> = Document.DefinitionInput<T>
/**
 * Typed token identifying a singleton or keyed document family.
 *
 * **Details**
 *
 * The token carries schema and lifecycle policy; it is not the document value. Acquire a
 * draft in a Session transaction, or read a detached committed snapshot.
 *
 * **Gotchas**
 *
 * isDocument checks the token’s nominal identity, not arbitrary stored document data.
 *
 * @category models
 */
export interface Document<in out T extends object>
  extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [TypeId]: { readonly _T: Types.Invariant<T> }
  readonly definition: Definition<T>
  readonly family: boolean
}
/**
 * Checks whether a value carries the nominal `Document` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
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
 * Failure reporting an invalid document kind, version or history/fork combination.
 *
 * @category errors
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
 * Validates a document definition and creates a singleton token.
 *
 * **When to use**
 *
 * Use when one value of a kind belongs to each selected session, conversation or task.
 *
 * **Details**
 *
 * kind must be nonempty and version a positive safe integer. Conversation definitions
 * declare history and fork policies; session/task definitions omit them.
 *
 * **Gotchas**
 *
 * An asOf fork policy requires rewindable history. Invalid definitions return
 * DocumentDefinitionError rather than throwing.
 *
 * @see {@link family} for multiple keyed values of one document kind.
 * @category constructors
 */
export const define = <T extends object>(
  definition: DefinitionInput<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => construct(definition, false))
/**
 * Validates a document definition and creates a keyed-family token.
 *
 * **When to use**
 *
 * Use when an owner needs independently addressed values under one document kind.
 *
 * **Gotchas**
 *
 * Supply an explicit target key on every acquisition or read. Singleton tokens exclude keys.
 * Definition validation is the same as define.
 *
 * @see {@link define} for one value per owner.
 * @category constructors
 */
export const family = <T extends object>(
  definition: DefinitionInput<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => construct(definition, true))
/**
 * Creates a singleton token or throws for an invalid definition.
 *
 * **When to use**
 *
 * Use when static definitions are controlled by the application.
 *
 * **Gotchas**
 *
 * Throws DocumentDefinitionError synchronously; use define when the definition comes from
 * input.
 *
 * @see {@link define} for validation as a Result.
 * @category constructors
 */
export const defineUnsafe = <T extends object>(definition: DefinitionInput<T>): Document<T> =>
  Result.getOrThrow(define(definition))
/**
 * Creates a family token or throws for an invalid definition.
 *
 * **Gotchas**
 *
 * Definition errors throw synchronously. Every document in the family still requires a
 * target key.
 *
 * @see {@link family} for validation as a Result.
 * @category constructors
 */
export const familyUnsafe = <T extends object>(definition: DefinitionInput<T>): Document<T> =>
  Result.getOrThrow(family(definition))
/**
 * Ownership traversal target constructors.
 *
 * @category models
 */
export type Target = Document.Target
/**
 * Detached document record and decoded value at a stored revision.
 *
 * @category models
 */
export type Snapshot<T extends object = Record.JsonObject> = Document.Snapshot<T>

/**
 * Fields supplied when constructing a nominal document snapshot.
 *
 * @category models
 */
export type SnapshotInput<T extends object = Record.JsonObject> = Document.SnapshotInput<T>
/**
 * Creates a nominal document-snapshot carrier from supplied fields.
 *
 * **Details**
 *
 * Preserves the supplied value and metadata; the constructor does not validate or deep-copy
 * them.
 *
 * **Gotchas**
 *
 * Detach mutable data before constructing a snapshot when it must outlive a draft.
 *
 * @category constructors
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
 * Token-local cache of decoded document migrations.
 *
 * @category models
 */
export interface MigrationCache {
  readonly [MigrationCacheTypeId]: typeof MigrationCacheTypeId
  readonly values: WeakMap<object, Cache.Cache<string, Record.JsonObject, StorageError>>
  readonly permit: Semaphore.Semaphore
}
/**
 * Checks whether a value carries the nominal `MigrationCache` marker.
 *
 * **Gotchas**
 *
 * This checks library identity, not the validity of arbitrary fields or stored JSON.
 *
 * @category guards
 */
export const isMigrationCache = (input: unknown): input is MigrationCache =>
  Predicate.hasProperty(input, MigrationCacheTypeId)
/**
 * Creates a token-local migration cache with captured decoding services.
 *
 * @category constructors
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
 * Recursively mutable view of decoded document data.
 *
 * @category models
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
 */
export const jsonObjectCodec: typeof Serialization.object = Serialization.object

/**
 * Encodes the decoded domain model into the separately validated JSON storage representation.
 *
 * @category schemas
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
 * Returns detached mutable JSON data while preserving primitive brands.
 *
 * **When to use**
 *
 * Use when data must outlive a transaction draft or be edited without changing it.
 *
 * **Gotchas**
 *
 * Copy a draft before its transaction ends. Unsupported or cyclic data returns CloneError;
 * the original value is unchanged.
 *
 * @see {@link copyEffect} for the StorageError channel inside an Effect.
 * @category combinators
 */
export const copy = <T>(self: T): Result.Result<Draft<T>, CloneError> =>
  Result.map(detached(self), (self) => self as Draft<T>)
/**
 * Copies JSON data or throws when it cannot be detached.
 *
 * **Gotchas**
 *
 * CloneError is thrown synchronously. Copy active drafts before their transaction ends.
 *
 * @see {@link copy} for a Result-based alternative.
 * @category combinators
 */
export const copyUnsafe = <T>(self: T): Draft<T> => detachedUnsafe(self) as Draft<T>
/**
 * Copies active drafts into the typed storage channel inside Effect transactions.
 *
 * @category combinators
 */
export const copyEffect = <T>(self: T): Effect.Effect<Draft<T>, StorageError> =>
  detachedEffect(self).pipe(Effect.map((self) => self as Draft<T>))

/**
 * Resolves a document token and target into its durable address.
 *
 * @category combinators
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
 * Decodes a stored snapshot using a document token and its migrations.
 *
 * **Details**
 *
 * Returns detached decoded data and may use a token-local MigrationCache. Older values are
 * projected through the declared migration; this read does not write the upgraded version.
 *
 * **Gotchas**
 *
 * Incompatible definitions, newer stored versions and failed migrations produce
 * StorageError.
 *
 * @category combinators
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
 * Type-level contracts for `Document`.
 *
 * @category utility types
 */
export declare namespace Document {
  /**
   * Schema and lifecycle policy for a document kind.
   *
   * **Details**
   *
   * schema encodes decoded values as JSON objects. initial supplies new values; migrate
   * upgrades older stored versions. Conversation history is latest or rewindable, and fork is
   * asOf, current or initial.
   *
   * **Gotchas**
   *
   * Migration is a pure value transformation. Newer stored versions are rejected. asOf
   * requires rewindable history; task and session documents do not accept conversation
   * history/fork policies.
   *
   * @category models
   */
  export interface Definition<in out T extends object>
    extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly [DefinitionTypeId]: { readonly _T: Types.Invariant<T> }
    /**
     * Stable nonempty document-kind name used in its logical address.
     */
    readonly kind: string
    /**
     * Positive safe-integer schema version used to select migrations.
     */
    readonly version: number
    /**
     * Ownership scope determining whether an owner identity is required.
     */
    readonly scope: Record.Scope['kind']
    /**
     * Conversation history policy: latest keeps present content; rewindable supports historical
     * cutoffs.
     */
    readonly history?: 'latest' | 'rewindable' | undefined
    /**
     * Conversation fork policy: asOf inherits the cutoff, current copies present content,
     * initial starts fresh.
     */
    readonly fork?: 'asOf' | 'current' | 'initial' | undefined
    /**
     * Codec between decoded document data and its JSON-object storage form.
     */
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
    /**
     * Pure transformation from an older stored version to the current decoded model.
     */
    readonly migrate?: ((value: Record.JsonObject, fromVersion: number) => T) | undefined
    /**
     * Selects when staged operations should be saved as a full checkpoint instead of another
     * delta.
     */
    readonly checkpointWhen?:
      | ((
          value: Readonly<T>,
          ops: ReadonlyArray<Record.Op>,
          info: { readonly deltasSinceBase: number },
        ) => boolean)
      | undefined
  }
  /**
   * Document definition supplied before the library adds its nominal identity.
   *
   * @category models
   */
  export type DefinitionInput<T extends object> = handle.Input<
    Definition<T>,
    typeof DefinitionTypeId
  >
  /**
   * Owner, family key and optional seed for document acquisition.
   *
   * **Details**
   *
   * owner selects the conversation or task for those scopes; session documents need no owner.
   * seed is passed to initial only when creating a value.
   *
   * **Gotchas**
   *
   * Families require key; singletons reject it. Recreating a retired address creates a new
   * incarnation rather than reviving old watches.
   *
   * @category models
   */
  export interface Target {
    /**
     * Conversation or task identity required by the document scope.
     */
    readonly owner?: Record.ConversationId | Record.TaskId | undefined
    /**
     * Explicit family key; required for family tokens and excluded for singleton tokens.
     */
    readonly key?: string | undefined
    /**
     * JSON initialization input used only when creating a document incarnation.
     */
    readonly seed?: Record.Json | undefined
  }
  /**
   * Detached decoded value and metadata at a document revision.
   *
   * **Details**
   *
   * version is the decoded schema version. deltasSinceBase counts retained deltas since the
   * checkpoint. Reading a migrated value does not itself persist the migration.
   *
   * @category models
   */
  export interface Snapshot<out T extends object = Record.JsonObject>
    extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly [SnapshotTypeId]: { readonly _T: Types.Covariant<T> }
    readonly record: Record.Document
    /**
     * Positive safe-integer schema version used to select migrations.
     */
    readonly version: number
    readonly value: Readonly<T>
    readonly deltasSinceBase: number
  }
  /**
   * Fields supplied when constructing a nominal document snapshot.
   *
   * @category models
   */
  export type SnapshotInput<T extends object = Record.JsonObject> = Omit<
    Snapshot<T>,
    typeof SnapshotTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >
  /**
   * Recursively mutable view of decoded document data.
   *
   * @category models
   */
  export type Draft<T> = T extends string | number | boolean | null | undefined
    ? T
    : T extends ReadonlyArray<infer A>
      ? Array<Draft<A>>
      : T extends object
        ? { -readonly [K in keyof T]: Draft<T[K]> }
        : T
}
