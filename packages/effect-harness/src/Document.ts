/**
 * Validated document definitions, scoped draft values and detached snapshots.
 */
import { dual } from 'effect/Function'
import * as handle from './internal/handle.ts'
const DefinitionProto = handle.prototype({
  id: '@effect-harness/durable/Document/Definition',
  fields: ['kind', 'version', 'scope', 'history', 'fork'],
})
const DocumentProto = handle.prototype({
  id: '@effect-harness/durable/Document/Document',
  fields: ['kind', 'version', 'scope', 'family'],
})
const SnapshotProto = handle.prototype({
  id: '@effect-harness/durable/Document/Snapshot',
  fields: ['record', 'version', 'value', 'deltasSinceBase'],
})
import type * as Pipeable from 'effect/Pipeable'
import type * as Inspectable from 'effect/Inspectable'
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
import { rejected, type StorageError, InvalidError } from './StorageError.ts'
import {
  detached,
  detachedUnsafe,
  detachedEffect,
  validate,
  type CloneError,
} from './internal/records.ts'
/**
 * Public APIs from `./internal/records.ts`.
 *
 * @category re-exports
 */
export { CloneError } from './internal/records.ts'

const DefinitionTypeId = '~@effect-harness/durable/Document/Definition'
const TypeId = '~@effect-harness/durable/Document'
const SnapshotTypeId = '~@effect-harness/durable/Document/Snapshot'
const MigrationCacheProto = handle.prototype({
  id: '@effect-harness/durable/Document/MigrationCache',
  fields: ['values'],
})
const MigrationCacheTypeId = '~@effect-harness/durable/Document/MigrationCache'
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
  readonly definition: Document.Definition<T>
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
export const isDocument = (u: unknown): u is Document.Any => Predicate.hasProperty(u, TypeId)
const makeDocument = <T extends object>(
  input: Document.DefinitionInput<T>,
  family: boolean,
): Document<T> => {
  const definition = handle.make(DefinitionProto, {
    ...input,
    [DefinitionTypeId]: { _T: identity },
  })
  Object.defineProperty(definition, DefinitionTypeId, { enumerable: false })
  // Definition fields above are owned data properties captured by the original spread.
  // Reusing them for diagnostics does not resample the caller's accessors.
  const token = handle.make(
    DocumentProto,
    { definition, family, [TypeId]: { _T: identity } },
    { kind: definition.kind, version: definition.version, scope: definition.scope },
  )
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
  definition: Document.DefinitionInput<T>,
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
  definition: Document.DefinitionInput<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => makeDocument(definition, false))
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
  definition: Document.DefinitionInput<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => makeDocument(definition, true))
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
export const defineUnsafe = <T extends object>(
  definition: Document.DefinitionInput<T>,
): Document<T> => Result.getOrThrow(define(definition))
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
export const familyUnsafe = <T extends object>(
  definition: Document.DefinitionInput<T>,
): Document<T> => Result.getOrThrow(family(definition))
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
export const makeSnapshot = <T extends object>(
  input: Document.SnapshotInput<T>,
): Document.Snapshot<T> => {
  const value = handle.make(SnapshotProto, handle.marked(input, SnapshotTypeId, { _T: identity }))
  return Object.defineProperty(value, SnapshotTypeId, { enumerable: false })
}

const addressImpl = Effect.fnUntraced(function* <T extends object>(
  self: Document<T>,
  target: Document.Target = {},
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
  if (definition.scope === 'session') scope = { _tag: 'session' }
  else if (definition.scope === 'conversation')
    scope = {
      _tag: 'conversation',
      conversationId: yield* validate(Record.ConversationId, target.owner),
    }
  else scope = { _tag: 'task', taskId: yield* validate(Record.TaskId, target.owner) }
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
export interface MigrationCache extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [MigrationCacheTypeId]: typeof MigrationCacheTypeId
  readonly values: WeakMap<object, Cache.Cache<string, Schema.JsonObject, StorageError>>
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
export const isMigrationCache = (u: unknown): u is MigrationCache =>
  Predicate.hasProperty(u, MigrationCacheTypeId)
/**
 * Creates a token-local migration cache with captured decoding services.
 *
 * @category constructors
 */
export const makeMigrationCache: Effect.Effect<MigrationCache> = Effect.gen(function* () {
  const cache = handle.make<
    Omit<MigrationCache, keyof Pipeable.Pipeable | keyof Inspectable.Inspectable>
  >(MigrationCacheProto, {
    [MigrationCacheTypeId]: MigrationCacheTypeId,
    values: new WeakMap<object, Cache.Cache<string, Schema.JsonObject, StorageError>>(),
    permit: yield* Semaphore.make(1),
  })
  return Object.defineProperty(cache, MigrationCacheTypeId, { enumerable: false })
})

const typedImpl = Effect.fnUntraced(function* <T extends object>(
  self: Document<T>,
  snapshot: Document.Snapshot,
  cache?: MigrationCache,
): Effect.fn.Return<Document.Snapshot<T>, StorageError> {
  const definition = self.definition
  if (
    snapshot.record.scope._tag !== definition.scope ||
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
        Effect.mapError((cause) =>
          rejected('Invalid migration cache snapshot', InvalidError, cause),
        ),
      )
      const input = yield* detachedEffect(storedValue)
      const migrated = yield* Effect.try({
        try: () => definition.migrate?.(input, version) ?? storedValue,
        catch: (cause) => rejected('Document migration failed', InvalidError, cause),
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
 * Encodes the decoded domain model into the separately validated JSON storage representation.
 *
 * @category schemas
 */
const encodeImpl = <T extends object>(
  token: Document<T>,
  value: T,
): Effect.Effect<Schema.JsonObject, StorageError> =>
  Schema.encodeEffect(token.definition.schema)(value).pipe(
    Effect.mapError((cause) => rejected('Document cannot be encoded', InvalidError, cause)),
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
export const copy = <T>(self: T): Result.Result<Document.Draft<T>, CloneError> =>
  Result.map(detached(self), (self) => self as Document.Draft<T>)
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
export const copyUnsafe = <T>(self: T): Document.Draft<T> =>
  detachedUnsafe(self) as Document.Draft<T>
/**
 * Copies active drafts into the typed storage channel inside Effect transactions.
 *
 * @category combinators
 */
export const copyEffect = <T>(self: T): Effect.Effect<Document.Draft<T>, StorageError> =>
  detachedEffect(self).pipe(Effect.map((self) => self as Document.Draft<T>))

/**
 * Resolves a document token and target into its durable address.
 *
 * @category combinators
 */
export const address: {
  (
    target?: Document.Target,
  ): <T extends object>(self: Document<T>) => Effect.Effect<Record.Address, StorageError>
  <T extends object>(
    self: Document<T>,
    target?: Document.Target,
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
    snapshot: Document.Snapshot,
    cache?: MigrationCache,
  ): <T extends object>(self: Document<T>) => Effect.Effect<Document.Snapshot<T>, StorageError>
  <T extends object>(
    self: Document<T>,
    snapshot: Document.Snapshot,
    cache?: MigrationCache,
  ): Effect.Effect<Document.Snapshot<T>, StorageError>
} = dual((args) => isDocument(args[0]), typedImpl)

/**
 * Type-level contracts for `Document`.
 *
 */
export declare namespace Document {
  /** A document token whose invariant decoded type is deliberately erased.
   * @category models
   */
  export interface Any extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly [TypeId]: { readonly _T: unknown }
    readonly definition: AnyDefinition
    readonly family: boolean
  }
  /** Definition metadata and callbacks that remain safe after erasing the decoded type.
   *
   * The schema retains its identity through Schema.Constraint. Initialization and migration
   * yield only object; checkpointWhen cannot be called with an unproven decoded value.
   * @category models
   */
  export interface AnyDefinition extends Pipeable.Pipeable, Inspectable.Inspectable {
    readonly [DefinitionTypeId]: { readonly _T: unknown }
    readonly kind: string
    readonly version: number
    readonly scope: Record.Scope['_tag']
    readonly history?: 'latest' | 'rewindable' | undefined
    readonly fork?: 'asOf' | 'current' | 'initial' | undefined
    readonly schema: Schema.Constraint
    readonly initial: (seed?: Schema.Json) => object
    readonly migrate?: ((value: Schema.JsonObject, fromVersion: number) => object) | undefined
    readonly checkpointWhen?:
      | ((
          value: never,
          ops: ReadonlyArray<Record.Op>,
          info: { readonly deltasSinceBase: number },
        ) => boolean)
      | undefined
  }
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
    readonly scope: Record.Scope['_tag']
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
    readonly schema: Schema.Codec<T, Schema.JsonObject>
    /**
     * Returns the initial decoded value.
     *
     * **Details**
     *
     * Seed decoding may throw. Session.transaction maps initializer failures to
     * rejected StorageError before commit.
     */
    readonly initial: (seed?: Schema.Json) => T
    /**
     * Pure transformation from an older stored version to the current decoded model.
     */
    readonly migrate?: ((value: Schema.JsonObject, fromVersion: number) => T) | undefined
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
    readonly seed?: Schema.Json | undefined
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
  export interface Snapshot<out T extends object = Schema.JsonObject>
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
  export type SnapshotInput<T extends object = Schema.JsonObject> = Omit<
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

/** Encodes a document value through its owned storage codec.
 * @category combinators
 */
export const encode: {
  <A extends object>(
    value: A,
  ): <T extends object>(
    self: Document<T> & ([A] extends [T] ? unknown : never),
  ) => Effect.Effect<Schema.JsonObject, StorageError>
  <T extends object>(self: Document<T>, value: T): Effect.Effect<Schema.JsonObject, StorageError>
} = dual(2, encodeImpl)

/** Checks the decoded DocumentDefinitionError contract without decoding or coercing input.
 * @category guards
 */
export const isDocumentDefinitionError: (u: unknown) => u is DocumentDefinitionError = Schema.is(
  Schema.toType(DocumentDefinitionError),
)

/** Checks the nominal definition identity without recovering its invariant decoded type.
 * @category guards
 */
export const isDefinition = (u: unknown): u is Document.AnyDefinition =>
  Predicate.hasProperty(u, DefinitionTypeId)

/** Checks the nominal snapshot identity at its safe covariant object target.
 * @category guards
 */
export const isSnapshot = (u: unknown): u is Document.Snapshot<object> =>
  Predicate.hasProperty(u, SnapshotTypeId)
