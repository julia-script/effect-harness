import { Effect, Schema } from 'effect'
import { copyJson, Invalid, isArray } from '../../internal/json.ts'
import type {
  DocumentAddress,
  DocumentCreate,
  DocumentRecord,
  DocumentScope,
  Op,
  StoredDocument,
} from '../../storage/schema/Document.ts'
import { ConversationId, TaskId } from '../../storage/schema/Ids.ts'
import type { DocumentId } from '../../storage/schema/Ids.ts'
import type { JsonObject } from '../../storage/schema/Json.ts'
import type { CheckpointInfo, Semantics } from '../Document.ts'
import { DocumentError } from '../SessionError.ts'

/**
 * Document definitions after overload resolution, ported from pi's `documents.ts`. Records are
 * matched against definitions here: scope, history and fork policy must agree, and a stored
 * version must be the definition's or an older one it can migrate.
 */

/**
 * A singleton or family definition with its value type erased, and its callbacks' error type:
 * their failures pass through the Session untouched, and the public overloads type them again.
 */
export type AnyDefinition = Semantics & {
  readonly kind: string
  readonly version: number
  readonly family?: true
  // Methods, so that a typed definition's narrower parameters still fit.
  initial(seed?: Schema.Json): Effect.Effect<JsonObject>
  migrate?(value: JsonObject, fromVersion: number): Effect.Effect<JsonObject>
  checkpointWhen?(
    value: Readonly<JsonObject>,
    ops: ReadonlyArray<Op>,
    info: CheckpointInfo,
  ): Effect.Effect<boolean>
}

/** A singleton or family token with its value type erased. */
export interface AnyToken {
  readonly definition: AnyDefinition
}

/** A logical address and its string identity for maps. */
export interface ResolvedAddress {
  readonly address: DocumentAddress
  readonly id: string
  /** Index of the first argument after the owner and family key. */
  readonly next: number
}

export const refused = (message: string) => Effect.fail(new DocumentError({ message }))

const isConversationId = Schema.is(ConversationId)
const isTaskId = Schema.is(TaskId)

/** The owner scope a definition takes from the head of an argument list. */
const resolveScope = (definition: AnyDefinition, owner: unknown): DocumentScope | undefined => {
  if (definition.scope === 'session') return { kind: 'session' }
  if (definition.scope === 'conversation') {
    return isConversationId(owner) ? { kind: 'conversation', conversationId: owner } : undefined
  }
  return isTaskId(owner) ? { kind: 'task', taskId: owner } : undefined
}

/** Stable string identity of one logical address. */
export const addressId = (address: DocumentAddress): string => {
  const scope = address.scope
  const owner = (): number | null => {
    if (scope.kind === 'session') return null
    return scope.kind === 'conversation' ? scope.conversationId : scope.taskId
  }
  return JSON.stringify([address.kind, scope.kind, owner(), address.key ?? null])
}

/**
 * Resolve the owner and family key of an overloaded argument list. Fails when the definition's
 * version is not a positive integer or an argument is not what the definition requires.
 */
export const resolveAddress = (
  definition: AnyDefinition,
  args: ReadonlyArray<unknown>,
): Effect.Effect<ResolvedAddress, DocumentError> => {
  const { kind, version } = definition
  if (!Number.isSafeInteger(version) || version < 1) {
    return refused(`Document ${kind} version must be a positive integer`)
  }
  const owned = definition.scope !== 'session'
  const scope = resolveScope(definition, args[0])
  if (scope === undefined) {
    return refused(`Document ${kind} requires a ${definition.scope} ID`)
  }
  let next = owned ? 1 : 0
  const key = definition.family === true ? args[next++] : undefined
  if (definition.family === true && typeof key !== 'string') {
    return refused(`Document family ${kind} requires a string key`)
  }
  const address: DocumentAddress = typeof key === 'string' ? { kind, scope, key } : { kind, scope }
  return Effect.succeed({ address, id: addressId(address), next })
}

/** The storage create record of a new incarnation at an address. */
export const documentCreate = (
  definition: AnyDefinition,
  address: DocumentAddress,
  id: DocumentId,
): DocumentCreate => {
  const identity = {
    id,
    kind: address.kind,
    ...(address.key === undefined ? {} : { key: address.key }),
  }
  const scope = address.scope
  if (scope.kind === 'session') return { ...identity, scope }
  if (scope.kind === 'task') return { ...identity, scope }
  return definition.scope === 'conversation' && definition.history === 'rewindable'
    ? { ...identity, scope, history: 'rewindable', fork: definition.fork }
    : { ...identity, scope, history: 'latest', fork: latestFork(definition) }
}

const latestFork = (definition: AnyDefinition): 'current' | 'initial' =>
  definition.scope === 'conversation' && definition.fork === 'initial' ? 'initial' : 'current'

const describe = (record: DocumentCreate | DocumentRecord) =>
  `Document ${record.id} (${record.kind})`

/** Refuse typed access whose definition disagrees with the record's scope, history, or fork. */
export const checkRecordScope = (
  definition: AnyDefinition,
  record: DocumentCreate | DocumentRecord,
): Effect.Effect<void, DocumentError> => {
  const agrees =
    record.scope.kind === definition.scope &&
    (record.scope.kind !== 'conversation' ||
      !('history' in record) ||
      (definition.scope === 'conversation' &&
        record.history === definition.history &&
        record.fork === definition.fork))
  return agrees
    ? Effect.void
    : refused(`${describe(record)} does not match the supplied definition semantics`)
}

/** Refuse typed access to a stored version the definition cannot use. */
export const checkRecordVersion = (
  definition: AnyDefinition,
  record: DocumentCreate | DocumentRecord,
  version: number,
): Effect.Effect<void, DocumentError> => {
  if (version > definition.version) {
    return refused(`${describe(record)} has newer version ${version} than ${definition.version}`)
  }
  if (version < definition.version && definition.migrate === undefined) {
    return refused(`${describe(record)} requires migration from version ${version}`)
  }
  return Effect.void
}

/** Copy a value supplied by a definition callback, which must be a strict-JSON object. */
export const ownedObject = (
  value: unknown,
  what: string,
): Effect.Effect<JsonObject, DocumentError> => {
  const copied = copyJson(value)
  if (copied instanceof Invalid) return refused(`${what}: ${copied.message}`)
  if (copied === null || typeof copied !== 'object' || isArray(copied)) {
    return refused(`${what}: expected a JSON object`)
  }
  return Effect.succeed(copied)
}

/** Check and materialize a detached value for typed access, migrating an older version. */
export const materializeValue = Effect.fnUntraced(function* (
  definition: AnyDefinition,
  record: DocumentCreate | DocumentRecord,
  version: number,
  value: JsonObject,
) {
  yield* checkRecordScope(definition, record)
  yield* checkRecordVersion(definition, record, version)
  if (version === definition.version || definition.migrate === undefined) return value
  return yield* ownedObject(
    yield* definition.migrate(value, version),
    `${describe(record)} migration from version ${version}`,
  )
})

export const materialize = (definition: AnyDefinition, stored: StoredDocument) =>
  materializeValue(definition, stored.record, stored.version, stored.value)

/** The initial value of a new incarnation, from the family seed when there is one. */
export const initialValue = (definition: AnyDefinition, seed: Schema.Json | undefined) =>
  Effect.flatMap(
    // A family's seed is its initializer's only argument; a singleton's initializer takes none.
    definition.family === true ? definition.initial(seed) : definition.initial(),
    (value) => ownedObject(value, `Document ${definition.kind} initial value`),
  )
