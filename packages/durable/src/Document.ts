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

export interface Definition<T extends Record.JsonObject> {
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
export interface Document<T extends Record.JsonObject> {
  readonly definition: Definition<T>
  readonly family: boolean
}
export class DocumentDefinitionError extends Schema.TaggedError<DocumentDefinitionError>(
  '@effect-harness/durable/Document/DocumentDefinitionError',
)('DocumentDefinitionError', { message: Schema.String }) {}
const checkDefinition = <T extends Record.JsonObject>(
  definition: Definition<T>,
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
export const define = <T extends Record.JsonObject>(
  definition: Definition<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => ({ definition, family: false }))
export const family = <T extends Record.JsonObject>(
  definition: Definition<T>,
): Result.Result<Document<T>, DocumentDefinitionError> =>
  Result.map(checkDefinition(definition), () => ({ definition, family: true }))
export const defineUnsafe = <T extends Record.JsonObject>(definition: Definition<T>): Document<T> =>
  Result.getOrThrow(define(definition))
export const familyUnsafe = <T extends Record.JsonObject>(definition: Definition<T>): Document<T> =>
  Result.getOrThrow(family(definition))
export interface Target {
  readonly owner?: Record.ConversationId | Record.TaskId
  readonly key?: string
  readonly seed?: Record.Json
}
export interface Snapshot<T extends Record.JsonObject = Record.JsonObject> {
  readonly record: Record.Document
  readonly version: number
  readonly value: Readonly<T>
  readonly deltasSinceBase: number
}

export const address = Effect.fnUntraced(function* <T extends Record.JsonObject>(
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
  readonly values: WeakMap<object, Map<string, Record.JsonObject>>
}
export const makeMigrationCache = (): MigrationCache => ({ values: new WeakMap() })
export type Draft<T> = T extends string | number | boolean | null | undefined
  ? T
  : T extends ReadonlyArray<infer A>
    ? Array<Draft<A>>
    : T extends object
      ? { -readonly [K in keyof T]: Draft<T[K]> }
      : T

export const typed = Effect.fnUntraced(function* <T extends Record.JsonObject>(
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
    const previous = cache?.values.get(token)?.get(key)
    if (previous !== undefined) value = yield* detachedEffect(previous)
    else {
      const input = yield* detachedEffect(snapshot.value)
      value = yield* Effect.try({
        try: () => definition.migrate?.(input, snapshot.version) ?? snapshot.value,
        catch: (cause) => rejected('Document migration failed', Invalid, cause),
      })
      value = yield* detachedEffect(value)
      value = yield* validate(definition.schema, value)
      yield* validate(Schema.JsonObject, value)
      if (cache !== undefined) {
        const values = cache.values.get(token) ?? new Map<string, Record.JsonObject>()
        values.set(key, yield* detachedEffect(value))
        cache.values.set(token, values)
      }
    }
  }
  const decoded = yield* validate(definition.schema, value)
  yield* validate(Schema.JsonObject, decoded)
  return { ...snapshot, version: definition.version, value: yield* detachedEffect(decoded) }
})

/** Detaches validated JSON values into mutable data while preserving primitive brands. */
export const copy = <T extends Record.Json>(value: T): Result.Result<Draft<T>, CloneError> =>
  Result.map(detached(value), (value) => value as Draft<T>)
/** Synchronous copy for validated static data or documented synchronous callbacks. */
export const copyUnsafe = <T extends Record.Json>(value: T): Draft<T> =>
  detachedUnsafe(value) as Draft<T>
/** Copies active drafts into the typed storage channel inside Effect transactions. */
export const copyEffect = <T extends Record.Json>(
  value: T,
): Effect.Effect<Draft<T>, StorageError> =>
  detachedEffect(value).pipe(Effect.map((value) => value as Draft<T>))
