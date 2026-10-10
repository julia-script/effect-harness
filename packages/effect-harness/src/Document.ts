/** Schema-backed document definitions and detached revisions. */
import { identity as invariant } from 'effect/Function'
import { pipeArguments } from 'effect/Pipeable'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import type * as Types from 'effect/Types'
import type * as Effect from 'effect/Effect'
import type { SessionError } from './SessionError.js'
import * as Record from './Record.js'
import { DocumentRecordSchema, type StoredDocument } from './Record.js'

export const TypeId = '~effect-harness/Document'

/** Document codecs preserve both the decoded model and their service requirements. */
export type Codec = Schema.ConstraintCodec<object, Schema.JsonObject, unknown, unknown>

const identity = {
  kind: Schema.NonEmptyString,
  version: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
}

/**
 * Session/task documents have no history policy; only rewindable documents support asOf.
 * A task's transition to terminal through Transaction.putTask retires its active documents.
 * Acquisition and replacement reject terminal owners, including legacy terminal tasks.
 * Detached snapshots remain values; legacy documents are readable until explicitly retired.
 */
export const MetadataSchema = Schema.Union([
  Schema.Struct({ ...identity, scope: Schema.tag('session') }),
  Schema.Struct({ ...identity, scope: Schema.tag('task') }),
  Schema.Struct({
    ...identity,
    scope: Schema.tag('conversation'),
    history: Schema.tag('latest'),
    fork: Schema.Literals(['current', 'initial']),
  }),
  Schema.Struct({
    ...identity,
    scope: Schema.tag('conversation'),
    history: Schema.tag('rewindable'),
    fork: Schema.Literals(['asOf', 'current', 'initial']),
  }),
])
export type Metadata = typeof MetadataSchema.Type

/**
 * Directly converts one older encoded JSON value to the current encoded shape.
 * Decode the old shape with its schema; return an Effect with no service requirements.
 * The target schema validates the result before any transaction mutation is staged.
 * Callbacks should only transform values: external side effects cannot be rolled back.
 */
export type Migration = (
  value: Schema.JsonObject,
) => Effect.Effect<Schema.JsonObject, SessionError | Schema.SchemaError>

/** A definition is a runtime token; its metadata and document values have separate schemas. */
export type Definition<S extends Codec> = Metadata & {
  readonly schema: S
  readonly initial: (seed?: Schema.Json) => S['Type']
  /**
   * Opt-in upgrades keyed by persisted source version. Each callback maps directly to
   * this definition's version; callbacks are never chained. Missing versions, downgrades
   * and policy changes conflict. ensureDocument and updateDocument persist upgrades
   * in their enclosing commit; snapshots, watches, setDocument and retirement require
   * an exact version. Historical reads require the definition for that historical version.
   * Forks preserve their selected revision's version and upgrade on later acquisition.
   */
  readonly migrations?: { readonly [version: number]: Migration | undefined }
}

export interface Document<S extends Codec> extends Pipeable.Pipeable {
  readonly [TypeId]: Types.Invariant<S>
  readonly definition: Definition<S>
  readonly family: boolean
}

/** Defines one document per scope. Definition validation occurs on Session access. */
export const define = <S extends Codec>(definition: Definition<S>): Document<S> => ({
  [TypeId]: invariant,
  definition: Object.freeze({
    ...definition,
    ...(definition.migrations === undefined
      ? {}
      : { migrations: Object.freeze({ ...definition.migrations }) }),
  }),
  family: false,
  pipe() {
    return pipeArguments(this, arguments)
  },
})

/** Defines keyed documents; access must supply a family member key. */
export const family = <S extends Codec>(definition: Definition<S>): Document<S> => ({
  ...define(definition),
  family: true,
})

/** Explicit ownership of a singleton document or keyed family member. */
export const TargetSchema = Schema.Struct({
  scope: Record.Scope,
  key: Schema.optionalKey(Schema.String),
})
export type Target = typeof TargetSchema.Type

/** Schema factory for a detached, decoded document revision and its storage metadata. */
export const SnapshotSchema = <S extends Codec>(schema: S) =>
  Schema.Struct({
    record: DocumentRecordSchema,
    version: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
    value: schema,
    deltasSinceBase: Schema.Natural,
  })
export type Snapshot<S extends Codec> = Omit<StoredDocument, 'value'> & {
  readonly value: S['Type']
}
