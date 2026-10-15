/** Schema-backed durable checkpoints. A started tool intent is persisted before its handler runs. */
import * as PromptWire from '../PromptWire.js'
import * as Schema from 'effect/Schema'
import * as Struct from 'effect/Struct'
import * as Record from '../Record.js'
import * as Agent from '../Agent.js'
import * as Submission from '../Submission.js'
import * as PreparedRequest from '../PreparedRequest.js'
import * as Request from './RequestRecord.js'
import * as Effect from 'effect/Effect'
import type * as Transaction from '../Transaction.js'
import type * as Session from '../Session.js'
import * as Usage from '../Usage.js'
import * as LiveState from '../LiveState.js'
import * as AiError from 'effect/ai/AiError'
import * as Redacted from 'effect/Redacted'

const shared = { submissionId: Record.SubmissionId }
const running = {
  ...shared,
  agent: Agent.StateSchema,
  ordinal: Schema.optionalKey(Schema.Natural),
  compacted: Schema.optionalKey(Record.TaskId),
  overflow: Schema.optionalKey(AiError.AiError),
  preparation: Schema.optionalKey(PreparedRequest.SnapshotSchema),
  attempt: Schema.optionalKey(Usage.AttemptId),
  retries: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  ),
}
const prepared = {
  ...running,
  preparation: PreparedRequest.SnapshotSchema,
  attempt: Usage.AttemptId,
  retries: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
}
const deadline = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
)
export const RunStateSchema = Schema.Union([
  Schema.TaggedStruct('queued', {
    phase: Schema.tag('queued'),
    ...shared,
    draft: Submission.InputDraftSchema,
  }),
  Schema.TaggedStruct('request', { phase: Schema.tag('request'), ...running }),
  Schema.TaggedStruct('compaction', {
    phase: Schema.tag('compaction'),
    ...running,
    child: Record.TaskId,
  }),
  Schema.TaggedStruct('deferred-start', {
    phase: Schema.tag('deferred-start'),
    ...prepared,
    expiresAt: deadline,
    usage: Schema.optionalKey(Usage.RecordSchema),
  }),
  Schema.TaggedStruct('deferred', {
    phase: Schema.tag('deferred'),
    ...prepared,
    handle: Schema.Json,
    pollAt: deadline,
    expiresAt: deadline,
    pollFailures: Schema.Int.check(
      Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
    ),
    usage: Schema.optionalKey(Usage.RecordSchema),
  }),
  Schema.TaggedStruct('streaming', {
    phase: Schema.tag('streaming'),
    ...prepared,
    revision: Schema.Natural,
    partial: Schema.optionalKey(LiveState.PartialSchema),
  }),
  Schema.TaggedStruct('retry', {
    phase: Schema.tag('retry'),
    ...prepared,
    until: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER })),
  }),
  Schema.TaggedStruct('toolRound', {
    phase: Schema.tag('toolRound'),
    ...running,
    preparation: PreparedRequest.SnapshotSchema,
    roundKey: Schema.NonEmptyString,
    assistant: Record.EntryId,
    children: Schema.Array(Record.TaskId),
  }),
])
export type RunState = typeof RunStateSchema.Type
const safeHeaders = (headers: typeof AiError.HttpRequestDetails.Type.headers) =>
  Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [
      key,
      Redacted.isRedacted(value) ? '<redacted>' : value,
    ]),
  )
const safeHttp = (http: typeof AiError.HttpContext.Type | undefined) =>
  http === undefined
    ? undefined
    : {
        ...http,
        request: { ...http.request, headers: safeHeaders(http.request.headers) },
        response:
          http.response === undefined
            ? undefined
            : { ...http.response, headers: safeHeaders(http.response.headers) },
      }
/** Native JSON codecs unwrap Redacted values; replace them before durable encoding. */
const safeOverflow = (error: AiError.AiError): AiError.AiError => {
  const reason = error.reason
  let saved: AiError.AiError['reason']
  switch (reason._tag) {
    case 'NetworkError':
      saved = new AiError.NetworkError({
        reason: reason.reason,
        description: reason.description,
        request: { ...reason.request, headers: safeHeaders(reason.request.headers) },
      })
      break
    case 'RateLimitError':
      saved = new AiError.RateLimitError({
        retryAfter: reason.retryAfter,
        metadata: reason.metadata,
        http: safeHttp(reason.http),
      })
      break
    case 'QuotaExhaustedError':
      saved = new AiError.QuotaExhaustedError({
        resetAt: reason.resetAt,
        metadata: reason.metadata,
        http: safeHttp(reason.http),
      })
      break
    case 'AuthenticationError':
      saved = new AiError.AuthenticationError({
        kind: reason.kind,
        description: reason.description,
        metadata: reason.metadata,
        http: safeHttp(reason.http),
      })
      break
    case 'ContentPolicyError':
      saved = new AiError.ContentPolicyError({
        description: reason.description,
        metadata: reason.metadata,
        http: safeHttp(reason.http),
      })
      break
    case 'InvalidRequestError':
      saved = new AiError.InvalidRequestError({
        parameter: reason.parameter,
        constraint: reason.constraint,
        description: reason.description,
        metadata: reason.metadata,
        http: safeHttp(reason.http),
      })
      break
    case 'InternalProviderError':
      saved = new AiError.InternalProviderError({
        description: reason.description,
        metadata: reason.metadata,
        http: safeHttp(reason.http),
      })
      break
    case 'UnknownError':
      saved = new AiError.UnknownError({
        description: reason.description,
        metadata: reason.metadata,
        http: safeHttp(reason.http),
      })
      break
    default:
      saved = reason
  }
  return AiError.make({ module: error.module, method: error.method, reason: saved })
}
export const encode = (state: RunState) =>
  Schema.encodeEffect(Schema.toCodecJson(RunStateSchema))(
    state._tag !== 'queued' && state.overflow !== undefined
      ? { ...state, overflow: safeOverflow(state.overflow) }
      : state,
  )
export const decode = Schema.decodeUnknownEffect(Schema.toCodecJson(RunStateSchema))
export const Messages = Schema.Array(Schema.toCodecJson(PromptWire.Message))

/** Validate committed prefixes without repairing, discarding or replaying stored work. */
export const validPrefix = (state: RunState): boolean => {
  if (state._tag !== state.phase) return false
  if (
    state._tag !== 'queued' &&
    state.preparation !== undefined &&
    state.preparation.ordinal !== state.ordinal
  )
    return false
  if (
    (state._tag === 'deferred' || state._tag === 'deferred-start') &&
    state.preparation.execution !== 'deferred'
  )
    return false
  if (state._tag === 'toolRound')
    return (
      state.roundKey.length > 0 &&
      state.children.length > 0 &&
      new Set(state.children).size === state.children.length
    )
  return true
}

/** Persistent checkpoints carry only a request identity; transcript context is projected from entries. */
export const StoredSchema = Schema.Union(
  RunStateSchema.members.map((member) =>
    member.mapFields((fields) =>
      'preparation' in fields
        ? { ...fields, preparation: Schema.optionalKey(Request.ReferenceSchema) }
        : fields,
    ),
  ),
)
export type Stored = typeof StoredSchema.Type
export const decodeStored = Schema.decodeUnknownEffect(Schema.toCodecJson(StoredSchema))
export const persist = Effect.fnUntraced(function* (
  tx: Transaction.Transaction,
  task: Record.Task,
  state: RunState,
) {
  const old =
    task.state.checkpoint === undefined ? undefined : yield* decodeStored(task.state.checkpoint)
  const preparation =
    state._tag === 'queued' || state.preparation === undefined
      ? undefined
      : yield* Request.save(tx, task.id, state.preparation)
  if (
    old !== undefined &&
    old._tag !== 'queued' &&
    old.preparation !== undefined &&
    old.preparation.requestId !== preparation?.requestId
  )
    yield* Request.retire(tx, old.preparation)
  const safe =
    state._tag === 'queued'
      ? state
      : {
          ...Struct.omit(state, ['preparation']),
          ...(state.overflow === undefined ? {} : { overflow: safeOverflow(state.overflow) }),
        }
  return yield* Schema.decodeUnknownEffect(StoredSchema)({
    ...safe,
    ...(preparation === undefined ? {} : { preparation }),
  })
})
export const hydrate = Effect.fnUntraced(function* <E>(
  state: Stored,
  resolve: (ref: Request.Reference) => Effect.Effect<PreparedRequest.Snapshot, E>,
) {
  if (state._tag === 'queued') return state
  const preparation =
    state.preparation === undefined ? undefined : yield* resolve(state.preparation)
  return yield* Schema.decodeUnknownEffect(RunStateSchema)({
    ...state,
    ...(preparation === undefined ? {} : { preparation }),
  })
})
export const read = (tx: Transaction.Transaction, value: unknown) =>
  decodeStored(value).pipe(
    Effect.flatMap((state) => hydrate(state, (ref) => Request.read(tx, ref))),
  )
export const get = (session: Session.Session, value: unknown) =>
  decodeStored(value).pipe(
    Effect.flatMap((state) => hydrate(state, (ref) => Request.get(session, ref))),
  )
