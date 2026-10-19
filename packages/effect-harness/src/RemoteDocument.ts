/** Materialized document reads through the handle's captured optional backend capability. */
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as Option from 'effect/Option'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import type * as Conversation from './Conversation.js'
import * as Document from './Document.js'
import * as Materialization from './DocumentMaterialization.js'
import * as Record from './Record.js'
import * as Observation from './Observation.js'
import type { HarnessError } from './HarnessError.js'
import type * as Session from './Session.js'
import { RemoteDocumentError } from './RemoteDocumentError.js'
import { Backend } from './internal/ConversationHost.js'
import * as Lifetime from './internal/RemoteDocumentLifetime.js'
import * as Value from './internal/DocumentValue.js'

export type Failure = RemoteDocumentError | HarnessError | Session.Failure
export const ReadRequestSchema = Schema.Struct({
  address: Record.DocumentAddressSchema,
  metadata: Document.MetadataSchema,
  family: Schema.Boolean,
})
export type ReadRequest = typeof ReadRequestSchema.Type
export const HistoricalRequestSchema = Schema.Struct({
  ...ReadRequestSchema.fields,
  at: Record.EntryId,
})
export type HistoricalRequest = typeof HistoricalRequestSchema.Type
const base = {
  protocol: Schema.Literal(1),
  target: Record.DocumentAddressSchema,
  incarnation: Record.DocumentId,
  epoch: Schema.NonEmptyString,
}
export const WireFrameSchema = Schema.Union([
  Schema.TaggedStruct('Snapshot', {
    ...base,
    revision: Observation.CounterSchema,
    value: Record.StoredDocumentSchema,
  }),
  Schema.TaggedStruct('Replace', {
    ...base,
    from: Observation.CounterSchema,
    to: Observation.CounterSchema,
    value: Record.StoredDocumentSchema,
  }),
  Schema.TaggedStruct('Rebase', {
    ...base,
    revision: Observation.CounterSchema,
    value: Record.StoredDocumentSchema,
  }),
  Schema.TaggedStruct('Retired', {
    ...base,
    from: Observation.CounterSchema,
    to: Observation.CounterSchema,
  }),
])
export type WireFrame = typeof WireFrameSchema.Type
export type MaterializedFrame<S extends Document.Codec> = WireFrame extends infer F
  ? F extends { readonly value: Record.StoredDocument }
    ? Omit<F, 'value'> & { readonly value: Materialization.Snapshot<S> }
    : F
  : never
export interface MaterializedState<S extends Document.Codec> {
  readonly protocol: 1
  readonly target: Record.DocumentAddress
  readonly incarnation: Record.DocumentId
  readonly epoch: string
  readonly revision: number
  readonly value: Option.Option<Materialization.Snapshot<S>>
}
export interface Capabilities {
  readonly snapshot?: (
    request: ReadRequest,
  ) => Effect.Effect<Option.Option<Record.StoredDocument>, Failure>
  readonly snapshotAsOf?: (
    request: HistoricalRequest,
  ) => Effect.Effect<Option.Option<Record.StoredDocument>, Failure>
  readonly watch?: (
    request: ReadRequest,
    options?: Observation.WatchOptions,
  ) => Stream.Stream<WireFrame, Failure>
}
export const unsupported = (operation: string) =>
  Effect.fail(
    new RemoteDocumentError({
      reason: 'unsupported',
      operation,
      message: 'The captured backend does not support this document operation',
    }),
  )
const request = Effect.fnUntraced(function* <S extends Document.Codec>(
  document: Document.Document<S>,
  target: Document.Target,
) {
  const address = yield* Value.address(document, target)
  return yield* Value.copy(ReadRequestSchema, {
    address,
    metadata: document.definition,
    family: document.family,
  })
})
/** Reads without persisting a migration; only the target codec's decoding services are required. */
export const snapshot = <S extends Document.Codec>(
  self: Conversation.Conversation,
  document: Document.Document<S>,
  target: Document.Target,
): Effect.Effect<Option.Option<Materialization.Snapshot<S>>, Failure, S['DecodingServices']> =>
  Effect.suspend(() => {
    const backend = self[Backend]
    const method = backend.documents?.snapshot
    if (method === undefined) return unsupported('remoteDocument.snapshot')
    return Lifetime.observe(backend, 'Current', () =>
      Effect.gen(function* () {
        const raw = yield* method(yield* request(document, target))
        return Option.isNone(raw)
          ? Option.none()
          : Option.some(yield* Materialization.materialize(document, raw.value))
      }),
    )
  })
/** Selects a visible historical revision on the server and materializes it on the client. */
export const snapshotAsOf = <S extends Document.Codec>(
  self: Conversation.Conversation,
  document: Document.Document<S>,
  target: Document.Target,
  at: Record.EntryId,
): Effect.Effect<Option.Option<Materialization.Snapshot<S>>, Failure, S['DecodingServices']> =>
  Effect.suspend(() => {
    const backend = self[Backend]
    const method = backend.documents?.snapshotAsOf
    if (method === undefined) return unsupported('remoteDocument.snapshotAsOf')
    return Lifetime.observe(backend, 'Historical', () =>
      Effect.gen(function* () {
        const raw = yield* method({ ...(yield* request(document, target)), at })
        return Option.isNone(raw)
          ? Option.none()
          : Option.some(yield* Materialization.materialize(document, raw.value))
      }),
    )
  })
const materializeFrame = <S extends Document.Codec>(
  document: Document.Document<S>,
  frame: WireFrame,
): Effect.Effect<MaterializedFrame<S>, Failure, S['DecodingServices']> =>
  frame._tag === 'Retired'
    ? Effect.succeed(frame)
    : Materialization.materialize(document, frame.value).pipe(
        Effect.map((value) => ({ ...frame, value })),
      )
/** Keeps retirement explicit and preserves stored-version transitions and mount continuity. */
export const watchWith = <S extends Document.Codec, A, E, R>(
  self: Conversation.Conversation,
  document: Document.Document<S>,
  target: Document.Target,
  callback: (frame: MaterializedFrame<S>) => Effect.Effect<A, E, R>,
  options?: Observation.WatchOptions,
): Stream.Stream<A, Failure | E, S['DecodingServices'] | R> =>
  Stream.unwrap(
    Effect.gen(function* () {
      const backend = self[Backend]
      const method = backend.documents?.watch
      if (method === undefined) return yield* unsupported('remoteDocument.watch')
      const outer = yield* Scope.Scope
      const acquired = yield* Lifetime.observe(backend, 'Watch', () =>
        Effect.gen(function* () {
          const selector = yield* request(document, target)
          const pull = yield* Stream.toPull(method(selector, options)).pipe(
            Effect.provideService(Scope.Scope, outer),
          )
          return {
            pull,
            first: yield* pull.pipe(
              Effect.mapError((error) =>
                Cause.isDone(error)
                  ? new RemoteDocumentError({
                      reason: 'closed',
                      operation: 'remoteDocument.watch',
                      message: 'Document mount ended before initial state',
                    })
                  : error,
              ),
            ),
          }
        }),
      )
      let state: MaterializedState<S> | undefined
      return Stream.concat(
        Stream.fromIterable(acquired.first),
        Stream.fromPull(Effect.succeed(acquired.pull)),
      ).pipe(
        Stream.mapEffect((frame) =>
          Lifetime.observe(
            backend,
            'Frame',
            () =>
              materializeFrame(document, frame).pipe(
                Effect.flatMap((materialized) =>
                  reduce(state, materialized).pipe(
                    Effect.tap((next) =>
                      Effect.sync(() => {
                        state = next
                      }),
                    ),
                    Effect.andThen(Effect.suspend(() => callback(materialized))),
                  ),
                ),
              ),
            frame,
          ),
        ),
      )
    }),
  )
/** Emits complete materialized frames, including a terminal retirement frame. */
export const watchFrames = <S extends Document.Codec>(
  self: Conversation.Conversation,
  document: Document.Document<S>,
  target: Document.Target,
  options?: Observation.WatchOptions,
): Stream.Stream<MaterializedFrame<S>, Failure, S['DecodingServices']> =>
  watchWith(self, document, target, (frame) => Effect.succeed(frame), options)
/** Emits non-retired snapshots and ends with the mounted incarnation. */
export const watch = <S extends Document.Codec>(
  self: Conversation.Conversation,
  document: Document.Document<S>,
  target: Document.Target,
  options?: Observation.WatchOptions,
): Stream.Stream<Materialization.Snapshot<S>, Failure, S['DecodingServices']> =>
  watchFrames(self, document, target, options).pipe(
    Stream.filterMap((frame) =>
      frame._tag === 'Retired' ? Result.fail(undefined) : Result.succeed(frame.value),
    ),
  )
/** Folds complete replacements without re-encoding a serviceful decoded value. */
export const reduce = Effect.fnUntraced(function* <S extends Document.Codec>(
  current: MaterializedState<S> | undefined,
  frame: MaterializedFrame<S>,
): Effect.fn.Return<MaterializedState<S>, RemoteDocumentError> {
  const fail = () =>
    new RemoteDocumentError({
      reason: 'continuity',
      operation: 'remoteDocument.reduce',
      message: 'Foreign, duplicate or discontinuous document frame',
    })
  const revision = frame._tag === 'Snapshot' || frame._tag === 'Rebase' ? frame.revision : frame.to
  if (frame.protocol !== 1) return yield* fail()
  if (
    frame._tag !== 'Retired' &&
    (frame.value.record.id !== frame.incarnation ||
      !Schema.toEquivalence(Record.DocumentAddressSchema)(frame.value.record, frame.target))
  )
    return yield* fail()
  if (current === undefined) {
    if (frame._tag !== 'Snapshot') return yield* fail()
  } else if (
    frame._tag === 'Snapshot' ||
    current.protocol !== 1 ||
    current.epoch !== frame.epoch ||
    current.incarnation !== frame.incarnation ||
    !Schema.toEquivalence(Record.DocumentAddressSchema)(current.target, frame.target) ||
    Option.isNone(current.value)
  )
    return yield* fail()
  else if (frame._tag === 'Rebase') {
    if (frame.revision <= current.revision) return yield* fail()
  } else if (frame.from !== current.revision || frame.to !== frame.from + 1) return yield* fail()
  const target = yield* Value.copy(Record.DocumentAddressSchema, frame.target).pipe(
    Effect.mapError(fail),
  )
  return {
    protocol: 1,
    target,
    incarnation: frame.incarnation,
    epoch: frame.epoch,
    revision,
    value: frame._tag === 'Retired' ? Option.none() : Option.some(frame.value),
  }
})
