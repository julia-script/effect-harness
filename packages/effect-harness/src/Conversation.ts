import type * as Usage from './Usage.js'
/** Pipeable client handles; every backend operation carries schema-backed data. */
import * as Effect from 'effect/Effect'
import { dual } from 'effect/Function'
import * as Option from 'effect/Option'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import type * as Record from './Record.js'
import type * as Agent from './Agent.js'
import * as Document from './Document.js'
import type * as DocumentMaterialization from './DocumentMaterialization.js'
import * as HarnessRuntime from './HarnessRuntime.js'
import * as Session from './Session.js'
import { HarnessError } from './HarnessError.js'
import type { HarnessBackendService, Watch } from './HarnessBackend.js'
import type * as Submission from './Submission.js'
import * as ConversationHost from './internal/ConversationHost.js'
import { ConversationTypeId } from './internal/ClientHandle.js'
import { observation } from './internal/ConversationInvocation.js'
import * as Observation from './Observation.js'
import type * as ConversationView from './ConversationView.js'
import type * as AgentEvent from './AgentEvent.js'

export { ForkOptionsSchema, type ForkOptions } from './HarnessBackend.js'
import type { ForkOptions, AbortOptions, ResetOptions } from './HarnessBackend.js'
export {
  AbortOptionsSchema,
  ResetOptionsSchema,
  type AbortOptions,
  type ResetOptions,
} from './HarnessBackend.js'
export const TypeId = ConversationTypeId
export interface Conversation extends Pipeable.Pipeable {
  readonly [TypeId]: typeof TypeId
  readonly [ConversationHost.Backend]: HarnessBackendService
  readonly id: Record.ConversationId
}
const backend = (self: Conversation) => self[ConversationHost.Backend]
/** Coherent committed native view; acquiring it never starts pending work. */
export const view = (
  self: Conversation,
): Effect.Effect<ConversationView.Snapshot, Observation.Failure> => {
  const service = backend(self)
  const read = () =>
    Effect.suspend(() =>
      service.observations?.conversationSnapshot === undefined
        ? Observation.unsupported('conversation.view')
        : service.observations.conversationSnapshot(self.id),
    )
  return observation(service)?.observe(read) ?? read()
}
export const watchView = (
  self: Conversation,
  options?: Observation.WatchOptions,
): Stream.Stream<ConversationView.Frame, Observation.Failure> => {
  const service = backend(self)
  const read = () =>
    Effect.suspend(() =>
      service.observations?.watchView === undefined
        ? Observation.unsupported('conversation.watchView')
        : Effect.succeed(service.observations.watchView(self.id, options)),
    )
  return Stream.unwrap(observation(service)?.observe(read) ?? read())
}
export const events = (
  self: Conversation,
  options?: Observation.WatchOptions,
): Stream.Stream<AgentEvent.Frame, Observation.Failure> => {
  const service = backend(self)
  const read = () =>
    Effect.suspend(() =>
      service.observations?.watchEvents === undefined
        ? Observation.unsupported('conversation.events')
        : Effect.succeed(service.observations.watchEvents(self.id, options)),
    )
  return Stream.unwrap(observation(service)?.observe(read) ?? read())
}
export const watchViewWith = <A, E, R>(
  self: Conversation,
  callback: (frame: ConversationView.Frame) => Effect.Effect<A, E, R>,
  options?: Observation.WatchOptions,
): Stream.Stream<A, E | Observation.Failure, R> => {
  const observed = observation(backend(self))
  return watchView(self, options).pipe(
    Stream.mapEffect(
      (frame) =>
        observed?.observe(() => callback(frame)) ??
        Effect.scoped(Effect.suspend(() => callback(frame))),
    ),
  )
}
export const eventsWith = <A, E, R>(
  self: Conversation,
  callback: (frame: AgentEvent.Frame) => Effect.Effect<A, E, R>,
  options?: Observation.WatchOptions,
): Stream.Stream<A, E | Observation.Failure, R> => {
  const observed = observation(backend(self))
  return events(self, options).pipe(
    Stream.mapEffect(
      (frame) =>
        observed?.observe(() => callback(frame)) ??
        Effect.scoped(Effect.suspend(() => callback(frame))),
    ),
  )
}
/** Start manual durable compaction even when automatic compaction is disabled. Wait on its task handle. */
export const compact = (
  self: Conversation,
  options?: import('./CompactionTask.js').Options,
): Effect.Effect<import('./CompactionTask.js').Handle, HarnessError> => {
  const service = backend(self)
  return service.compact === undefined
    ? Effect.fail(
        new HarnessError({
          reason: 'invalid',
          operation: 'conversation.compact',
          message: 'Backend does not support compaction',
        }),
      )
    : service.compact(self.id, options)
}
export const submit: {
  (
    draft: Submission.Draft,
  ): (self: Conversation) => Effect.Effect<Submission.Submission, HarnessError>
  (self: Conversation, draft: Submission.Draft): Effect.Effect<Submission.Submission, HarnessError>
} = dual(2, (self: Conversation, draft: Submission.Draft) =>
  backend(self)
    .submit({ conversationId: self.id, draft })
    .pipe(
      Effect.map((ref) => ConversationHost.submission(backend(self), ref.id, ref.conversationId)),
    ),
)
export const configure: {
  (change: Agent.Change): (self: Conversation) => Effect.Effect<void, HarnessError>
  (self: Conversation, change: Agent.Change): Effect.Effect<void, HarnessError>
} = dual(2, (self: Conversation, change: Agent.Change) =>
  backend(self).configure({ conversationId: self.id, change }),
)
export const agent: {
  (): (self: Conversation) => Effect.Effect<Agent.State, HarnessError>
  (self: Conversation): Effect.Effect<Agent.State, HarnessError>
} = dual(
  (args) => args.length !== 0,
  (self: Conversation) => backend(self).agent(self.id),
)
export const fork: {
  (options?: ForkOptions): (self: Conversation) => Effect.Effect<Conversation, HarnessError>
  (self: Conversation, options?: ForkOptions): Effect.Effect<Conversation, HarnessError>
} = dual(
  (args) => typeof args[0] === 'object' && args[0] !== null && TypeId in args[0],
  (self: Conversation, options?: ForkOptions) =>
    backend(self)
      .fork({ conversationId: self.id, ...(options === undefined ? {} : { options }) })
      .pipe(Effect.map((id) => ConversationHost.conversation(backend(self), id))),
)
/** Waits for supervised foreground cleanup; background inclusion captures the reached task set. */
export const abort: {
  (options?: AbortOptions): (self: Conversation) => Effect.Effect<void, HarnessError>
  (self: Conversation, options?: AbortOptions): Effect.Effect<void, HarnessError>
} = dual(
  (args) => typeof args[0] === 'object' && args[0] !== null && TypeId in args[0],
  (self: Conversation, options?: AbortOptions) => backend(self).abort(self.id, options),
)

/** Admit a passive context reset. Await its receipt for placement; tools finish before busy placement. */
export const reset: {
  (
    options?: ResetOptions,
  ): (self: Conversation) => Effect.Effect<Submission.Submission, HarnessError>
  (self: Conversation, options?: ResetOptions): Effect.Effect<Submission.Submission, HarnessError>
} = dual(
  (args) => typeof args[0] === 'object' && args[0] !== null && TypeId in args[0],
  (self: Conversation, options: ResetOptions = {}) =>
    Effect.gen(function* () {
      const service = backend(self)
      if (service.reset !== undefined) {
        const ref = yield* service.reset(self.id, options)
        return ConversationHost.submission(service, ref.id, ref.conversationId)
      }
      const model =
        options.handoff === undefined
          ? undefined
          : yield* Schema.encodeEffect(Schema.Array(Schema.toCodecJson(Prompt.Message)))([
              Prompt.userMessage({ content: [Prompt.textPart({ text: options.handoff })] }),
            ]).pipe(
              Effect.mapError(
                (cause) =>
                  new HarnessError({
                    reason: 'invalid',
                    operation: 'conversation.reset',
                    message: 'Invalid handoff',
                    cause,
                  }),
              ),
            )
      return yield* submit(self, {
        type: 'write',
        entry: { kind: 'reset', head: 'self', ...(model === undefined ? {} : { model }) },
        ...(options.requestId === undefined ? {} : { requestId: options.requestId }),
      })
    }),
)
export const entries: {
  (
    options?: Omit<Watch, 'conversationId'>,
  ): (self: Conversation) => Stream.Stream<Record.Entry, HarnessError>
  (
    self: Conversation,
    options?: Omit<Watch, 'conversationId'>,
  ): Stream.Stream<Record.Entry, HarnessError>
} = dual(
  (args) => typeof args[0] === 'object' && args[0] !== null && TypeId in args[0],
  (self: Conversation, options?: Omit<Watch, 'conversationId'>) =>
    backend(self).entries({ conversationId: self.id, ...options }),
)

export const snapshot: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ): (
    self: Conversation,
  ) => Effect.Effect<
    Option.Option<Document.Snapshot<S>>,
    HarnessError | Schema.SchemaError,
    S['DecodingServices']
  >
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
  ): Effect.Effect<
    Option.Option<Document.Snapshot<S>>,
    HarnessError | Schema.SchemaError,
    S['DecodingServices']
  >
} = dual(
  3,
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
  ) => {
    const service = backend(self)
    const read = () =>
      service
        .snapshot({ kind: document.definition.kind, ...target })
        .pipe(
          Effect.flatMap((value) =>
            Option.isSome(value)
              ? Schema.decodeEffect(Document.SnapshotSchema(document.definition.schema))(
                  value.value,
                ).pipe(Effect.asSome)
              : Effect.succeedNone,
          ),
        )
    return observation(service)?.observe(read) ?? read()
  },
)
export const watch: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ): (
    self: Conversation,
  ) => Stream.Stream<Document.Snapshot<S>, HarnessError | Schema.SchemaError, S['DecodingServices']>
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
  ): Stream.Stream<Document.Snapshot<S>, HarnessError | Schema.SchemaError, S['DecodingServices']>
} = dual(
  3,
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
  ) => {
    const service = backend(self)
    const observed = observation(service)
    if (observed !== undefined)
      return Stream.unwrap(
        observed.observe(() =>
          Effect.sync(() => service.watch({ kind: document.definition.kind, ...target })),
        ),
      ).pipe(
        Stream.mapEffect((value) =>
          observed.observe(() =>
            Schema.decodeEffect(Document.SnapshotSchema(document.definition.schema))(value),
          ),
        ),
      )
    return service
      .watch({ kind: document.definition.kind, ...target })
      .pipe(
        Stream.mapEffect((value) =>
          Schema.decodeEffect(Document.SnapshotSchema(document.definition.schema))(value),
        ),
      )
  },
)

/**
 * Materializes through the owning local runtime without persistence. Provide its
 * HarnessRuntime Layer and any target decoding services. Remote backends need a local
 * runtime bridge; a runtime from another backend fails instead of reading unrelated data.
 */
export const materializedSnapshot: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ): (
    self: Conversation,
  ) => Effect.Effect<
    Option.Option<DocumentMaterialization.Snapshot<S>>,
    HarnessError | Session.Failure,
    HarnessRuntime.HarnessRuntime | S['DecodingServices']
  >
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
  ): Effect.Effect<
    Option.Option<DocumentMaterialization.Snapshot<S>>,
    HarnessError | Session.Failure,
    HarnessRuntime.HarnessRuntime | S['DecodingServices']
  >
} = dual(
  3,
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
  ) =>
    materializationSession(self).pipe(
      Effect.flatMap((session) => Session.materializedSnapshot(session, document, target)),
    ),
)

/** Materializes a visible rewindable historical revision through the owning local runtime. */
export const materializedSnapshotAsOf: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
    at: Record.EntryId,
  ): (
    self: Conversation,
  ) => Effect.Effect<
    Option.Option<DocumentMaterialization.Snapshot<S>>,
    HarnessError | Session.Failure,
    HarnessRuntime.HarnessRuntime | S['DecodingServices']
  >
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
    at: Record.EntryId,
  ): Effect.Effect<
    Option.Option<DocumentMaterialization.Snapshot<S>>,
    HarnessError | Session.Failure,
    HarnessRuntime.HarnessRuntime | S['DecodingServices']
  >
} = dual(
  4,
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
    at: Record.EntryId,
  ) =>
    materializationSession(self).pipe(
      Effect.flatMap((session) => Session.materializedSnapshotAsOf(session, document, target, at)),
    ),
)

/**
 * Watches materialized revisions through the owning local runtime. Migration-only
 * equivalent frames emit their changed storedVersion; subsequent frames decode their
 * actual stored version. Retirement, cancellation and scope cleanup release subscriptions.
 */
export const watchMaterialized: {
  <S extends Document.Codec>(
    document: Document.Document<S>,
    target: Document.Target,
  ): (
    self: Conversation,
  ) => Stream.Stream<
    DocumentMaterialization.Snapshot<S>,
    HarnessError | Session.Failure,
    HarnessRuntime.HarnessRuntime | S['DecodingServices']
  >
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
  ): Stream.Stream<
    DocumentMaterialization.Snapshot<S>,
    HarnessError | Session.Failure,
    HarnessRuntime.HarnessRuntime | S['DecodingServices']
  >
} = dual(
  3,
  <S extends Document.Codec>(
    self: Conversation,
    document: Document.Document<S>,
    target: Document.Target,
  ) =>
    Stream.unwrap(
      materializationSession(self).pipe(
        Effect.map((session) => Session.watchMaterialized(session, document, target)),
      ),
    ),
)

const materializationSession = Effect.fnUntraced(function* (self: Conversation) {
  const runtime = yield* HarnessRuntime.HarnessRuntime
  if (runtime.backend !== backend(self))
    return yield* new HarnessError({
      reason: 'invalid',
      operation: 'conversation.materialize',
      message: 'Materialization requires the runtime owning this conversation backend',
    })
  return runtime.session
})

/** Current accounting for this conversation's own work, excluding inherited fork history. */
export const usage = (self: Conversation): Effect.Effect<Usage.Summary, HarnessError> => {
  const service = backend(self)
  return service.usage === undefined
    ? Effect.fail(
        new HarnessError({
          reason: 'invalid',
          operation: 'conversation.usage',
          message: 'Backend does not support usage queries',
        }),
      )
    : service.usage(self.id)
}
