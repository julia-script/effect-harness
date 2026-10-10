import type * as Usage from './Usage.js'
/** Pipeable client handles; every backend operation carries schema-backed data. */
import * as Effect from 'effect/Effect'
import { dual } from 'effect/Function'
import * as Option from 'effect/Option'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
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

export { ForkOptionsSchema, type ForkOptions } from './HarnessBackend.js'
import type { ForkOptions } from './HarnessBackend.js'
export const TypeId = ConversationTypeId
export interface Conversation extends Pipeable.Pipeable {
  readonly [TypeId]: typeof TypeId
  readonly [ConversationHost.Backend]: HarnessBackendService
  readonly id: Record.ConversationId
}
const backend = (self: Conversation) => self[ConversationHost.Backend]
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
export const abort: {
  (): (self: Conversation) => Effect.Effect<void, HarnessError>
  (self: Conversation): Effect.Effect<void, HarnessError>
} = dual(
  (args) => args.length !== 0,
  (self: Conversation) => backend(self).abort(self.id),
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
  ) =>
    backend(self)
      .snapshot({ kind: document.definition.kind, ...target })
      .pipe(
        Effect.flatMap((value) =>
          Option.isSome(value)
            ? Schema.decodeEffect(Document.SnapshotSchema(document.definition.schema))(
                value.value,
              ).pipe(Effect.asSome)
            : Effect.succeedNone,
        ),
      ),
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
  ) =>
    backend(self)
      .watch({ kind: document.definition.kind, ...target })
      .pipe(
        Stream.mapEffect((value) =>
          Schema.decodeEffect(Document.SnapshotSchema(document.definition.schema))(value),
        ),
      ),
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
