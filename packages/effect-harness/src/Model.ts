import type { ProviderAffinity } from './ProviderAffinity.js'
import * as Usage from './Usage.js'
import * as Identity from './Identity.js'
/** Schema-backed model metadata and live Effect LanguageModel handles. */
import * as Context from 'effect/Context'
import * as Cause from 'effect/Cause'
import * as Capability from './internal/Capability.js'
import * as Effect from 'effect/Effect'
import * as Clock from 'effect/Clock'
import type * as Pipeable from 'effect/Pipeable'
import * as Schema from 'effect/Schema'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as AiError from 'effect/ai/AiError'
import type * as Prompt from 'effect/ai/Prompt'
import type * as Tool from 'effect/ai/Tool'
import type * as Toolkit from 'effect/ai/Toolkit'
import { ModelError, ModelInvalidResponseError, ModelUnsupportedError } from './ModelError.js'
import type * as Settings from './Settings.js'
import type * as Prepared from './PreparedRequest.js'

export const TypeId = '~effect-harness/Model'

export const ReferenceSchema = Schema.Struct({
  provider: Schema.NonEmptyString,
  modelId: Schema.NonEmptyString,
})
export type Reference = typeof ReferenceSchema.Type

export const CapabilitiesSchema = Schema.Struct({
  tools: Schema.Boolean,
  reasoning: Schema.Boolean,
  images: Schema.Boolean,
  structuredOutput: Schema.Boolean,
})
export type Capabilities = typeof CapabilitiesSchema.Type

/** Limits are explicit caller declarations, not a claim of current remote availability. */
export const DefinitionSchema = Schema.Struct({
  ref: ReferenceSchema,
  pricing: Schema.optionalKey(Usage.PricingSchema),
  capabilities: CapabilitiesSchema,
  contextWindow: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  maxOutputTokens: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  /** Explicit selected-model limits; omitted limits do not grant image reading. */
  imageLimits: Schema.optionalKey(
    Schema.Struct({
      maxWidth: Schema.Int.check(
        Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
      ),
      maxHeight: Schema.Int.check(
        Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
      ),
      maxBytes: Schema.Int.check(
        Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
      ),
      maxInputPixels: Schema.Int.check(
        Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
      ),
      transparency: Schema.optionalKey(Schema.Literals(['preserve', 'flatten-white'])),
    }),
  ),
})
export type Definition = typeof DefinitionSchema.Type

export const SelectionSchema = Schema.Struct({
  ref: ReferenceSchema,
  options: Schema.JsonObject,
})
export type Selection = typeof SelectionSchema.Type

/** Only an adapter's explicitly declared request configuration is persisted, never ambient services. */
export interface Configuration {
  readonly binding: Prepared.Binding
  readonly encode: (context: Context.Context<never>) => Effect.Effect<Schema.Json, ModelError>
  readonly restore: (value: Schema.Json) => Effect.Effect<Context.Context<never>, ModelError>
}
const configurationError = (cause: unknown) =>
  new ModelError({
    reason: new ModelUnsupportedError({
      message: 'Request configuration cannot be restored by its declared codec',
      cause,
    }),
  })

/**
 * Capture a concrete configuration codec and its own services before runtime erasure.
 * capture and restore must name the supported native configuration tags and fields;
 * clients, credentials and toolkit services are not request configuration.
 * Increase version when changing this persisted contract. Recovery calls restore, never configure.
 */
export const makeConfiguration = <S extends Schema.Constraint, R, E>(input: {
  readonly id: string
  readonly version: number
  readonly schema: S
  /** Exact native request configuration tags; all other returned services are rejected. */
  readonly tags: ReadonlyArray<{ readonly key: string }>
  readonly capture: (context: Context.Context<never>) => Effect.Effect<S['Type'], E, R>
  readonly restore: (value: S['Type']) => Effect.Effect<Context.Context<never>, E, R>
}): Effect.Effect<Configuration, never, R | S['EncodingServices'] | S['DecodingServices']> =>
  Effect.gen(function* () {
    const context = yield* Effect.context<R | S['EncodingServices'] | S['DecodingServices']>()
    const { capture, restore } = input
    const codec = Schema.toCodecJson(input.schema)
    const tags = new Set(input.tags.map((tag) => tag.key))
    const validate = (
      configuration: Context.Context<never>,
    ): Effect.Effect<Context.Context<never>, ModelError> =>
      [...configuration.mapUnsafe.keys()].every((key) => tags.has(key))
        ? Effect.succeed(configuration)
        : Effect.fail(configurationError('Configuration contains an undeclared service tag'))
    return {
      binding: Object.freeze({ id: input.id, version: input.version }),
      encode: (configuration) =>
        validate(configuration).pipe(
          Effect.flatMap((configuration) => Effect.suspend(() => capture(configuration))),
          Effect.flatMap(Schema.encodeEffect(codec, { onExcessProperty: 'error' })),
          Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
          Effect.mapError(configurationError),
          Effect.provideContext(context),
        ),
      restore: (value) =>
        Schema.decodeEffect(codec, { onExcessProperty: 'error' })(value).pipe(
          Effect.flatMap((value) => Effect.suspend(() => restore(value))),
          Effect.flatMap(validate),
          Effect.catchCause((cause) => Effect.failCause(Cause.map(cause, configurationError))),
          Effect.provideContext(context),
        ),
    }
  })

/** The portable default accepts only empty configuration. It never serializes arbitrary Context values. */
export const emptyConfiguration: Configuration = {
  binding: Object.freeze({ id: 'effect-harness/empty-configuration', version: 1 }),
  encode: (context) =>
    context.mapUnsafe.size === 0
      ? Effect.succeed(null)
      : Effect.fail(
          configurationError('Supply a declared configuration codec for native request services'),
        ),
  restore: (value) =>
    value === null
      ? Effect.succeed(Context.empty())
      : Effect.fail(configurationError('Expected empty configuration')),
}
export interface PreparationPolicy {
  readonly requestId: Prepared.Snapshot['requestId']
  readonly ordinal: number
  readonly attempt: Prepared.Snapshot['attempt']
  readonly settings: Settings.Snapshot
}

/** Immutable prepared-work identity, distinct from provider attempt and durable logical ordinal. */
export const DeferredIdentitySchema = Schema.Struct({
  requestId: Identity.RequestId,
  attempt: Usage.AttemptId,
})

/** Reconstitute the pinned prompt, offered declarations and configured services after recovery. */
export interface DeferredRequest<
  Tools extends Record<string, Tool.Any> = {},
> extends DeferredIdentity {
  readonly prompt: Prompt.Prompt
  readonly toolkit: Toolkit.Toolkit<Tools>
  readonly toolChoice?: LanguageModel.ToolChoice<NoInfer<Tools[keyof Tools]['name']>> | undefined
  /** Live configuration only; persist its schema-encoded options separately. */
  readonly configuration?: Context.Context<never> | undefined
}
export type DeferredIdentity = typeof DeferredIdentitySchema.Type
export type DeferredRequestServices<Request extends DeferredRequest<any>> =
  LanguageModel.ExtractServices<{
    readonly toolkit: Request['toolkit']
    readonly disableToolCallResolution: true
  }>

export type DeferredResponse = Pick<
  LanguageModel.GenerateTextResponse<Record<string, Tool.Any>, 'encoded'>,
  'content' | 'text' | 'usage' | 'finishReason'
>
export interface Complete {
  readonly _tag: 'Complete'
  readonly response: DeferredResponse
}
export interface Pending {
  readonly _tag: 'Pending'
  /** Absolute epoch milliseconds, strictly later than the previous deadline and operation admission clock. */
  readonly nextPollAt: number
}
export interface DeferredHandle<H> {
  readonly _tag: 'Deferred'
  readonly handle: H
  readonly nextPollAt: number
  readonly expiresAt?: number | undefined
}

/** Terminal remote work failure; transport failures retain their native AiError. */
export class DeferredTerminalError extends Schema.TaggedError<DeferredTerminalError>(
  'effect-harness/Model/DeferredTerminalError',
)('DeferredTerminalError', {
  reason: Schema.Literals(['failed', 'cancelled', 'expired']),
  responseId: Schema.String,
  message: Schema.String,
}) {
  get isRetryable(): boolean {
    return false
  }
}
export type DeferredError = AiError.AiError | ModelError | DeferredTerminalError

/**
 * Optional, captured provider capability. Handles are JSON codec output, never live services.
 *
 * The caller commits Deferred before polling, sleeps until the saved absolute deadline, and
 * supplies the same prepared request to poll. Persist an abort mark and fence late completions
 * before invoking cancel with a bounded timeout. Closing a local scope is not remote cancellation.
 * This descriptor does not implement durable polling, retries, checkpointing or exactly-once I/O.
 */
export interface Deferred {
  readonly start: <Request extends DeferredRequest<any>>(
    request: Request,
  ) => Effect.Effect<
    Complete | DeferredHandle<Schema.Json>,
    DeferredError,
    DeferredRequestServices<Request>
  >
  readonly poll: <Request extends DeferredRequest<any>>(
    handle: Schema.Json,
    previousPollAt: number,
    request: Request,
  ) => Effect.Effect<Complete | Pending, DeferredError, DeferredRequestServices<Request>>
  readonly cancel: (handle: Schema.Json) => Effect.Effect<void, DeferredError>
}

const invalidDeferred = (message: string, cause?: unknown) =>
  new ModelError({
    reason: new ModelInvalidResponseError({ message, ...(cause === undefined ? {} : { cause }) }),
  })
const deadlineSchema = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
)

/**
 * Captures custom callback and handle codec services in the caller-owned Layer scope.
 * Codec failures stay in the ModelError channel. Encoding is additionally validated as JSON.
 * An explicit capture effect can restrict the retained context; configuration on each prepared
 * request takes precedence over this capture. The owner must keep acquired resources alive.
 * Supply R explicitly for callbacks that require services. Poll deadlines are checked against
 * the admission clock so encoding or callback work cannot invalidate a previously future deadline.
 */
export const makeDeferred = <H extends Schema.Constraint, R>(input: {
  readonly handle: H
  readonly start: <Request extends DeferredRequest<any>>(
    request: Request,
  ) => Effect.Effect<
    Complete | DeferredHandle<H['Type']>,
    DeferredError,
    R | DeferredRequestServices<Request>
  >
  readonly poll: <Request extends DeferredRequest<any>>(
    handle: H['Type'],
    previousPollAt: number,
    request: Request,
  ) => Effect.Effect<Complete | Pending, DeferredError, R | DeferredRequestServices<Request>>
  readonly cancel: (handle: H['Type']) => Effect.Effect<void, DeferredError, R>
  readonly capture?: Effect.Effect<
    Context.Context<R | H['DecodingServices'] | H['EncodingServices']>,
    never,
    R | H['DecodingServices'] | H['EncodingServices']
  >
}): Effect.Effect<Deferred, never, R | H['DecodingServices'] | H['EncodingServices']> =>
  Effect.gen(function* () {
    const { start, poll, cancel } = input
    const services = yield* (
      input.capture ?? Effect.context<R | H['DecodingServices'] | H['EncodingServices']>()
    )
    const codec = Schema.toCodecJson(input.handle)
    const decode = (handle: Schema.Json) =>
      Schema.decodeEffect(codec)(handle).pipe(
        Effect.mapError((cause) => invalidDeferred('Invalid deferred handle', cause)),
      )
    const deadline = Effect.fnUntraced(function* (next: number, now: number, previous = -1) {
      yield* Schema.decodeEffect(deadlineSchema)(next).pipe(
        Effect.mapError((cause) => invalidDeferred('Invalid deferred poll deadline', cause)),
      )
      if (next <= now || next <= previous)
        return yield* invalidDeferred(
          'Deferred poll deadlines must strictly increase beyond the operation admission clock',
        )
    })
    const configured = <A, R0, Tools extends Record<string, Tool.Any>>(
      effect: Effect.Effect<
        A,
        DeferredError,
        R | H['DecodingServices'] | H['EncodingServices'] | R0
      >,
      request?: DeferredRequest<Tools>,
    ) => {
      const captured = effect.pipe(Effect.provideContext(services))
      // Inner provision wins: frozen request configuration overrides construction defaults.
      return request?.configuration === undefined
        ? captured
        : Effect.provideContext(effect.pipe(Effect.provideContext(request.configuration)), services)
    }
    return {
      start: <Request extends DeferredRequest<any>>(request: Request) =>
        configured(
          Effect.gen(function* () {
            yield* Schema.decodeEffect(DeferredIdentitySchema)(request).pipe(
              Effect.mapError((cause) =>
                invalidDeferred('Invalid deferred request identity', cause),
              ),
            )
            const admittedAt = yield* Clock.currentTimeMillis
            const outcome = yield* start<Request>(request)
            if (outcome._tag === 'Complete') return outcome
            yield* deadline(outcome.nextPollAt, admittedAt)
            if (outcome.expiresAt !== undefined)
              yield* Schema.decodeEffect(deadlineSchema)(outcome.expiresAt).pipe(
                Effect.mapError((cause) => invalidDeferred('Invalid deferred expiry', cause)),
              )
            const encoded = yield* Schema.encodeEffect(codec)(outcome.handle).pipe(
              Effect.mapError((cause) =>
                invalidDeferred('Invalid deferred handle encoding', cause),
              ),
            )
            const handle = yield* Schema.decodeEffect(Schema.Json)(encoded).pipe(
              Effect.mapError((cause) =>
                invalidDeferred('Deferred handle must be serializable JSON', cause),
              ),
            )
            return { ...outcome, handle }
          }),
          request,
        ),
      poll: <Request extends DeferredRequest<any>>(
        handle: Schema.Json,
        previous: number,
        request: Request,
      ) =>
        configured(
          Effect.gen(function* () {
            yield* Schema.decodeEffect(deadlineSchema)(previous).pipe(
              Effect.mapError((cause) =>
                invalidDeferred('Invalid previous deferred deadline', cause),
              ),
            )
            const admittedAt = yield* Clock.currentTimeMillis
            const outcome = yield* poll<Request>(yield* decode(handle), previous, request)
            if (outcome._tag === 'Pending')
              yield* deadline(outcome.nextPollAt, admittedAt, previous)
            return outcome
          }),
          request,
        ),
      cancel: (handle) => configured(Effect.flatMap(decode(handle), cancel)),
    } satisfies Deferred
  })

/** Missing optional capability fails semantically, without affecting ordinary generation. */
export const deferred = (self: Any): Effect.Effect<Deferred, ModelError> =>
  self.deferred === undefined
    ? Effect.fail(
        new ModelError({
          reason: new ModelUnsupportedError({
            message: 'Model does not support deferred responses',
          }),
        }),
      )
    : Effect.succeed(self.deferred)

export interface Any extends Pipeable.Pipeable {
  readonly [TypeId]: { readonly _Requirements: (_: never) => unknown }
  readonly definition: Definition
  readonly languageModel: typeof LanguageModel.LanguageModel.Service
  readonly options: Schema.Constraint
  readonly configure: (options: never, preparation: PreparationPolicy) => unknown
  readonly deferred?: Deferred | undefined
  /** Stable provider construction contract. Increase on incompatible callback/default changes. */
  readonly version?: number | undefined
  readonly configuration?: Configuration | undefined
  /** Default leading/latest is an explicit native fallback, not positional tool support. */
  readonly positioning?: Prepared.Snapshot['translation']['positioning'] | undefined
}

export type Requirements<M extends Any> = M extends {
  readonly [TypeId]: { readonly _Requirements: (_: never) => infer R }
}
  ? Exclude<R, ProviderAffinity>
  : never

export interface Model<Options extends Schema.Constraint, R = never> extends Any {
  readonly [TypeId]: { readonly _Requirements: (_: never) => R }
  readonly options: Options
  readonly deferred?: Deferred | undefined
  /**
   * Maps schema-decoded options to provider request services without changing the pinned model.
   * ProviderAffinity is supplied by the runtime. Native LanguageModel callbacks can read
   * it with Effect.serviceOption because their required services are owned by Effect AI.
   */
  readonly configure: (
    options: Options['Type'],
    preparation: PreparationPolicy,
  ) => Effect.Effect<Context.Context<never>, ModelError, R>
}

/** Receives an already constructed provider; provider Layers retain resource ownership. */
export const make: <Options extends Schema.Constraint, R>(input: {
  readonly definition: Definition
  readonly languageModel: typeof LanguageModel.LanguageModel.Service
  readonly options: Options
  readonly deferred?: Deferred | undefined
  readonly version?: number | undefined
  readonly configuration?: Configuration | undefined
  readonly positioning?: Prepared.Snapshot['translation']['positioning'] | undefined
  readonly configure: (
    options: Options['Type'],
    preparation: PreparationPolicy,
  ) => Effect.Effect<Context.Context<never>, ModelError, R>
}) => Model<Options, R | Options['DecodingServices'] | Options['EncodingServices']> = (input) => ({
  ...input,
  [TypeId]: { _Requirements: (_: never) => _ },
  pipe: Capability.pipe,
})

export type { ModelError } from './ModelError.js'
