import * as Fiber from 'effect/Fiber'
import * as Scope from 'effect/Scope'
/** One serialized deferred request; provider capabilities remain owned by their model Layers. */
import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as AiError from 'effect/ai/AiError'
import * as Schema from 'effect/Schema'
import * as Model from '../Model.js'
import type * as Settings from '../Settings.js'
import type * as Run from './RunState.js'
import * as ModelStream from './ModelStream.js'
import { HarnessError } from '../HarnessError.js'
import * as Usage from '../Usage.js'
import { NativeWriteFailure } from './NativeWriteFailure.js'
import { StreamPublicationFailure } from './StreamPublicationFailure.js'

type Starting = Extract<Run.RunState, { _tag: 'deferred-start' }>
type Polling = Extract<Run.RunState, { _tag: 'deferred' }>
const deadline = Schema.Int.check(
  Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
)
const invalid = (message: string) =>
  new HarnessError({ reason: 'invalid', operation: 'model.deferred', message })
const expired = () =>
  new Model.DeferredTerminalError({
    reason: 'expired',
    responseId: 'saved deferred request',
    message: 'Deferred request reached its saved expiry',
  })

/** A finite local safety limit, independent of the provider's captured transport retry budget. */
export const expiry = Effect.fnUntraced(function* (settings: Settings.Snapshot) {
  const hint = settings.stream.deferred
  const window = typeof hint === 'object' ? hint.window : undefined
  let duration = 15 * 60 * 1000
  if (window === '1h') duration = 60 * 60 * 1000
  if (window === '24h') duration = 24 * 60 * 60 * 1000
  return yield* Schema.decodeEffect(deadline)((yield* Clock.currentTimeMillis) + duration)
})

export const run = Effect.fnUntraced(function* <E, R>(
  initial: Starting | Polling,
  adapter: Model.Deferred,
  request: Model.DeferredRequest,
  save: (state: Starting | Polling) => Effect.Effect<void, E, R>,
  policy: Effect.Effect<Settings.Snapshot>,
  create: boolean,
) {
  let state = initial
  const publish = (checkpoint: Starting | Polling) =>
    save(checkpoint).pipe(
      Effect.catchCause((cause): Effect.Effect<never, E | StreamPublicationFailure> => {
        const usage = checkpoint.usage
        const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
        return reason !== undefined &&
          Cause.isFailReason(reason) &&
          reason.error instanceof NativeWriteFailure &&
          usage !== undefined
          ? Effect.fail(
              new StreamPublicationFailure({
                error: reason.error,
                requestId: request.requestId,
                attempt: request.attempt,
                usage,
              }),
            )
          : Effect.failCause(cause)
      }),
    )
  const complete = (outcome: Model.Complete, checkpoint: Starting | Polling) =>
    Effect.gen(function* () {
      const usage = yield* Schema.decodeEffect(Usage.RecordSchema)(
        ModelStream.usage(outcome.response.usage, checkpoint.preparation),
      )
      yield* publish({ ...checkpoint, usage })
      return outcome
    })
  if (state._tag === 'deferred-start') {
    if (!create)
      return yield* invalid(
        'Deferred create outcome is unknown: no saved handle; automatic recreation is refused',
      )
    yield* publish(state)
    const admittedAt = yield* Clock.currentTimeMillis
    const created = yield* Effect.exit(
      adapter.start(request).pipe(
        Effect.timeoutOrElse({
          duration: Math.max(0, state.expiresAt - admittedAt),
          orElse: expired,
        }),
      ),
    )
    if (Exit.isFailure(created)) {
      const reason = created.cause.reasons.length === 1 ? created.cause.reasons[0] : undefined
      if (reason !== undefined && Cause.isFailReason(reason) && AiError.isAiError(reason.error)) {
        const usage = ModelStream.errorUsage(reason.error, state.preparation)
        if (usage !== undefined) yield* publish({ ...state, usage })
      }
      return yield* Effect.failCause(created.cause)
    }
    const outcome = created.value
    if (outcome._tag === 'Complete') return yield* complete(outcome, state)
    yield* Schema.decodeEffect(deadline)(outcome.nextPollAt)
    if (outcome.nextPollAt <= (yield* Clock.currentTimeMillis))
      return yield* invalid('Deferred start must supply a future absolute poll deadline')
    if (outcome.expiresAt !== undefined) yield* Schema.decodeEffect(deadline)(outcome.expiresAt)
    const { expiresAt, ...pinned } = state
    state = {
      ...pinned,
      _tag: 'deferred',
      phase: 'deferred',
      handle: yield* Schema.decodeEffect(Schema.Json)(outcome.handle),
      pollAt: outcome.nextPollAt,
      expiresAt: Math.min(expiresAt, outcome.expiresAt ?? expiresAt),
      pollFailures: 0,
    }
    // Publication failure never authorizes a poll or a second create. A cold reader
    // reconciles the authoritative handle after an uncertain acknowledged write.
    yield* publish(state)
  }
  for (;;) {
    const pending: Polling = state
    let now = yield* Clock.currentTimeMillis
    let remaining = Math.min(pending.pollAt, pending.expiresAt) - now
    while (remaining > 0) {
      yield* Effect.sleep(Math.min(remaining, 0x7fffffff))
      now = yield* Clock.currentTimeMillis
      remaining = Math.min(pending.pollAt, pending.expiresAt) - now
    }
    if (now >= pending.expiresAt) return yield* expired()
    const outcome = yield* Effect.exit(
      adapter
        .poll(pending.handle, pending.pollAt, request)
        .pipe(Effect.timeoutOrElse({ duration: pending.expiresAt - now, orElse: expired })),
    )
    if (Exit.isFailure(outcome)) {
      const cause = outcome.cause
      // Interruption, mixed Causes, configuration and storage failures never borrow
      // provider retry classification or manufacture accounting observations.
      const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
      const error =
        reason !== undefined && Cause.isFailReason(reason) && AiError.isAiError(reason.error)
          ? reason.error
          : undefined
      const current = yield* policy
      now = yield* Clock.currentTimeMillis
      const retryAt =
        error === undefined
          ? undefined
          : ModelStream.retryAt(error, pending.pollFailures, current, now)
      const usage =
        error === undefined ? undefined : ModelStream.errorUsage(error, pending.preparation)
      const pollAt =
        retryAt === undefined ? undefined : Math.max(retryAt, now + 1, pending.pollAt + 1)
      const retry =
        pollAt !== undefined && Number.isSafeInteger(pollAt) && pollAt < pending.expiresAt
      if (retry || usage !== undefined) {
        state = {
          ...pending,
          ...(usage === undefined ? {} : { usage }),
          ...(retry ? { pollAt: pollAt!, pollFailures: pending.pollFailures + 1 } : {}),
        }
        yield* publish(state)
      }
      if (!retry) return yield* Effect.failCause(cause)
      continue
    }
    now = yield* Clock.currentTimeMillis
    if (now >= pending.expiresAt) return yield* expired()
    if (outcome.value._tag === 'Complete') return yield* complete(outcome.value, pending)
    const next = outcome.value.nextPollAt
    yield* Schema.decodeEffect(deadline)(next)
    if (next <= pending.pollAt || next <= now)
      return yield* invalid('Deferred poll deadlines must strictly increase beyond admission')
    state = { ...pending, pollAt: next }
    yield* publish(state)
  }
})

/** Remote cleanup has no local mutation capability; the runtime owns its finalizer until joined. */
export type CancelReport = {
  readonly status: 'completed' | 'failed' | 'timeout'
  readonly message?: string
}
export const cancel = Effect.fnUntraced(function* <E, R>(
  operation: Effect.Effect<void, E, R>,
  owner: import('effect/Scope').Closeable,
) {
  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const scope = yield* Scope.make()
      const fiber = yield* operation.pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.forkIn(scope),
      )
      return yield* restore(
        Fiber.await(fiber).pipe(
          Effect.map((exit): CancelReport =>
            Exit.isSuccess(exit)
              ? { status: 'completed' }
              : { status: 'failed', message: Cause.pretty(exit.cause) },
          ),
          Effect.timeoutOrElse({
            duration: 10000,
            orElse: () =>
              Effect.succeed<CancelReport>({
                status: 'timeout',
                message:
                  'Remote cancellation exceeded 10000ms; cleanup is still owned by the runtime',
              }),
          }),
        ),
      ).pipe(
        Effect.ensuring(
          Scope.close(scope, Exit.void).pipe(
            Effect.uninterruptible,
            Effect.forkIn(owner),
            Effect.asVoid,
          ),
        ),
      )
    }),
  )
})
