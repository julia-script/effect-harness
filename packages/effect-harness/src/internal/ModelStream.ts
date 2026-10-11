/** Native stream aggregation and one invocation-owned trailing checkpoint writer. */
import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import type * as Scope from 'effect/Scope'
import * as Deferred from 'effect/Deferred'
import * as Duration from 'effect/Duration'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as AiError from 'effect/ai/AiError'
import * as LanguageModel from 'effect/ai/LanguageModel'
import * as Prompt from 'effect/ai/Prompt'
import * as Response from 'effect/ai/Response'
import type * as Tool from 'effect/ai/Tool'
import type * as Prepared from '../PreparedRequest.js'
import type * as Settings from '../Settings.js'
import type * as LiveState from '../LiveState.js'
import * as Usage from '../Usage.js'
import { HarnessError } from '../HarnessError.js'

const invalid = (message: string) =>
  new HarnessError({ reason: 'invalid', operation: 'model.stream', message })
type Tools = Record<string, Tool.Any>
type Part = Response.StreamPart<Tools, 'encoded'>
type Content = Response.Part<Tools, 'encoded'>
export const usage = (value: Response.Usage, preparation: Prepared.Snapshot): Usage.Record =>
  Usage.model(
    {
      ...(value.inputTokens.total === undefined ? {} : { input: value.inputTokens.total }),
      ...(value.inputTokens.uncached === undefined
        ? {}
        : { inputUncached: value.inputTokens.uncached }),
      ...(value.inputTokens.cacheRead === undefined
        ? {}
        : { cacheRead: value.inputTokens.cacheRead }),
      ...(value.inputTokens.cacheWrite === undefined
        ? {}
        : { cacheWrite: value.inputTokens.cacheWrite }),
      ...(value.outputTokens.total === undefined ? {} : { output: value.outputTokens.total }),
      ...(value.outputTokens.text === undefined ? {} : { outputText: value.outputTokens.text }),
      ...(value.outputTokens.reasoning === undefined
        ? {}
        : { reasoning: value.outputTokens.reasoning }),
    },
    preparation.model._tag === 'descriptor' ? preparation.model.definition.ref : undefined,
    preparation.model._tag === 'descriptor' ? preparation.model.definition.pricing : undefined,
  )

/** Native typed usage metadata is optional; absence never invents a charge. */
export const errorUsage = (
  error: AiError.AiError,
  preparation: Prepared.Snapshot,
): Usage.Record | undefined => {
  if (!('metadata' in error.reason)) return undefined
  const reported = Schema.decodeUnknownOption(AiError.UsageInfo)(error.reason.metadata['usage'])
  if (Option.isNone(reported)) return undefined
  const { promptTokens, completionTokens, totalTokens } = reported.value
  if (promptTokens === undefined && completionTokens === undefined && totalTokens === undefined)
    return undefined
  return Usage.model(
    {
      ...(promptTokens === undefined ? {} : { input: promptTokens }),
      ...(completionTokens === undefined ? {} : { output: completionTokens }),
      ...(totalTokens === undefined ? {} : { totalTokens }),
    },
    preparation.model._tag === 'descriptor' ? preparation.model.definition.ref : undefined,
    preparation.model._tag === 'descriptor' ? preparation.model.definition.pricing : undefined,
  )
}

/** Only typed transport/rate-limit/internal-provider failures are transient. */
export const retryAt = (
  error: AiError.AiError,
  retries: number,
  settings: Settings.Snapshot,
  now: number,
): number | undefined => {
  if (!settings.retry.enabled || retries >= settings.retry.maxRetries || !error.isRetryable)
    return undefined
  const reason = error.reason
  if (
    reason._tag !== 'RateLimitError' &&
    reason._tag !== 'InternalProviderError' &&
    !(reason._tag === 'NetworkError' && reason.isRetryable)
  )
    return undefined
  const policy = settings.retry
  const exponential =
    policy.baseDelayMs === 0
      ? 0
      : Math.min(policy.maxAgentDelayMs, policy.baseDelayMs * 2 ** Math.min(retries, 1024))
  const hint = error.retryAfter === undefined ? 0 : Duration.toMillis(error.retryAfter)
  if (!Number.isFinite(hint) || hint < 0) return undefined
  // The host cap applies to both sources; a provider-specific tighter cap is honored too.
  const providerCap = settings.stream.maxRetryDelayMs
  const cappedHint = Math.min(
    policy.maxAgentDelayMs,
    providerCap === undefined || providerCap === 0 ? hint : Math.min(providerCap, hint),
  )
  const until = now + Math.ceil(Math.max(exponential, cappedHint))
  return Number.isSafeInteger(until) && until >= now ? until : undefined
}

export const consume = Effect.fnUntraced(function* <E, R, R2>(
  stream: Stream.Stream<Part, AiError.AiError, R>,
  preparation: Prepared.Snapshot,
  interval: number,
  publish: (partial: LiveState.Partial, revision: number) => Effect.Effect<void, E, R2>,
): Effect.fn.Return<
  {
    readonly exit: Exit.Exit<
      LanguageModel.GenerateTextResponse<Tools, 'encoded'>,
      AiError.AiError | HarnessError | Schema.SchemaError | E
    >
    readonly partial: LiveState.Partial | undefined
    readonly publicationFailure?: true
  },
  E | Schema.SchemaError | AiError.AiError | HarnessError,
  R | R2 | Scope.Scope
> {
  const content: Content[] = []
  const texts = new Map<
    string,
    {
      type: 'text' | 'reasoning'
      text: string
      metadata: Response.ProviderMetadata
      ended: boolean
    }
  >()
  const parameters = new Map<string, { id: string; name: string; text: string; ended: boolean }>()
  let finish: Response.FinishPart | undefined
  let revision = 0
  let written = 0
  let active = true
  let stopping = false
  let latest: LiveState.Partial | undefined
  let failure: Cause.Cause<E> | undefined
  let wake = Deferred.makeUnsafe<void>()
  const stop = yield* Deferred.make<void>()
  const failed = yield* Deferred.make<never, E>()
  const snapshot = (): LiveState.Partial => ({
    message: Prompt.make([
      Prompt.assistantMessage({
        content: [...texts.values()].map((item) =>
          item.type === 'text'
            ? Prompt.textPart({ text: item.text, options: item.metadata })
            : Prompt.reasoningPart({ text: item.text, options: item.metadata }),
        ),
      }),
    ]),
    parameters: [...parameters.values()].map(({ ended: _ended, ...item }) => item),
    ...(finish === undefined ? {} : { usage: usage(finish.usage, preparation) }),
  })
  const save = Effect.gen(function* () {
    if (latest === undefined || written === revision || !active) return
    const value = latest
    const included = revision
    yield* publish(value, included)
    written = included
  })
  const worker = yield* Effect.gen(function* () {
    while (!stopping && active) {
      if (written === revision) {
        yield* Deferred.await(wake)
        wake = Deferred.makeUnsafe<void>()
        continue
      }
      const started = yield* Clock.currentTimeMillis
      let remaining = interval
      do {
        yield* Deferred.await(stop).pipe(
          Effect.timeoutOrElse({
            duration: Math.min(remaining, 0x7fffffff),
            orElse: () => Effect.void,
          }),
        )
        remaining = Math.max(0, interval - ((yield* Clock.currentTimeMillis) - started))
      } while (remaining > 0 && !stopping && active)
      if (stopping || !active) break
      yield* save
    }
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.gen(function* () {
        failure = cause
        yield* Deferred.failCause(failed, cause)
      }),
    ),
    Effect.forkScoped,
  )
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      active = false
    }),
  )
  const append = Effect.fnUntraced(function* (part: Part) {
    if (!active || stopping) return yield* invalid('Stream invocation has ended')
    if (finish !== undefined) return yield* invalid('Provider emitted content after finish')
    switch (part.type) {
      case 'text-start':
      case 'reasoning-start':
        if (texts.has(part.id)) return yield* invalid('Duplicate text/reasoning stream identity')
        texts.set(part.id, {
          type: part.type === 'text-start' ? 'text' : 'reasoning',
          text: '',
          metadata: part.metadata,
          ended: false,
        })
        break
      case 'text-delta':
      case 'reasoning-delta': {
        const item = texts.get(part.id)
        if (
          item === undefined ||
          item.ended ||
          item.type !== (part.type === 'text-delta' ? 'text' : 'reasoning')
        )
          return yield* invalid('Unmatched text/reasoning delta')
        item.text += part.delta
        item.metadata = { ...item.metadata, ...part.metadata }
        break
      }
      case 'text-end':
      case 'reasoning-end': {
        const item = texts.get(part.id)
        if (
          item === undefined ||
          item.ended ||
          item.type !== (part.type === 'text-end' ? 'text' : 'reasoning')
        )
          return yield* invalid('Unmatched text/reasoning end')
        item.ended = true
        item.metadata = { ...item.metadata, ...part.metadata }
        content.push(
          item.type === 'text'
            ? Response.makePart('text', { text: item.text, metadata: item.metadata })
            : Response.makePart('reasoning', { text: item.text, metadata: item.metadata }),
        )
        break
      }
      case 'tool-params-start':
        if (parameters.has(part.id) || part.providerExecuted)
          return yield* invalid('Invalid streamed tool parameters')
        parameters.set(part.id, { id: part.id, name: part.name, text: '', ended: false })
        break
      case 'tool-params-delta':
      case 'tool-params-end': {
        const item = parameters.get(part.id)
        if (item === undefined || item.ended) return yield* invalid('Unmatched tool parameter part')
        if (part.type === 'tool-params-end') item.ended = true
        else item.text += part.delta
        break
      }
      case 'tool-call':
        if (part.providerExecuted) return yield* invalid('Providers must not execute tools')
        content.push(part)
        break
      case 'tool-result':
      case 'tool-approval-request':
        return yield* invalid('Providers must return unresolved tool intents')
      case 'error':
        return yield* AiError.isAiError(part.error)
          ? Effect.fail(part.error)
          : Effect.fail(invalid('Provider emitted an untyped stream error'))
      case 'finish':
        finish = part
        content.push(part)
        break
      default:
        content.push(part)
    }
    if (revision === Number.MAX_SAFE_INTEGER) return yield* invalid('Partial revision exhausted')
    revision++
    latest = yield* Schema.decodeEffect(Schema.toCodecJson(importPartialSchema))(
      yield* Schema.encodeEffect(Schema.toCodecJson(importPartialSchema))(snapshot()),
    )
    yield* Deferred.succeed(wake, undefined)
  })
  const exit = yield* Effect.exit(
    Effect.raceFirst(Stream.runForEach(stream, append), Deferred.await(failed)),
  )
  yield* Effect.uninterruptible(
    Effect.gen(function* () {
      stopping = true
      yield* Deferred.succeed(stop, undefined)
      yield* Deferred.succeed(wake, undefined)
      yield* Fiber.join(worker)
    }),
  )
  if (failure !== undefined)
    return { exit: Exit.failCause(failure), partial: latest, publicationFailure: true }
  if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause))
    return yield* Effect.failCause(exit.cause)
  const saved = yield* Effect.exit(save)
  if (Exit.isFailure(saved))
    return { exit: Exit.failCause(saved.cause), partial: latest, publicationFailure: true }
  active = false
  if (Exit.isFailure(exit)) return { exit: Exit.failCause(exit.cause), partial: latest }
  if (
    finish === undefined ||
    [...texts.values()].some((item) => !item.ended) ||
    [...parameters.values()].some(
      (item) =>
        !item.ended ||
        !content.some(
          (part) => part.type === 'tool-call' && part.id === item.id && part.name === item.name,
        ),
    )
  )
    return {
      exit: Exit.fail(invalid('Stream ended without a complete terminal response')),
      partial: latest,
    }
  return {
    exit: Exit.succeed(new LanguageModel.GenerateTextResponse<Tools, 'encoded'>(content)),
    partial: latest,
  }
})
import { PartialSchema as importPartialSchema } from '../LiveState.js'
