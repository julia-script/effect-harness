import * as Model from '@effect-harness/harness/Model'
import { ModelError, ModelNoModel, ModelUnsupported } from '@effect-harness/harness/Error'
import * as Usage from '@effect-harness/harness/Usage'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import type * as Response from 'effect/ai/Response'
import * as Provider from './LanguageModel.ts'
import * as RequestOptions from './RequestOptions.ts'

const Effort = Schema.Literals(['low', 'medium', 'high', 'xhigh', 'max'])
const Limit = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)
export const Entry = Schema.Struct({
  modelId: Schema.NonEmptyString,
  contextWindow: Limit,
  maxOutputTokens: Limit,
  efforts: Schema.optional(Schema.Array(Effort)),
  supportsThinkingOff: Schema.optional(Schema.Boolean),
}).check(Schema.makeFilter((entry) => entry.maxOutputTokens <= entry.contextWindow))
export type Entry = typeof Entry.Type
export interface Options {
  readonly models: ReadonlyArray<Entry>
  readonly provider?: string | undefined
  readonly cwd?: string | undefined
  readonly historyMode?: Provider.Options['historyMode']
}
const fail = (message: string, cause?: unknown) =>
  new ModelError({
    reason: new ModelUnsupported({ message, ...(cause === undefined ? {} : { cause }) }),
  })
const NativeOptions = Schema.Struct({ effort: Schema.optionalKey(Effort) })
const positive = (value: number) => Number.isSafeInteger(value) && value > 0
const metadata = Schema.Struct({
  claudeCode: Schema.Struct({
    totalCostUsd: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))),
    costUnavailable: Schema.optionalKey(Schema.Boolean),
  }),
})
const usage = (value: Response.Usage, provider: Response.ProviderMetadata): Usage.Usage => {
  const result = Schema.decodeUnknownResult(metadata)(provider)
  const cost =
    Result.isSuccess(result) && result.success.claudeCode.costUnavailable !== true
      ? result.success.claudeCode.totalCostUsd
      : undefined
  return Usage.fromResponse(
    value,
    cost === undefined
      ? {}
      : {
          cost: {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            total: cost,
            known: false,
            totalKnown: true,
          },
        },
  )
}

/** Native CLI catalogue. Transport and policy/history opt-ins remain explicit caller-owned Layers. */
export const descriptor = Effect.fnUntraced(function* (
  entry: Entry,
  options?: Omit<Options, 'models'>,
) {
  yield* Schema.decodeEffect(Entry)(entry).pipe(
    Effect.mapError((cause) => fail('Invalid CLI catalogue entry or declared limits', cause)),
  )
  const model = yield* Provider.make({
    model: entry.modelId,
    cwd: options?.cwd,
    historyMode: options?.historyMode,
  })
  const configure = Effect.fnUntraced(function* (request: Model.RequestOptions) {
    if (request.sessionId !== undefined)
      yield* Schema.decodeEffect(Schema.String.check(Schema.isUUID(7)))(request.sessionId).pipe(
        Effect.mapError((cause) => fail('Conversation sessionId must be UUID7', cause)),
      )
    const supplied = yield* Schema.decodeEffect(NativeOptions, {
      onExcessProperty: 'error',
    })(request.options).pipe(
      Effect.mapError((cause) => fail('Unsupported or invalid CLI request options', cause)),
    )
    let effort = supplied.effort
    let thinkingEnabled: boolean | undefined
    if (request.thinking === 'off') {
      if (entry.supportsThinkingOff !== true)
        return yield* fail(
          'This CLI model does not declare that thinking can be turned off; choose default or a declared effort',
        )
      if (supplied.effort !== undefined)
        return yield* fail('Do not combine thinking off with an effort override')
      thinkingEnabled = false
    } else if (request.thinking !== 'default') {
      const requested = yield* Schema.decodeUnknownEffect(Effort)(request.thinking).pipe(
        Effect.mapError((cause) =>
          fail('CLI thinking must be default, supported off, or a declared effort', cause),
        ),
      )
      if (effort !== undefined && effort !== requested)
        return yield* fail('Conflicting CLI effort and pinned thinking')
      effort = requested
      thinkingEnabled = true
    }
    if (effort !== undefined && !entry.efforts?.includes(effort))
      return yield* fail('Requested CLI effort is not declared supported by this model')
    if (
      request.maxTokens !== undefined &&
      (!positive(request.maxTokens) || request.maxTokens > entry.maxOutputTokens)
    )
      return yield* fail('maxTokens must be a positive integer within the declared CLI output cap')
    return Context.make(RequestOptions.Current, {
      model: entry.modelId,
      sessionId: request.sessionId,
      effort,
      thinkingEnabled,
      maxTokens: request.maxTokens ?? entry.maxOutputTokens,
      cache: request.cache,
      autoCompact: false,
    })
  })
  return {
    ref: { provider: options?.provider ?? 'claude-code', modelId: entry.modelId },
    model,
    contextWindow: entry.contextWindow,
    maxOutputTokens: entry.maxOutputTokens,
    configure,
    usage,
    classify: (error) => Model.classify(error, 'claude-code'),
  } satisfies Model.Descriptor
})
export const layer = (options: Options) =>
  Layer.effect(Model.Catalog)(
    Effect.gen(function* () {
      if (new Set(options.models.map((entry) => entry.modelId)).size !== options.models.length)
        return yield* fail('Duplicate CLI catalogue model IDs')
      const entries = yield* Effect.forEach(options.models, (entry) => descriptor(entry, options))
      const byId = new Map(entries.map((entry) => [entry.ref.modelId, entry]))
      return Model.Catalog.of({
        resolve: (ref) => {
          const found = byId.get(ref.modelId)
          return found !== undefined && found.ref.provider === ref.provider
            ? Effect.succeed(found)
            : Effect.fail(
                new ModelError({
                  reason: new ModelNoModel({
                    message: 'CLI model is not available in this catalogue',
                  }),
                }),
              )
        },
      })
    }),
  )
