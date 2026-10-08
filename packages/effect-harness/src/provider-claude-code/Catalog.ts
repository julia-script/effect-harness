const DescriptorTypeId = '~effect-harness/provider-claude-code/Catalog/Descriptor'

/**
 * Validated model catalogues with pinned request configuration and usage accounting.
 */
import * as Pipeable from 'effect/Pipeable'
import * as Inspectable from 'effect/Inspectable'
import * as Predicate from 'effect/Predicate'
import { dual, constUndefined } from 'effect/Function'
import * as Arr from 'effect/Array'
import type * as AiError from 'effect/ai/AiError'
import type * as Cli from './Cli.ts'
import type * as IntentServer from './IntentServer.ts'
import * as HashMap from 'effect/HashMap'
import * as Option from 'effect/Option'
import * as Model from 'effect-harness/Model'
import { ModelError, ModelNoModelError, ModelUnsupportedError } from 'effect-harness/ModelError'
import * as Usage from 'effect-harness/Usage'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as Config from 'effect/Config'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import type * as Response from 'effect/ai/Response'
import * as ClaudeCodeLanguageModel from './ClaudeCodeLanguageModel.ts'
import * as RequestOptions from './RequestOptions.ts'

const Effort = Schema.Literals(['low', 'medium', 'high', 'xhigh', 'max'])
const Limit = Schema.Int.check(
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
)
/**
 * Schema for a caller-declared model and its supported request capabilities.
 *
 * **Details**
 *
 * contextWindow and maxOutputTokens must be positive and the output limit cannot exceed the
 * context window. Optional capability fields control accepted request options.
 *
 * **Gotchas**
 *
 * Entries describe application policy; they do not discover current remote models or account
 * entitlements.
 *
 * @category models
 */
export const Entry = Schema.Struct({
  modelId: Schema.NonEmptyString,
  contextWindow: Limit,
  maxOutputTokens: Limit,
  efforts: Schema.optional(Schema.Array(Effort)),
  supportsThinkingOff: Schema.optional(Schema.Boolean),
}).check(Schema.makeFilter((entry) => entry.maxOutputTokens <= entry.contextWindow))
/**
 * Caller-declared model identity, token limits and supported request capabilities.
 *
 * @category models
 */
export type Entry = typeof Entry.Type
/**
 * Type-level contracts for `Catalog`.
 */
export declare namespace Catalog {
  /**
   * Declared catalogue entries and defaults for provider model construction.
   *
   * @category models
   */
  export interface Options {
    readonly models: ReadonlyArray<Entry>
    readonly provider?: string | undefined
    readonly cwd?: string | undefined
    readonly historyMode?:
      | ClaudeCodeLanguageModel.ClaudeCodeLanguageModel.Options['historyMode']
      | undefined
  }
}
const fail = (message: string, cause?: unknown) =>
  new ModelError({
    reason: new ModelUnsupportedError({ message, ...(cause === undefined ? {} : { cause }) }),
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
  const cost = Result.getOrElse(
    Result.map(result, (self) =>
      self.claudeCode.costUnavailable !== true ? self.claudeCode.totalCostUsd : undefined,
    ),
    constUndefined,
  )
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

/**
 * Native model with validated provider configuration, usage accounting and error
 * classification.
 *
 * **Details**
 *
 * This owned handle supports piping and bounded inspection. `toJSON` is a diagnostic
 * projection; use the original fields for protocol values and resource references.
 *
 * @category models
 */
export interface Descriptor extends Pipeable.Pipeable, Inspectable.Inspectable {
  readonly [DescriptorTypeId]: typeof DescriptorTypeId
  readonly ref: Model.Descriptor['ref']
  readonly model: Model.Descriptor['model']
  readonly contextWindow: number
  readonly maxOutputTokens: number
  readonly configure: (
    options: Model.RequestOptions,
  ) => Effect.Effect<Context.Context<never>, ModelError>
  readonly usage: NonNullable<Model.Descriptor['usage']>
  readonly classify: NonNullable<Model.Descriptor['classify']>
}

/**
 * Checks the established nominal `Descriptor` marker; it does not validate arbitrary payload fields.
 *
 * @category guards
 */
export const isDescriptor = (u: unknown): u is Descriptor =>
  Predicate.hasProperty(u, DescriptorTypeId) && u[DescriptorTypeId] === DescriptorTypeId

/**
 * Owns a `Descriptor` handle while preserving payload descriptors and exact resource references.
 *
 * **Details**
 *
 * Construction and diagnostics do not evaluate payload accessors. Inspection is a bounded
 * diagnostic projection; read the original fields for protocol values.
 *
 * @category constructors
 */
export const makeDescriptor = (
  input: Omit<
    Descriptor,
    typeof DescriptorTypeId | keyof Pipeable.Pipeable | keyof Inspectable.Inspectable
  >,
): Descriptor => {
  const handle: Descriptor = Object.create(DescriptorProto)
  const descriptors = Object.getOwnPropertyDescriptors(input)
  // The owned protocol cannot be replaced by extra runtime payload keys.
  for (const key of [DescriptorTypeId, 'pipe', 'toJSON', 'toString', Inspectable.NodeInspectSymbol])
    Reflect.deleteProperty(descriptors, key)
  Object.defineProperties(handle, descriptors)
  Object.defineProperty(handle, DescriptorTypeId, { value: DescriptorTypeId, enumerable: false })
  return handle
}

const DescriptorProto = {
  ...Pipeable.Prototype,
  ...Inspectable.BaseProto,
  toJSON(): unknown {
    return {
      _id: 'effect-harness/provider-claude-code/Catalog/Descriptor',
      model: '<LanguageModel>',
      configure: '<function>',
    }
  },
}

/** Native CLI catalogue. Transport and policy/history opt-ins remain explicit caller-owned Layers. */
const descriptorImpl = Effect.fnUntraced(function* (
  self: Entry,
  options?: Omit<Catalog.Options, 'models'>,
): Effect.fn.Return<Descriptor, ModelError | AiError.AiError, Cli.Cli | IntentServer.IntentServer> {
  yield* Schema.decodeEffect(Entry)(self).pipe(
    Effect.mapError((cause) => fail('Invalid CLI catalogue entry or declared limits', cause)),
  )
  const model = yield* ClaudeCodeLanguageModel.make({
    model: self.modelId,
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
      if (self.supportsThinkingOff !== true)
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
    if (effort !== undefined && !self.efforts?.includes(effort))
      return yield* fail('Requested CLI effort is not declared supported by this model')
    if (
      request.maxTokens !== undefined &&
      (!positive(request.maxTokens) || request.maxTokens > self.maxOutputTokens)
    )
      return yield* fail('maxTokens must be a positive integer within the declared CLI output cap')
    return Context.make(RequestOptions.Current, {
      model: self.modelId,
      sessionId: request.sessionId,
      effort,
      thinkingEnabled,
      maxTokens: request.maxTokens ?? self.maxOutputTokens,
      cache: request.cache,
      autoCompact: false,
    })
  })
  return makeDescriptor({
    ref: { provider: options?.provider ?? 'claude-code', modelId: self.modelId },
    model,
    contextWindow: self.contextWindow,
    maxOutputTokens: self.maxOutputTokens,
    configure,
    usage,
    classify: (error) => Model.classify(error, 'claude-code'),
  }) satisfies Model.Descriptor
})
/**
 * Creates a model descriptor from a validated catalogue entry.
 *
 * **Details**
 *
 * Binds native model configuration, usage accounting and error classification to declared
 * capabilities. Unsupported request options fail with ModelError.
 *
 * @category constructors
 */
export const descriptor: {
  (): (self: Entry) => ReturnType<typeof descriptorImpl>
  (
    options: NonNullable<Parameters<typeof descriptorImpl>[1]> &
      (
        | { readonly provider: NonNullable<Parameters<typeof descriptorImpl>[1]>['provider'] }
        | { readonly cwd: NonNullable<Parameters<typeof descriptorImpl>[1]>['cwd'] }
        | { readonly historyMode: NonNullable<Parameters<typeof descriptorImpl>[1]>['historyMode'] }
      ),
  ): (self: Entry) => ReturnType<typeof descriptorImpl>
  (self: Entry, options?: Parameters<typeof descriptorImpl>[1]): ReturnType<typeof descriptorImpl>
  // Empty objects are malformed subjects, not meaningful curried options.
  // The data-last form requires a known supplied option key, or no argument for defaults.
} = dual(
  Predicate.or(
    (args: IArguments) => args.length >= 2,
    Predicate.and(
      (args: IArguments) => args.length === 1,
      Predicate.mapInput(
        Predicate.and(
          Predicate.isNotUndefined,
          Predicate.not(
            Predicate.and(
              Predicate.isObjectOrArray,
              Predicate.and(
                Predicate.not(Predicate.hasProperty('modelId')),
                Predicate.or(
                  Predicate.hasProperty('provider'),
                  Predicate.or(Predicate.hasProperty('cwd'), Predicate.hasProperty('historyMode')),
                ),
              ),
            ),
          ),
        ),
        (args: IArguments) => args[0],
      ),
    ),
  ),
  descriptorImpl,
)

/**
 * Provides a Model.Catalog from declared provider model entries.
 *
 * **Details**
 *
 * Validates entries and resolves only the registered provider/model pairs. Duplicate model
 * IDs are rejected; unknown references fail with ModelNoModelError.
 *
 * **Gotchas**
 *
 * Supply the required native client or CLI services. Catalogue construction does not
 * authorize a remote account.
 *
 * @category layers
 */
export const layer = (
  options: Catalog.Options,
): Layer.Layer<Model.Catalog, ModelError | AiError.AiError, Cli.Cli | IntentServer.IntentServer> =>
  Layer.effect(Model.Catalog)(
    Effect.gen(function* () {
      if (Arr.dedupe(options.models.map((entry) => entry.modelId)).length !== options.models.length)
        return yield* fail('Duplicate CLI catalogue model IDs')
      const entries = yield* Effect.forEach(options.models, (entry) => descriptor(entry, options))
      const byId = HashMap.fromIterable(entries.map((entry) => [entry.ref.modelId, entry]))
      return Model.Catalog.of({
        resolve: (ref) => {
          const found = HashMap.get(byId, ref.modelId)
          return Effect.fromOption(
            Option.filter(found, (self) => self.ref.provider === ref.provider),
            () =>
              new ModelError({
                reason: new ModelNoModelError({
                  message: 'CLI model is not available in this catalogue',
                }),
              }),
          )
        },
      })
    }),
  )

/** Checks the decoded Entry contract without decoding or coercing input.
 * @category guards
 */
export const isEntry: (u: unknown) => u is Entry = Schema.is(Schema.toType(Entry))
/**
 * Provides declared CLI models from the caller's ConfigProvider.
 *
 * @category layers
 */
export const layerConfig = (
  config: Config.Wrap<Catalog.Options>,
): Layer.Layer<
  Model.Catalog,
  ModelError | AiError.AiError | Config.ConfigError,
  Cli.Cli | IntentServer.IntentServer
> => Layer.unwrap(Config.unwrap(config).pipe(Effect.map(layer)))
