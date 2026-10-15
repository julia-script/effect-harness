/** Reusable Chat Completions transport with consumer-owned metadata discovery. */
import * as Effect from 'effect/Effect'
import * as Context from 'effect/Context'
import * as Schema from 'effect/Schema'
import * as Native from '@effect/ai-openai-compat/OpenAiLanguageModel'
import * as Client from '@effect/ai-openai-compat/OpenAiClient'
import * as Provider from '../Provider.js'
import * as Model from '../Model.js'
import * as ModelCatalog from '../ModelCatalog.js'
import * as ModelInfo from '../ModelInfo.js'
import * as Credential from '../provider-auth/Credential.js'
import { ModelError, ModelUnsupportedError } from '../ModelError.js'
import { ModelCatalogError } from '../ModelCatalogError.js'
import { AuthError } from '../provider-auth/AuthError.js'

export const Settings = Schema.Struct({
  temperature: Schema.optionalKey(
    Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 2 })),
  ),
  max_output_tokens: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  reasoning: Schema.optionalKey(Schema.Struct({ effort: Schema.NonEmptyString })),
})
export type Settings = typeof Settings.Type
export interface Connection {
  readonly baseUrl: string
  /** Explicitly permit anonymous gateways. Missing API keys otherwise require authentication. */
  readonly authentication?: 'required' | 'optional'
}

/** Declare endpoint configuration and model metadata while sharing inference mechanics. */
export const define = <C extends Provider.Codec, LE, LR, GE, GR>(input: {
  readonly version: number
  readonly config: C
  readonly connection: (config: C['Type']) => Connection
  readonly models: {
    readonly list: (
      context: Provider.Context<C['Type'], Credential.ApiKey>,
      input: ModelCatalog.ListOptions,
    ) => Effect.Effect<ModelCatalog.Page, LE, LR>
    readonly get: (
      context: Provider.Context<C['Type'], Credential.ApiKey>,
      modelId: string,
    ) => Effect.Effect<ModelInfo.ModelInfo, GE, GR>
  }
}) =>
  Provider.define({
    version: input.version,
    config: input.config,
    secrets: Credential.ApiKey,
    models: input.models,
    createModel: Effect.fnUntraced(function* (
      context: Provider.Context<C['Type'], Credential.ApiKey>,
      resolved: Provider.Resolved,
    ) {
      if (resolved.options.reasoning?.thinking !== undefined)
        return yield* new ModelCatalogError({
          reason: 'unsupportedOptions',
          modelId: resolved.info.id,
          message: 'Chat Completions does not support the portable thinking control',
        })
      const connection = input.connection(context.config)
      const secrets = yield* context.secrets
      if (secrets === undefined && connection.authentication !== 'optional')
        return yield* new AuthError({ reason: 'MissingCredential' })
      const client = yield* Client.make({ apiUrl: connection.baseUrl, apiKey: secrets?.apiKey })
      const effort = resolved.options.reasoning?.effort ?? resolved.info.reasoning?.effort?.default
      const defaults: Settings = effort === undefined ? {} : { reasoning: { effort } }
      const languageModel = yield* Native.make({ model: resolved.info.id, config: defaults }).pipe(
        Effect.provideService(Client.OpenAiClient, client),
      )
      const validate = (settings: Settings) =>
        ModelInfo.validateOptions(
          resolved.info,
          settings.reasoning === undefined ? {} : { reasoning: settings.reasoning },
        ).pipe(
          Effect.as(settings),
          Effect.mapError(
            () =>
              new ModelError({
                reason: new ModelUnsupportedError({
                  message: 'Unsupported gateway reasoning effort',
                }),
              }),
          ),
        )
      const configuration = yield* Model.makeConfiguration({
        id: 'effect-harness/openai-compatible/settings',
        version: 1,
        schema: Settings,
        tags: [Native.Config],
        capture: (context) =>
          Schema.decodeUnknownEffect(Settings, { onExcessProperty: 'error' })(
            Context.getOrUndefined(context, Native.Config) ?? defaults,
          ).pipe(Effect.flatMap(validate)),
        restore: (value) =>
          validate(value).pipe(Effect.map((settings) => Context.make(Native.Config, settings))),
      })
      return Model.make({
        definition: yield* ModelInfo.toDefinition(resolved.info, 'openai-compatible'),
        languageModel,
        options: Settings,
        configuration,
        configure: (settings) =>
          validate({ ...defaults, ...settings }).pipe(
            Effect.map((value) => Context.make(Native.Config, value)),
          ),
      })
    }),
  })

/** Static metadata for private gateways that offer no discovery endpoint. */
export const Config = Schema.Struct({
  baseUrl: Schema.NonEmptyString,
  authentication: Schema.optionalKey(Schema.Literals(['required', 'optional'])),
  models: Schema.Array(ModelInfo.ModelInfoSchema),
})
export const definition = define({
  version: 1,
  config: Config,
  connection: (config) => ({
    baseUrl: config.baseUrl,
    ...(config.authentication === undefined ? {} : { authentication: config.authentication }),
  }),
  models: ModelCatalog.fromConfig((config: typeof Config.Type) => config.models),
})
