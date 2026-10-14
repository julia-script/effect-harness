/** Durable configuration binding shared by subscription providers; secrets are never configuration. */
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Model from '../Model.js'
import { ModelError, ModelUnsupportedError } from '../ModelError.js'
import * as Http from './SubscriptionHttp.js'
import * as Credential from '../provider-auth/Credential.js'

export const make = <A extends object, I>(input: {
  readonly settings: Schema.Codec<A, A>
  readonly tag: Context.Key<I, A>
  readonly defaults: A
  readonly binding: Credential.Binding
  readonly protocol: string
  readonly definition: Model.Definition
  readonly languageModel: typeof LanguageModel.LanguageModel.Service
  readonly capabilities: Schema.Json
  readonly summary: {
    readonly outputBudget: 'provider' | 'local-acceptance'
    readonly configure: (settings: A, maximum: number) => Effect.Effect<A, ModelError>
  }
}) =>
  Effect.gen(function* () {
    const configuration = yield* Model.makeConfiguration({
      id: input.protocol,
      version: 1,
      schema: Schema.Struct({
        protocol: Schema.String,
        binding: Credential.Binding,
        model: Model.ReferenceSchema,
        settings: input.settings,
        capabilities: Schema.Json,
      }),
      tags: [input.tag],
      capture: (context) =>
        Effect.succeed({
          protocol: input.protocol,
          binding: input.binding,
          model: input.definition.ref,
          settings: Context.getOrUndefined(context, input.tag) ?? input.defaults,
          capabilities: input.capabilities,
        }),
      restore: (snapshot) => {
        if (
          !Schema.toEquivalence(Schema.Json)(snapshot.capabilities, input.capabilities) ||
          snapshot.protocol !== input.protocol ||
          snapshot.binding.identity !== input.binding.identity ||
          snapshot.binding.provider !== input.binding.provider ||
          snapshot.binding.profile !== input.binding.profile ||
          snapshot.model.provider !== input.definition.ref.provider ||
          snapshot.model.modelId !== input.definition.ref.modelId
        )
          return Effect.fail(
            new ModelError({
              reason: new ModelUnsupportedError({
                message:
                  'Prepared subscription protocol/account/model binding changed; explicitly reprepare the request',
              }),
            }),
          )
        return Effect.succeed(Context.make(input.tag, snapshot.settings))
      },
    })
    const compaction = yield* Model.makeCompaction({
      binding: { id: `${input.protocol}/summary/${input.summary.outputBudget}`, version: 1 },
      configuration,
      outputBudget: input.summary.outputBudget,
      isOverflow: Http.isOverflow,
      configure: (context, policy) =>
        input.summary
          .configure(
            Context.getOrUndefined(context, input.tag) ?? input.defaults,
            policy.maxOutputTokens,
          )
          .pipe(Effect.map((settings) => Context.make(input.tag, settings))),
    })
    return Model.make({
      compaction,
      measurement: {
        identity: JSON.stringify([
          input.protocol,
          input.binding,
          input.definition,
          input.defaults,
          input.capabilities,
        ]),
        overheadTokens: input.binding.provider === 'anthropic' ? 64 : 32,
      },
      definition: input.definition,
      languageModel: input.languageModel,
      options: input.settings,
      configuration,
      version: 1,
      configure: (settings) =>
        Effect.succeed(Context.make(input.tag, { ...input.defaults, ...settings })),
    })
  })
