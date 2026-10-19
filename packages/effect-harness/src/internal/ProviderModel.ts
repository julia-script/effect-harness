/** Instance identity is retained across runtime selection, preparation, compaction and recovery. */
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import type * as Tool from 'effect/ai/Tool'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Model from '../Model.js'
import type * as Provider from '../Provider.js'
import { ModelError, ModelUnsupportedError } from '../ModelError.js'
import { toAiError } from '../provider-auth/AuthError.js'

export const bind = (model: Model.Any, instance: Provider.Context<unknown, unknown>) => {
  const check = instance.check.pipe(Effect.mapError(toAiError))
  const configuration = (original: Model.Configuration): Model.Configuration => ({
    binding: {
      id: `effect-harness/provider-instance/${original.binding.id}`,
      version: original.binding.version,
    },
    encode: (context) =>
      Effect.map(original.encode(context), (value) => ({
        instanceId: instance.id,
        revision: instance.revision,
        value,
      })),
    restore: (input) =>
      Effect.gen(function* () {
        const snapshot = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ instanceId: Schema.String, revision: Schema.String, value: Schema.Json }),
        )(input).pipe(
          Effect.mapError(
            () =>
              new ModelError({
                reason: new ModelUnsupportedError({
                  message: 'Invalid provider instance configuration',
                }),
              }),
          ),
        )
        if (snapshot.instanceId !== instance.id || snapshot.revision !== instance.revision)
          return yield* new ModelError({
            reason: new ModelUnsupportedError({
              message: 'Provider instance changed; explicitly reprepare the request',
            }),
          })
        return yield* original.restore(snapshot.value)
      }),
  })
  // Forward the native overloads unchanged while fencing each operation before dispatch.
  const languageModel: typeof LanguageModel.LanguageModel.Service = {
    ...model.languageModel,
    generateText: (<Options extends Parameters<typeof model.languageModel.generateText>[0]>(
      options: Options,
    ) =>
      Effect.andThen(
        check,
        model.languageModel.generateText(options),
      )) as typeof model.languageModel.generateText,
    generateObject: (<S extends Schema.Encoder<Record<string, unknown>, unknown>>(
      options: LanguageModel.GenerateObjectOptions<Record<string, Tool.Any>, S>,
    ) =>
      Effect.andThen(
        check,
        model.languageModel.generateObject(options),
      )) as typeof model.languageModel.generateObject,
    streamText: (<Options extends Parameters<typeof model.languageModel.streamText>[0]>(
      options: Options,
    ) =>
      Stream.unwrap(
        Effect.as(check, model.languageModel.streamText(options)),
      )) as typeof model.languageModel.streamText,
  }
  const deferred = model.deferred
  return {
    ...model,
    definition: { ...model.definition, ref: { ...model.definition.ref, instanceId: instance.id } },
    languageModel,
    configuration: configuration(model.configuration ?? Model.emptyConfiguration),
    ...(model.measurement === undefined
      ? {}
      : {
          measurement: {
            ...model.measurement,
            identity: JSON.stringify([instance.id, instance.revision, model.measurement.identity]),
          },
        }),
    ...(model.compaction === undefined
      ? {}
      : {
          compaction: {
            ...model.compaction,
            configuration: configuration(model.compaction.configuration),
          },
        }),
    ...(deferred === undefined
      ? {}
      : {
          deferred: {
            start: (<Request extends Parameters<typeof deferred.start>[0]>(request: Request) =>
              Effect.andThen(check, deferred.start(request))) as typeof deferred.start,
            poll: ((handle, at, request) =>
              Effect.andThen(check, deferred.poll(handle, at, request))) as typeof deferred.poll,
            cancel: (handle: Schema.Json, context?: import('effect/Context').Context<never>) =>
              Effect.andThen(check, deferred.cancel(handle, context)),
          },
        }),
  }
}
