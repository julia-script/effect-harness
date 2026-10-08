import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Prompt from 'effect/ai/Prompt'
import * as AiError from 'effect/ai/AiError'
import * as HttpClient from 'effect/http/HttpClient'
import * as OpenAiClient from '@effect/ai-openai/OpenAiClient'
import * as ChatGptLanguageModel from 'effect-harness/provider-openai/ChatGptLanguageModel'
import * as OpenAiLanguageModel from 'effect-harness/provider-openai/OpenAiLanguageModel'

class RequestAudit extends Context.Service<RequestAudit, { readonly identity: object }>()(
  'effect-harness/test/provider-openai/ChatGptRequestPolicy/RequestAudit',
) {}

describe('ChatGptRequestPolicy', () => {
  it.effect(
    'all SDK decorators retain request services, flat errors, defects and pre-serialization account policy',
    () =>
      Effect.gen(function* () {
        const identity = {}
        const defect = new Error('native SDK defect')
        const typed = new AiError.AiError({
          module: 'fixture',
          method: 'request',
          reason: new AiError.UnknownError({ description: 'native SDK typed failure' }),
        })
        let failure: 'defect' | 'typed' = 'defect'
        const captured: Array<
          Parameters<typeof OpenAiClient.OpenAiClient.Service.createResponse>[0]
        > = []
        const invoke = (
          options: Parameters<typeof OpenAiClient.OpenAiClient.Service.createResponse>[0],
        ) =>
          Effect.context<never>().pipe(
            Effect.flatMap((context) => {
              assert.strictEqual(Context.getOrUndefined(context, RequestAudit)?.identity, identity)
              captured.push(options)
              return failure === 'defect' ? Effect.die(defect) : Effect.fail(typed)
            }),
          )
        const client = OpenAiClient.OpenAiClient.of({
          client: HttpClient.make(() => Effect.die('Unexpected HTTP boundary')),
          createResponse: invoke,
          createResponseStream: invoke,
          createEmbedding: () => Effect.die('Unexpected embedding boundary'),
        })
        const model = yield* ChatGptLanguageModel.make({ model: 'captured-model' }).pipe(
          Effect.provideService(OpenAiClient.OpenAiClient, client),
        )
        const prompt = Prompt.fromMessages([
          Prompt.userMessage({ content: [Prompt.textPart({ text: 'first question' })] }),
          Prompt.assistantMessage({
            content: [
              Prompt.textPart({
                text: 'first answer',
                options: { openai: { itemId: 'old-item' } },
              }),
            ],
          }),
          Prompt.userMessage({ content: [Prompt.textPart({ text: 'next question' })] }),
        ])
        for (const mode of ['defect', 'typed'] as const) {
          failure = mode
          for (const request of [
            model.generateText({ prompt }).pipe(Effect.asVoid),
            model
              .generateObject({ prompt, schema: Schema.Struct({ answer: Schema.String }) })
              .pipe(Effect.asVoid),
            model.streamText({ prompt }).pipe(Stream.runDrain),
          ]) {
            const exit = yield* request.pipe(
              Effect.provideService(RequestAudit, { identity }),
              Effect.provideService(
                OpenAiLanguageModel.Config,
                OpenAiLanguageModel.Config.of({ store: true, useItemReferences: true }),
              ),
              Effect.exit,
            )
            assert.isTrue(Exit.isFailure(exit))
            if (Exit.isFailure(exit))
              assert.strictEqual(Cause.squash(exit.cause), mode === 'defect' ? defect : typed)
          }
        }
        assert.strictEqual(captured.length, 6)
        for (const payload of captured) {
          assert.strictEqual(payload.model, 'captured-model')
          assert.strictEqual(payload.store, false)
          const serialized = JSON.stringify(payload.input)
          assert.include(serialized, 'first question')
          assert.include(serialized, 'first answer')
          assert.include(serialized, 'next question')
          assert.notInclude(serialized, 'item_reference')
        }
      }),
  )
})
