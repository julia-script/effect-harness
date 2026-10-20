/**
 * The test's Workflow: each run answers its content in one `HarnessWorkflow.turn`, over an
 * OpenAI-compatible API at the `apiUrl` it starts with, since the test serves the model only once
 * the stack is up.
 */
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect, Layer, Redacted } from 'effect'
import { FetchHttpClient } from 'effect/http'
import type { UserInput } from '../../../harness/Submission.ts'
import { Model } from '../../../model/index.ts'
import { OpenAi } from '../../../openai/index.ts'
import { HarnessWorkflow } from '../../index.ts'

export interface AskInput {
  readonly content: UserInput
  readonly apiUrl: string
}

const model = (apiUrl: string) =>
  Model.layer([
    OpenAi.provider({
      name: 'local',
      apiUrl,
      apiKey: Redacted.make('sk-test'),
      models: [{ id: 'test-model' }],
    }),
  ]).pipe(Layer.provide(FetchHttpClient.layer))

export default class Ask extends Cloudflare.Workflow<Ask>()(
  'Ask',
  Effect.succeed((input: AskInput) =>
    HarnessWorkflow.turn('turn', input.content, {
      settings: { model: { provider: 'local', modelId: 'test-model' } },
    }).pipe(Effect.provide(model(input.apiUrl))),
  ),
) {}
