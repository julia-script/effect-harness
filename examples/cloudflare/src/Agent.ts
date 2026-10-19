/**
 * The Durable Object: one harness loop per name, with its conversations, accounts and credentials
 * in the object's SQLite.
 */
import * as Cloudflare from 'alchemy/Cloudflare'
import { Effect, Layer } from 'effect'
import { FetchHttpClient } from 'effect/http'
import { Anthropic } from 'effect-harness/anthropic'
import { HarnessObject } from 'effect-harness/cloudflare'
import { OpenAi, OpenAiCodex } from 'effect-harness/openai'
import { adapters } from './adapters.ts'

export default class Agent extends Cloudflare.RpcDurableObject<Agent>()(
  'Agent',
  { schema: HarnessObject.Rpcs },
  // Alchemy's form: an Effect at deploy time that gives the Effect each activation runs.
  Effect.succeed(
    Effect.succeed(
      HarnessObject.layer({ adapters }).pipe(
        Layer.provide([
          OpenAiCodex.layerAdapter(),
          OpenAi.layerAdapter,
          Anthropic.layerAdapter,
          FetchHttpClient.layer,
        ]),
      ),
    ),
  ),
) {}
