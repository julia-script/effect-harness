/**
 * The test's Durable Object: the harness with OpenAI-compatible accounts, plus a `Reset` request
 * that makes Cloudflare reset the object, as an eviction or a deploy would.
 */
import * as Cloudflare from 'alchemy/Cloudflare'
import { RuntimeContext } from 'alchemy/RuntimeContext'
import { Effect, Layer, Schema } from 'effect'
import { FetchHttpClient } from 'effect/http'
import { Rpc, RpcGroup } from 'effect/rpc'
import { Adapter } from '../../../accounts/index.ts'
import { OpenAi } from '../../../openai/index.ts'
import { OpenAi as OpenAiAdapter } from '../../../openai/adapter/index.ts'
import { HarnessObject } from '../../index.ts'

export class TestRpcs extends RpcGroup.make(Rpc.make('Reset', { success: Schema.Void })) {}

export const AgentRpcs = HarnessObject.Rpcs.merge(TestRpcs)

const adapters = Adapter.group(OpenAiAdapter.adapter)

export default class Agent extends Cloudflare.RpcDurableObject<Agent>()(
  'Agent',
  { schema: AgentRpcs },
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState
    // Alchemy's form: the outer Effect returns the one each activation runs.
    // oxlint-disable-next-line effecttsgo/return-effect-in-gen
    return Effect.gen(function* () {
      const runtime = yield* RuntimeContext
      const reset = Effect.provideService(state.abort('reset by the test'), RuntimeContext, runtime)
      return Layer.mergeAll(
        HarnessObject.layer({
          adapters,
          settings: { model: { provider: 'local', modelId: 'test-model' } },
          keepAwake: '1 second',
        }),
        TestRpcs.toLayer({ Reset: () => reset }),
      ).pipe(Layer.provide([OpenAi.layerAdapter, FetchHttpClient.layer]))
    })
  }),
) {}
