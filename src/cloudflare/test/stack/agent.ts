/**
 * The test's Durable Object: the harness with OpenAI-compatible accounts and a few Worker tools,
 * plus a `Reset` request that makes Cloudflare reset the object, as an eviction or a deploy would,
 * and a `Stats` request that reads the object's SQLite.
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
import * as Tools from './tools.ts'

export const Stats = Schema.Struct({
  databaseSize: Schema.Finite,
  largestValue: Schema.Finite,
  rows: Schema.Record(Schema.String, Schema.Finite),
  counter: Schema.Finite,
})

export class TestRpcs extends RpcGroup.make(
  Rpc.make('Reset', { success: Schema.Void }),
  Rpc.make('Stats', { success: Stats }),
) {}

export const AgentRpcs = HarnessObject.Rpcs.merge(TestRpcs)

const adapters = Adapter.group(OpenAiAdapter.adapter)

/** Small enough that a long session compacts several times. */
export const compaction = {
  modelOverrides: {
    'local/test-model': { reserveTokens: 120_000, keepRecentTokens: 2_000, backgroundTokens: 0 },
  },
}

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
      const stats = Effect.provideService(Tools.stats, Cloudflare.DurableObjectState, state)
      return Layer.mergeAll(
        HarnessObject.layer({
          adapters,
          extensions: [Tools.extension],
          settings: {
            model: { provider: 'local', modelId: 'test-model' },
            compaction,
            retry: { baseDelayMs: 200, maxDelayMs: 1_000 },
          },
          keepAwake: '1 second',
        }),
        TestRpcs.toLayer({ Reset: () => reset, Stats: () => stats }),
      ).pipe(
        Layer.provide(Tools.layer),
        Layer.provide([OpenAi.layerAdapter, FetchHttpClient.layer]),
      )
    })
  }),
) {}
