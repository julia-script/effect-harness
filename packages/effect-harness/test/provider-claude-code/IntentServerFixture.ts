import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as NodeHttpServer from '@effect/platform-node/NodeHttpServer'
import * as FetchHttpClient from 'effect/http/FetchHttpClient'
import * as HttpClient from 'effect/http/HttpClient'
import { createServer } from 'node:http'
import * as IntentServer from 'effect-harness/provider-claude-code/IntentServer'

/** Scoped native loopback fixture; the native server layer owns listener cleanup. */
export class IntentServerFixture extends Context.Service<
  IntentServerFixture,
  {
    readonly server: typeof IntentServer.IntentServer.Service
    readonly client: HttpClient.HttpClient
  }
>()('effect-harness/test/provider-claude-code/IntentServerFixture/IntentServerFixture') {
  static readonly layer = Layer.effect(IntentServerFixture)(
    Effect.gen(function* () {
      return { server: yield* IntentServer.IntentServer, client: yield* HttpClient.HttpClient }
    }),
  ).pipe(
    Layer.provideMerge(
      IntentServer.layer.pipe(
        Layer.provide(
          NodeHttpServer.layer(createServer, {
            host: '127.0.0.1',
            port: 0,
            gracefulShutdownTimeout: '1 second',
          }),
        ),
        Layer.merge(FetchHttpClient.layer),
      ),
    ),
  )
}
