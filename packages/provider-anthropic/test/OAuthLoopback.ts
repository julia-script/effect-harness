import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import * as NodeHttpServer from '@effect/platform-node/NodeHttpServer'
import * as HttpServer from 'effect/http/HttpServer'
import { createServer } from 'node:http'

/** The real native loopback listener is acquired and closed by its owning layer scope. */
export class OAuthLoopback extends Context.Service<
  OAuthLoopback,
  typeof HttpServer.HttpServer.Service
>()('test/OAuthLoopback') {
  static readonly layer = Layer.effect(OAuthLoopback)(Effect.service(HttpServer.HttpServer)).pipe(
    Layer.provideMerge(
      NodeHttpServer.layer(createServer, {
        host: '127.0.0.1',
        port: 53692,
        gracefulShutdownTimeout: '1 second',
      }),
    ),
  )
}
