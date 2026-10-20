/**
 * A Harness loop in its own process, for `split.test.ts`: it reads credentials from the central
 * store at the URL in its first argument, answers each request with the `faux` API key it read,
 * and serves `HarnessRpcs` at `/harness` on an ephemeral port, which it prints as JSON.
 */
import { BunHttpServer, BunRuntime } from '@effect/platform-bun'
import { Console, Effect, Layer, Redacted } from 'effect'
import { HttpRouter, HttpServer } from 'effect/http'
import { HarnessTransport } from '../../index.ts'
import { Credentials } from '../../../credentials/index.ts'
import { HarnessServer } from '../../../harness/index.ts'
import { Faux, Model } from '../../../model/index.ts'
import { ModelError, Rejected } from '../../../model/ModelError.ts'
import { Storage } from '../../../storage/index.ts'

const credentialsUrl = process.argv[2] ?? ''

/** Reply with the key of the stored `faux` credential, read fresh for this request. */
const answer = (store: Credentials.Service) => () =>
  store.get('faux').pipe(
    Effect.map((credential) =>
      Faux.message(
        credential?.type === 'api_key' ? `key ${Redacted.value(credential.key)}` : 'no key',
      ),
    ),
    Effect.mapError(
      (cause) => new ModelError({ message: cause.message, reason: new Rejected(), cause }),
    ),
  )

const loop = Effect.gen(function* () {
  const store = yield* Credentials.Credentials
  const faux = yield* Faux.make()
  yield* faux.setResponses(Array.from({ length: 50 }, () => answer(store)))
  const harness = HarnessServer.layer({ settings: { model: faux.ref() } }).pipe(
    Layer.provide([
      HarnessTransport.layerWebSocketServer('/harness'),
      Model.layer([faux.provider]),
      Storage.layerMemory,
    ]),
  )
  const server = HttpRouter.serve(harness, { disableListenLog: true, disableLogger: true }).pipe(
    Layer.provideMerge(BunHttpServer.layer({ port: 0, hostname: '127.0.0.1' })),
  )
  const context = yield* Layer.build(server)
  const { address } = yield* Effect.provideContext(HttpServer.HttpServer, context)
  if (!('port' in address)) return yield* Effect.die('expected a TCP address')
  yield* Console.log(JSON.stringify({ port: address.port }))
  return yield* Effect.never
})

const main = loop.pipe(
  Effect.scoped,
  // The process's entry point.
  // oxlint-disable-next-line effecttsgo/strict-effect-provide
  Effect.provide(
    Credentials.layerRpc.pipe(Layer.provide(HarnessTransport.layerWebSocket(credentialsUrl))),
  ),
)

BunRuntime.runMain(main)
