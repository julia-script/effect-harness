/**
 * A watch outlives a restart of the loop: when the connection drops, the client opens the watch
 * again, and the next value it sees is the loop's state after the restart.
 */
import { BunHttpServer } from '@effect/platform-bun'
import { expect, test } from 'bun:test'
import { Effect, Exit, Fiber, Layer, Option, Schedule, Scope, Stream } from 'effect'
import { HttpRouter, HttpServer } from 'effect/http'
import { HarnessServer } from '../../harness/index.ts'
import { Faux, Model } from '../../model/index.ts'
import { Storage } from '../../storage/index.ts'
import { HarnessClient, HarnessTransport } from '../index.ts'

/** Serve a loop over `storage` at `/harness` on `port` (0: any) until `scope` closes; its port. */
const serve = (storage: Storage.Service, faux: Faux.Faux, port: number, scope: Scope.Scope) =>
  Effect.gen(function* () {
    const loop = HarnessServer.layer({ settings: { model: faux.ref() } }).pipe(
      Layer.provide([
        HarnessTransport.layerWebSocketServer('/harness'),
        Model.layer([faux.provider]),
        Layer.succeed(Storage.Storage, storage),
      ]),
    )
    const server = HttpRouter.serve(loop, { disableListenLog: true, disableLogger: true }).pipe(
      // Stop at once, as a crash would, instead of waiting for clients to leave.
      Layer.provideMerge(
        BunHttpServer.layer({ port, hostname: '127.0.0.1', gracefulShutdownTimeout: 0 }),
      ),
    )
    const context = yield* Layer.buildWithScope(server, scope)
    const { address } = yield* Effect.provideContext(HttpServer.HttpServer, context)
    return 'port' in address ? address.port : yield* Effect.die('expected a TCP address')
  })

test(
  'a view watch resumes after the loop restarts',
  () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const storage = yield* Storage.makeMemory
          const faux = yield* Faux.make()
          yield* faux.setResponses([Faux.message('before'), Faux.message('after')])
          const first = yield* Scope.make()
          const port = yield* serve(storage, faux, 0, first)
          const transport = yield* Layer.build(
            HarnessTransport.layerWebSocket(`ws://127.0.0.1:${port}/harness`),
          )
          const client = yield* Effect.provideContext(HarnessClient.make, transport)
          const root = yield* client.root()
          const ask = (content: string) =>
            Effect.flatMap(root.submit({ type: 'input', content }), (submission) => submission.wait)
          yield* ask('one')
          const view = yield* root.view
          const grown = yield* Effect.forkChild(
            view.changes.pipe(
              Stream.filter((change) => change.value.entries.length >= 4),
              Stream.runHead,
            ),
          )

          // Stop the loop, then start another on the same port and store.
          yield* Scope.close(first, Exit.void)
          yield* serve(storage, faux, port, yield* Effect.scope)
          // Calls fail until the client has reconnected.
          yield* ask('two').pipe(
            Effect.retry(
              Schedule.spaced('100 millis').pipe(Schedule.upTo({ duration: '10 seconds' })),
            ),
          )

          const change = yield* Fiber.join(grown)
          const kinds = Option.map(change, ({ value }) => value.entries.map((entry) => entry.kind))
          expect(Option.getOrUndefined(kinds)).toEqual([
            '$user',
            '$assistant',
            '$user',
            '$assistant',
          ])
        }),
      ),
    ),
  30_000,
)
