/** Scoped loopback listener; importing this module never opens a socket. */
import * as Http from 'node:http'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Layer from 'effect/Layer'
import { CallbackServer } from '../provider-auth/Login.js'
import { AuthError } from '../provider-auth/AuthError.js'

export const layer = Layer.succeed(CallbackServer, {
  open: (options) =>
    Effect.gen(function* () {
      const completion = yield* Deferred.make<string, AuthError>()
      const server = Http.createServer((request, response) => {
        let url: URL
        try {
          url = new URL(request.url ?? '/', 'http://localhost')
        } catch {
          response.writeHead(400).end('Invalid login callback')
          return
        }
        if (request.method !== 'GET' || url.pathname !== options.path) {
          response.writeHead(404).end()
          return
        }
        if (Deferred.isDoneUnsafe(completion)) {
          response.writeHead(409).end('Login already completed')
          return
        }
        if (
          url.searchParams.get('state') !== options.state ||
          !url.searchParams.get('code') ||
          url.searchParams.has('error')
        ) {
          response.writeHead(400).end('Invalid login callback')
          Deferred.doneUnsafe(completion, Effect.fail(new AuthError({ reason: 'InvalidState' })))
          return
        }
        Deferred.doneUnsafe(completion, Effect.succeed(url.href))
        response
          .writeHead(200, { 'content-type': 'text/plain', 'cache-control': 'no-store' })
          .end('Login received. You can close this window.')
      })
      yield* Effect.addFinalizer(() =>
        Effect.callback<void>((resume) => {
          server.closeAllConnections()
          server.close(() => resume(Effect.void))
        }),
      )
      const listen = (port: number) =>
        Effect.callback<void, AuthError>((resume) => {
          const onError = () => resume(Effect.fail(new AuthError({ reason: 'TransportFailure' })))
          server.once('error', onError)
          server.listen(port, '127.0.0.1', () => {
            server.off('error', onError)
            resume(Effect.void)
          })
          return Effect.sync(() => {
            server.off('error', onError)
          })
        })
      yield* listen(options.port).pipe(
        Effect.catch((error) => (options.fallbackPort ? listen(0) : Effect.fail(error))),
      )
      const address = server.address()
      if (address === null || typeof address === 'string')
        return yield* new AuthError({ reason: 'TransportFailure' })
      return {
        redirectUri: `http://localhost:${address.port}${options.path}`,
        completion: Deferred.await(completion),
      }
    }),
})
