import { AuthError, type OAuth } from '@effect-harness/auth/Credential'
import * as Context from 'effect/Context'
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Ref from 'effect/Ref'
import * as Layer from 'effect/Layer'
import * as HttpServer from 'effect/http/HttpServer'
import * as HttpServerRequest from 'effect/http/HttpServerRequest'
import * as HttpServerResponse from 'effect/http/HttpServerResponse'
import { ChatGpt, type Authorization } from './ChatGpt.ts'

export class Callback extends Context.Service<
  Callback,
  {
    readonly authorization: Authorization
    readonly await: Effect.Effect<OAuth, AuthError>
  }
>()('@effect-harness/provider-openai/Callback') {}

/** Starts the listener before exposing the authorization URL; callers provide a scoped loopback HttpServer adapter. */
export const layer = (options?: { readonly account?: string | undefined }) =>
  Layer.effect(Callback)(
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer
      const auth = yield* ChatGpt
      if (
        server.address._tag !== 'InetAddressV4' ||
        server.address.address.toString() !== '127.0.0.1'
      )
        return yield* new AuthError({
          reason: 'configuration',
          message: 'OAuth callback server must bind 127.0.0.1',
        })
      const redirectUri = `http://127.0.0.1:${server.address.port}/auth/callback`
      const result = yield* Deferred.make<OAuth, AuthError>()
      const claimed = yield* Ref.make(false)
      const authorization = yield* auth.begin({ redirectUri, account: options?.account })
      yield* Effect.addFinalizer(() => auth.cancel(authorization.state))
      yield* server.serve(
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest
          const parsed = yield* Effect.try({
            try: () => new URL(request.url, redirectUri),
            catch: () => new AuthError({ reason: 'callback', message: 'Invalid callback URL' }),
          }).pipe(Effect.option)
          if (parsed._tag === 'None') return HttpServerResponse.empty({ status: 400 })
          const url = parsed.value
          if (request.method !== 'GET' || url.pathname !== '/auth/callback')
            return HttpServerResponse.empty({ status: 404 })
          if (url.searchParams.get('state') !== authorization.state)
            return HttpServerResponse.text('Invalid sign-in attempt', { status: 400 })
          const exit = yield* Effect.uninterruptibleMask((restore) =>
            Ref.getAndSet(claimed, true).pipe(
              Effect.flatMap((alreadyClaimed) =>
                alreadyClaimed
                  ? restore(Deferred.await(result)).pipe(Effect.exit)
                  : restore(auth.complete(url.href)).pipe(
                      Effect.exit,
                      Effect.tap((exit) => Deferred.done(result, exit)),
                    ),
              ),
            ),
          )
          return HttpServerResponse.text(
            Exit.isSuccess(exit)
              ? 'Sign-in complete. You may close this window.'
              : 'Sign-in failed. Restart sign-in in the application.',
            {
              status: Exit.isSuccess(exit) ? 200 : 400,
              headers: {
                'cache-control': 'no-store',
                'content-security-policy': "default-src 'none'",
                'referrer-policy': 'no-referrer',
              },
            },
          )
        }),
      )
      return Callback.of({
        authorization,
        await: Effect.gen(function* () {
          const remaining = Math.max(0, authorization.expiresAt - (yield* Clock.currentTimeMillis))
          return yield* Deferred.await(result).pipe(
            Effect.timeoutOrElse({
              duration: remaining,
              orElse: () =>
                auth.cancel(authorization.state).pipe(
                  Effect.andThen(
                    Effect.fail(
                      new AuthError({
                        reason: 'expired',
                        message: 'Authorization attempt expired',
                      }),
                    ),
                  ),
                ),
            }),
          )
        }),
      })
    }),
  )
