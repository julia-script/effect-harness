/**
 * Scoped OpenAI loopback authorization callback ownership.
 *
 * @since 0.0.0
 */
import * as Option from 'effect/Option'
import * as Config from 'effect/Config'
import {
  AuthCallbackError,
  AuthConfigurationError,
  AuthExpiredError,
  AuthError,
  type OAuth,
} from '@effect-harness/auth/Credential'
import * as Context from 'effect/Context'
import * as DateTime from 'effect/DateTime'
import * as Duration from 'effect/Duration'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Ref from 'effect/Ref'
import * as Layer from 'effect/Layer'
import * as HttpServer from 'effect/http/HttpServer'
import * as HttpServerRequest from 'effect/http/HttpServerRequest'
import * as HttpServerResponse from 'effect/http/HttpServerResponse'
import { ChatGpt, type Authorization } from './ChatGpt.ts'

/**
 * Identifies the Callback service in the Effect context.
 *
 * @category services
 * @since 0.0.0
 */
export class Callback extends Context.Service<
  Callback,
  {
    readonly authorization: Authorization
    readonly await: Effect.Effect<OAuth, AuthError>
  }
>()('@effect-harness/provider-openai/Callback') {}

/**
 * Starts the listener before exposing the authorization URL; callers provide a scoped loopback HttpServer adapter.
 *
 * @category layers
 * @since 0.0.0
 */
export const layer = (options?: {
  readonly account?: string | undefined
}): Layer.Layer<Callback, AuthError, ChatGpt | HttpServer.HttpServer> =>
  Layer.effect(Callback)(
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer
      const auth = yield* ChatGpt
      if (
        server.address._tag !== 'InetAddressV4' ||
        server.address.address.toString() !== '127.0.0.1'
      )
        return yield* new AuthError({
          reason: new AuthConfigurationError({
            message: 'OAuth callback server must bind 127.0.0.1',
          }),
        })
      const redirectUri = `http://127.0.0.1:${server.address.port}/auth/callback`
      const result = yield* Deferred.make<OAuth, AuthError>()
      const claimed = yield* Ref.make(false)
      const authorization = yield* Effect.acquireRelease(
        auth.begin({ redirectUri, account: options?.account }),
        (authorization) => auth.cancel(authorization.state),
      )
      yield* server.serve(
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest
          const parsed = yield* Effect.try({
            try: () => new URL(request.url, redirectUri),
            catch: (cause) =>
              new AuthError({
                reason: new AuthCallbackError({ cause, message: 'Invalid callback URL' }),
              }),
          }).pipe(Effect.option)
          const url = yield* Option.match(parsed, {
            onNone: () => Effect.void,
            onSome: Effect.succeed,
          })
          if (url === undefined) return HttpServerResponse.empty({ status: 400 })
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
          const now = yield* DateTime.now
          const remaining = DateTime.isGreaterThan(authorization.expiresAt, now)
            ? DateTime.distance(now, authorization.expiresAt)
            : Duration.zero
          return yield* Deferred.await(result).pipe(
            Effect.timeoutOrElse({
              duration: remaining,
              orElse: () =>
                auth.cancel(authorization.state).pipe(
                  Effect.andThen(
                    Effect.fail(
                      new AuthError({
                        reason: new AuthExpiredError({
                          message: 'Authorization attempt expired',
                        }),
                      }),
                    ),
                  ),
                ),
            }),
          )
        }).pipe(Effect.withSpan('Callback.await')),
      })
    }),
  )

/**
 * Resolves all layer options through the caller's ConfigProvider.
 *
 * @category layers
 * @since 0.0.0
 */
export const layerConfig = (
  config: Config.Wrap<NonNullable<Parameters<typeof layer>[0]>>,
): Layer.Layer<Callback, AuthError | Config.ConfigError, ChatGpt | HttpServer.HttpServer> =>
  Layer.unwrap(
    Effect.gen(function* () {
      return layer(yield* Config.unwrap(config))
    }),
  )
