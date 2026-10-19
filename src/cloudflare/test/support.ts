/**
 * Test support: an OpenAI-compatible API on a local port, which workerd reaches. It lists one
 * model and answers each response request with `Hello`. While held, a response request waits for
 * `release` instead, and its arrival completes `requested`. It counts the response requests.
 */
import { Deferred, Effect } from 'effect'
import type { Scope } from 'effect'
import { events } from '../../openai/test/support.ts'

export interface ModelApi {
  readonly url: string
  /** Hold the next response requests until `release`. */
  readonly hold: Effect.Effect<void>
  /** Completes when a held request arrives. */
  readonly requested: Deferred.Deferred<void>
  readonly release: Effect.Effect<void>
  /** How many response requests arrived. */
  readonly responses: Effect.Effect<number>
}

const models = { object: 'list', data: [{ id: 'test-model', object: 'model' }] }

const stream = () =>
  new Response(events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join(''), {
    headers: { 'content-type': 'text/event-stream' },
  })

export const serveModel: Effect.Effect<ModelApi, never, Scope.Scope> = Effect.gen(function* () {
  const requested = yield* Deferred.make<void>()
  const gate = { held: false, open: Promise.withResolvers<void>(), responses: 0 }
  const server = yield* Effect.acquireRelease(
    Effect.sync(() =>
      Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        fetch: async (request) => {
          const path = new URL(request.url).pathname
          if (path.endsWith('/models')) return Response.json(models)
          gate.responses += 1
          if (gate.held) {
            Deferred.doneUnsafe(requested, Effect.void)
            await gate.open.promise
          }
          return stream()
        },
      }),
    ),
    (served) => Effect.promise(() => served.stop(true)),
  )
  return {
    url: `http://127.0.0.1:${server.port}`,
    requested,
    responses: Effect.sync(() => gate.responses),
    hold: Effect.sync(() => {
      gate.held = true
    }),
    release: Effect.sync(() => {
      gate.held = false
      gate.open.resolve()
    }),
  }
})
