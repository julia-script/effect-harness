/**
 * Test support: a local HTTP server standing in for a provider API. It records each request, its
 * JSON or form body decoded, and answers with the server-sent events `respond` gives, or with a
 * `Reply`.
 */
import { Effect } from 'effect'
import type { Scope } from 'effect'

export interface Recorded {
  readonly path: string
  /** The query string, with its `?`. */
  readonly search: string
  readonly headers: Readonly<Record<string, string>>
  readonly body: unknown
}

export interface FakeApi {
  /** The server's base URL. */
  readonly url: string
  readonly requests: ReadonlyArray<Recorded>
}

/** A JSON answer, for endpoints that do not stream. */
export interface Reply {
  readonly status?: number
  readonly json: unknown
}

const body = (text: string, contentType: string | null): unknown => {
  if (text === '') return undefined
  if (contentType?.includes('json') === true) return JSON.parse(text)
  return Object.fromEntries(new URLSearchParams(text))
}

const answer = (reply: ReadonlyArray<unknown> | Reply) =>
  'json' in reply
    ? new Response(JSON.stringify(reply.json), {
        status: reply.status ?? 200,
        headers: { 'content-type': 'application/json' },
      })
    : new Response(sse(reply), { headers: { 'content-type': 'text/event-stream' } })

const sse = (events: ReadonlyArray<unknown>) =>
  events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join('')

/** Serve on an ephemeral port while the scope is open. */
export const serve = (
  respond: (request: Recorded) => ReadonlyArray<unknown> | Reply,
): Effect.Effect<FakeApi, never, Scope.Scope> =>
  Effect.map(
    Effect.acquireRelease(
      Effect.sync(() => {
        const requests: Array<Recorded> = []
        const server = Bun.serve({
          hostname: '127.0.0.1',
          port: 0,
          fetch: (request) =>
            request.text().then((text) => {
              const recorded: Recorded = {
                path: new URL(request.url).pathname,
                search: new URL(request.url).search,
                headers: Object.fromEntries(request.headers),
                body: body(text, request.headers.get('content-type')),
              }
              requests.push(recorded)
              return answer(respond(recorded))
            }),
        })
        return { server, requests }
      }),
      ({ server }) => Effect.promise(() => server.stop(true)),
    ),
    ({ server, requests }): FakeApi => ({ url: `http://127.0.0.1:${server.port}`, requests }),
  )
