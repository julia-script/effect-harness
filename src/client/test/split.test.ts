/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * A client and a loop in separate processes, over WebSockets, sharing a central credential store
 * that this process serves: the loop reads its credential fresh for each request, and a client
 * write reaches the next one.
 */
import { BunHttpServer, BunServices } from '@effect/platform-bun'
import { describe, expect, test } from 'bun:test'
import { Effect, Fiber, Layer, Option, Path, Redacted, Schema, Stream } from 'effect'
import { HttpRouter, HttpServer } from 'effect/http'
import { ChildProcess, ChildProcessSpawner } from 'effect/process'
import { Credentials } from '../../credentials/index.ts'
import type { HarnessClient } from '../index.ts'
import { HarnessClient as Client, HarnessTransport } from '../index.ts'

const apiKey = (key: string) => ({ type: 'api_key', key: Redacted.make(key) }) as const

/** Serve a memory store at `/credentials` on an ephemeral port; its URL. */
const serveCredentials = Effect.gen(function* () {
  const served = HttpRouter.serve(
    Credentials.layerServer.pipe(
      Layer.provide(HarnessTransport.layerWebSocketServer('/credentials')),
      Layer.provide(Credentials.layerMemory([['faux', apiKey('k1')]])),
    ),
    { disableListenLog: true, disableLogger: true },
  ).pipe(Layer.provideMerge(BunHttpServer.layer({ port: 0, hostname: '127.0.0.1' })))
  const context = yield* Layer.build(served)
  const { address } = yield* Effect.provideContext(HttpServer.HttpServer, context)
  if (!('port' in address)) return yield* Effect.die('expected a TCP address')
  return `ws://127.0.0.1:${address.port}/credentials`
})

/** What the loop process prints once it serves. */
const Listening = Schema.fromJsonString(Schema.Struct({ port: Schema.Int }))

/** Start the loop process against `credentials`; the URL it serves. */
const startLoop = (credentials: string) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner
    const path = yield* Path.Path
    const entry = path.join(import.meta.dir, 'split', 'loop.ts')
    const handle = yield* spawner.spawn(
      ChildProcess.make('bun', [entry, credentials], { stdin: 'ignore', stderr: 'inherit' }),
    )
    const first = yield* handle.stdout.pipe(Stream.decodeText(), Stream.splitLines, Stream.runHead)
    const line = yield* Effect.fromOption(first)
    const { port } = yield* Schema.decodeEffect(Listening)(line)
    return `ws://127.0.0.1:${port}/harness`
  })

const lastAnswer = (conversation: HarnessClient.Conversation) =>
  Effect.map(conversation.entries({ order: 'descending' }, 10), (page) => {
    const answer = page.items.find((entry) => entry.kind === '$assistant')
    const content: unknown = answer?.model?.[0]?.['content']
    return Array.isArray(content)
      ? content.map((block: { text?: string }) => block.text).join('')
      : ''
  })

describe('a split client and loop', () => {
  test(
    'share a central credential store, read fresh for each request',
    () =>
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const credentials = yield* serveCredentials
            const harness = yield* startLoop(credentials)
            // Built in the test's scope, so the connections outlive this step.
            const toHarness = yield* Layer.build(HarnessTransport.layerWebSocket(harness))
            const client = yield* Effect.provideContext(Client.make, toHarness)
            const toStore = yield* Layer.build(HarnessTransport.layerWebSocket(credentials))
            const store = yield* Effect.provideContext(Credentials.makeRpc, toStore)
            const root = yield* client.root()
            const ask = (content: string) =>
              Effect.flatMap(
                root.submit({ type: 'input', content }),
                (submission) => submission.wait,
              )
            yield* ask('first')
            expect(yield* lastAnswer(root)).toBe('key k1')
            yield* store.set('faux', apiKey('k2'))
            // Streams cross the transport too: the view follows the next answer.
            const view = yield* root.view
            const before = view.value.entries.length
            const grown = yield* Effect.forkChild(
              view.changes.pipe(
                Stream.filter((change) => change.value.entries.length >= before + 2),
                Stream.runHead,
              ),
            )
            yield* ask('second')
            expect(Option.isSome(yield* Fiber.join(grown))).toBe(true)
            expect(yield* lastAnswer(root)).toBe('key k2')
          }),
        ).pipe(
          // The test is an entry point that provides its own platform.
          // oxlint-disable-next-line effecttsgo/strict-effect-provide
          Effect.provide(BunServices.layer),
        ),
      ),
    30_000,
  )
})
