/* oxlint-disable max-lines-per-function -- each test is one story */
/**
 * The loop in its own process, as a server deployment runs it: over WebSockets, with its store in
 * SQLite and the coding tools in a workspace, killed with SIGKILL as a crash or an evicted machine
 * would be. Clients reconnect, runs resume from the store, and watches open again.
 */
import { expect, test } from 'bun:test'
import { existsSync, mkdtempSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Effect, Layer, Schedule, Stream } from 'effect'
import type { Scope } from 'effect'
import type { Request } from '../../openai/test/simulator.ts'
import { serveSimulator } from '../../openai/test/simulator.ts'
import { HarnessClient, HarnessTransport } from '../index.ts'
import type { HarnessClient as Client } from '../index.ts'

const SERVER = join(import.meta.dir, 'process', 'server.ts')

const freePort = () => {
  const probe = Bun.serve({ port: 0, fetch: () => new Response() })
  const { port } = probe
  void probe.stop(true)
  return port
}

/** A server process over one store and workspace, which `kill` ends at once. */
const makeServer = (modelUrl: string) =>
  Effect.gen(function* () {
    const dir = mkdtempSync(join(tmpdir(), 'harness-'))
    const port = freePort()
    let child: ReturnType<typeof Bun.spawn> | undefined
    const start = Effect.sync(() => {
      child = Bun.spawn(['bun', SERVER], {
        env: {
          ...process.env,
          PORT: String(port),
          DB: join(dir, 'agent.db'),
          WORKSPACE: dir,
          MODEL_URL: modelUrl,
        },
        stdout: 'ignore',
        stderr: 'inherit',
      })
    })
    const kill = Effect.promise(async () => {
      child?.kill('SIGKILL')
      await child?.exited
    })
    yield* Effect.addFinalizer(() => kill)
    yield* start
    return { url: `ws://127.0.0.1:${port}/harness`, dir, start, kill }
  })

const connect = (url: string): Effect.Effect<Client.HarnessClient['Service'], never, Scope.Scope> =>
  Effect.flatMap(Layer.build(HarnessTransport.layerWebSocket(url)), (transport) =>
    Effect.provideContext(HarnessClient.make, transport),
  )

const untilConnected = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.retry(
    effect,
    Schedule.spaced('200 millis').pipe(Schedule.upTo({ duration: '30 seconds' })),
  )

const until = (check: () => boolean) =>
  Effect.retry(
    Effect.suspend(() => (check() ? Effect.void : Effect.fail('not yet'))),
    Schedule.spaced('100 millis').pipe(Schedule.upTo({ duration: '20 seconds' })),
  )

const outputsSeen = (request: Request | undefined) =>
  (request?.rounds ?? []).flat().map(({ name, output }) => `${name}: ${output}`)

test(
  'resumes a coding session after the process is killed in the middle of a command',
  () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const model = yield* serveSimulator((request) => {
            const turn = Number(/Step (\d+)/u.exec(request.user)?.[1] ?? 0)
            if (request.user === 'Slow' && request.rounds.length === 0)
              return {
                calls: [
                  { name: 'bash', arguments: { command: 'sleep 30 && echo late > late.txt' } },
                  { name: 'read', arguments: { path: 'step1.txt' } },
                ],
              }
            if (request.user === 'Slow') return { text: outputsSeen(request).join('\n') }
            if (request.rounds.length === 0)
              return {
                calls: [
                  {
                    name: 'write',
                    arguments: { path: `step${turn}.txt`, content: `FACT: step ${turn}\n` },
                  },
                ],
              }
            if (request.rounds.length === 1)
              return {
                calls: [
                  {
                    name: 'edit',
                    arguments: {
                      path: `step${turn}.txt`,
                      edits: [{ oldText: `step ${turn}`, newText: `step ${turn} edited` }],
                    },
                  },
                ],
              }
            if (request.rounds.length === 2)
              return { calls: [{ name: 'bash', arguments: { command: `cat step${turn}.txt` } }] }
            return { text: outputsSeen(request).join('\n') }
          })
          const server = yield* makeServer(model.url)
          const client = yield* connect(server.url)
          const root = yield* untilConnected(client.root())
          for (let turn = 1; turn <= 5; turn++) {
            const submission = yield* root.submit({ type: 'input', content: `Step ${turn}` })
            expect((yield* submission.wait).status).toBe('done')
            expect(outputsSeen(model.requests.at(-1)).at(-1)).toContain(`FACT: step ${turn} edited`)
          }

          const slow = yield* root.submit({ type: 'input', content: 'Slow' })
          yield* until(() => model.requests.some((request) => request.user === 'Slow'))
          yield* Effect.sleep('1 second')
          yield* server.kill
          yield* server.start
          const found = yield* untilConnected(client.submission(slow.id))
          const settled = yield* untilConnected(
            Effect.flatMap(Effect.fromNullishOr(found), (again) => again.wait),
          )
          expect(settled.status).toBe('done')
          const seen = outputsSeen(model.requests.at(-1))
          // The command the crash interrupted is not run again; the read is.
          expect(seen.find((line) => line.startsWith('bash:'))).toContain('interrupted')
          expect(seen.find((line) => line.startsWith('read:'))).toContain('FACT: step 1 edited')
          expect(existsSync(join(server.dir, 'late.txt'))).toBe(false)
          expect(readFileSync(join(server.dir, 'step5.txt'), 'utf8')).toBe('FACT: step 5 edited\n')
        }),
      ),
    ),
  60_000,
)

test(
  'opens a watch again after the process was down, and follows the next run',
  () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const model = yield* serveSimulator((request) =>
            request.rounds.length === 0
              ? { calls: [{ name: 'read', arguments: { path: 'missing.txt' } }] }
              : { text: `Done: ${request.user}` },
          )
          const server = yield* makeServer(model.url)
          const client = yield* connect(server.url)
          const root = yield* untilConnected(client.root())
          const seen: Array<string> = []
          const { batches } = yield* root.events
          yield* batches.pipe(
            Stream.flattenIterable,
            Stream.runForEach((event) => Effect.sync(() => seen.push(event.type))),
            Effect.forkScoped,
          )
          const held = model.hold()
          yield* root.submit({ type: 'input', content: 'First' })
          yield* Effect.promise(() => held)
          yield* server.kill
          model.release()
          yield* Effect.sleep('3 seconds')
          yield* server.start
          yield* untilConnected(root.waitForIdle)
          // What happened while the watch was closed is in its new snapshot.
          yield* until(() => seen.includes('snapshot'))
          const next = yield* root.submit({ type: 'input', content: 'Second' })
          expect((yield* next.wait).status).toBe('done')
          yield* until(() => seen.slice(seen.lastIndexOf('snapshot')).includes('run_end'))
        }),
      ),
    ),
  60_000,
)
