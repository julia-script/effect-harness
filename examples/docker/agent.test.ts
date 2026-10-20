/* oxlint-disable max-lines-per-function -- each test is one story */
/**
 * Coding sessions against the deployed container, with a simulated model on this machine: many
 * turns of file edits and commands, a container killed in the middle of a command, and a session
 * long enough to compact. Deploy the stack first:
 *
 *   HARNESS_TOKEN=dev-token bunx alchemy deploy examples/docker/alchemy.run.ts
 *   HARNESS_TOKEN=dev-token bun test examples/docker
 *
 * The container reaches the simulator at `MODEL_HOST` (default `172.17.0.1`, Docker's bridge).
 */
import { expect, test } from 'bun:test'
import { Effect, Layer, Redacted, Schedule } from 'effect'
import { RpcClient } from 'effect/rpc'
import { AccountsRpcs } from 'effect-harness/accounts'
import { HarnessClient, HarnessTransport } from 'effect-harness/client'
import type { Reply, Request } from '../../src/openai/test/simulator.ts'
import { serveSimulator } from '../../src/openai/test/simulator.ts'

const URL = process.env['HARNESS_URL'] ?? 'http://127.0.0.1:3000'
const TOKEN = process.env['HARNESS_TOKEN'] ?? 'dev-token'
const MODEL_HOST = process.env['MODEL_HOST'] ?? '172.17.0.1'
const RUN = Date.now().toString(36)

const connect = (name: string) =>
  Effect.gen(function* () {
    const ws = `${URL.replace(/^http/u, 'ws')}/agents/${name}-${RUN}?token=${TOKEN}`
    const transport = yield* Layer.build(HarnessTransport.layerWebSocket(ws))
    const harness = yield* Effect.provideContext(HarnessClient.make, transport)
    const accounts = yield* Effect.provideContext(
      RpcClient.make(AccountsRpcs.AccountsRpcs),
      transport,
    )
    return { harness, accounts }
  })

const untilConnected = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.retry(
    effect,
    Schedule.spaced('500 millis').pipe(Schedule.upTo({ duration: '60 seconds' })),
  )

/** The model's view of the tool outputs of its newest request. */
const outputs = (request: Request) =>
  request.rounds.flat().map(({ name, output }) => `${name}: ${output}`)

const docker = (command: string) =>
  Effect.promise(() => Bun.$`sh -c ${command}`.quiet().then((out) => out.text().trim()))

const setup = (name: string, brain: (request: Request) => Reply) =>
  Effect.gen(function* () {
    const model = yield* serveSimulator(brain)
    const port = new globalThis.URL(model.url).port
    const clients = yield* connect(name)
    const added = yield* untilConnected(
      clients.accounts.Add({
        adapter: 'openai',
        name: 'local',
        label: 'Local',
        settings: { apiUrl: `http://${MODEL_HOST}:${port}/v1` },
        apiKey: Redacted.make('sk-test'),
      }),
    )
    expect(added.status).toBe('ready')
    const root = yield* clients.harness.root()
    yield* root.configure({ model: { provider: 'local', modelId: 'test-model' } })
    return { model, root, ...clients }
  })

test(
  'writes, edits and runs code over many turns',
  () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { model, root } = yield* setup('coder', (request) => {
            const turn = Number(/Step (\d+)/u.exec(request.user)?.[1] ?? 0)
            const file = `src/step${turn}.ts`
            if (request.rounds.length === 0)
              return {
                text: `Writing ${file}.`,
                calls: [
                  {
                    name: 'write',
                    arguments: { path: file, content: `export const value = ${turn}\n` },
                  },
                ],
              }
            if (request.rounds.length === 1)
              return {
                calls: [
                  {
                    name: 'edit',
                    arguments: {
                      path: file,
                      edits: [{ oldText: `= ${turn}`, newText: `= ${turn} * 2` }],
                    },
                  },
                ],
              }
            if (request.rounds.length === 2)
              return {
                calls: [
                  {
                    name: 'bash',
                    arguments: {
                      command: `bun -e "import('./${file}').then((m) => console.log('value', m.value))"`,
                    },
                  },
                ],
              }
            return { text: `FACT: step ${turn} printed ${outputs(request).at(-1) ?? ''}` }
          })
          for (let turn = 1; turn <= 8; turn++) {
            const submission = yield* root.submit({ type: 'input', content: `Step ${turn}` })
            expect((yield* submission.wait).status).toBe('done')
            expect(outputs(model.requests.at(-1) as Request).at(-1)).toContain(`value ${turn * 2}`)
          }
          const listing = yield* root.submit({ type: 'input', content: 'Step 9' })
          expect((yield* listing.wait).status).toBe('done')
        }),
      ),
    ),
  { timeout: 180_000 },
)

test(
  'resumes after the container is killed in the middle of a command',
  () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { model, root, harness } = yield* setup('crash', (request) => {
            if (request.rounds.length === 0)
              return {
                calls: [
                  { name: 'bash', arguments: { command: 'sleep 30 && echo finished > slow.txt' } },
                  { name: 'write', arguments: { path: 'quick.txt', content: 'written\n' } },
                ],
              }
            if (request.rounds.length === 1)
              return { calls: [{ name: 'bash', arguments: { command: 'ls; cat quick.txt' } }] }
            return { text: outputs(request).join('\n') }
          })
          const submission = yield* root.submit({ type: 'input', content: 'Do the slow thing' })
          // Wait until the command runs, then kill the container as a crash would.
          yield* Effect.retry(
            Effect.suspend(() =>
              model.requests.length >= 1 ? Effect.void : Effect.fail('not yet'),
            ),
            Schedule.spaced('100 millis').pipe(Schedule.upTo({ duration: '20 seconds' })),
          )
          yield* Effect.sleep('2 seconds')
          const before = yield* docker('docker ps -q --filter name=effectharnessdocker-agent')
          yield* docker(`docker kill ${before}`)
          const found = yield* untilConnected(harness.submission(submission.id))
          const settled = yield* untilConnected(
            Effect.flatMap(Effect.fromNullishOr(found), (again) => again.wait),
          )
          const after = yield* docker('docker ps -q --filter name=effectharnessdocker-agent')
          expect(after).not.toBe(before)
          expect(settled.status).toBe('done')
          const seen = outputs(model.requests.at(-2) as Request)
          // The command a crash interrupted is not run again; the file write was finished.
          expect(seen.find((line) => line.startsWith('bash:'))).toContain('nterrupt')
          expect(outputs(model.requests.at(-1) as Request).at(-1)).toContain('written')
        }),
      ),
    ),
  { timeout: 180_000 },
)
