/* oxlint-disable max-lines-per-function -- each test is one story */
/**
 * The harness in a Durable Object, run by Alchemy in a local workerd: the Worker checks the token,
 * a client adds an account and chats over WebSockets, and a reset in the middle of a turn ends
 * with the answer, once the object has started again and resumed the turn from its SQLite.
 */
import { expect } from 'bun:test'
import * as Cloudflare from 'alchemy/Cloudflare'
import * as Test from 'alchemy/Test/Bun'
import { Deferred, Effect, Layer, Redacted, Schedule } from 'effect'
import { RpcClient } from 'effect/rpc'
import { AccountsRpcs } from '../../accounts/index.ts'
import { HarnessClient, HarnessTransport } from '../../client/index.ts'
import type { HarnessClient as Client } from '../../client/index.ts'
import { TestRpcs } from './stack/agent.ts'
import Stack from './stack/stack.ts'
import { serveModel } from './support.ts'

const TOKEN = 'test-token'
process.env['HARNESS_TOKEN'] = TOKEN

const { test, beforeAll, deploy } = Test.make({ providers: Cloudflare.providers(), dev: true })
const stack = beforeAll(deploy(Stack))

/** Objects keep their SQLite between runs of the local workerd: each run uses fresh names. */
const RUN = Date.now().toString(36)

/** The clients of the object named `name`, over one WebSocket, while the scope is open. */
const connect = (name: string) =>
  Effect.gen(function* () {
    const { url } = yield* stack
    const ws = `${String(url).replace(/^http/u, 'ws')}/agents/${name}-${RUN}?token=${TOKEN}`
    const transport = yield* Layer.build(HarnessTransport.layerWebSocket(ws))
    const harness = yield* Effect.provideContext(HarnessClient.make, transport)
    const accounts = yield* Effect.provideContext(
      RpcClient.make(AccountsRpcs.AccountsRpcs),
      transport,
    )
    const control = yield* Effect.provideContext(RpcClient.make(TestRpcs), transport)
    return { harness, accounts, control }
  })

/** The text of the conversation's last answer. */
const lastAnswer = (conversation: Client.Conversation) =>
  Effect.map(conversation.entries({ order: 'descending' }, 10), (page) => {
    const answer = page.items.find((entry) => entry.kind === 'pi.assistant')
    const content: unknown = answer?.model?.[0]?.['content']
    return Array.isArray(content)
      ? content.map((block: { text?: string }) => block.text).join('')
      : ''
  })

/** Calls fail while the client reconnects; try again until the object answers. */
const untilConnected = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.retry(
    effect,
    Schedule.spaced('200 millis').pipe(Schedule.upTo({ duration: '20 seconds' })),
  )

test(
  'refuses a request without the token',
  Effect.gen(function* () {
    const { url } = yield* stack
    const refused = yield* Effect.promise(() => fetch(`${String(url)}/agents/julia`))
    expect(refused.status).toBe(401)
    const unknown = yield* Effect.promise(() => fetch(`${String(url)}/elsewhere?token=${TOKEN}`))
    expect(unknown.status).toBe(404)
  }),
  { timeout: 120_000 },
)

test(
  'adds an account and answers over it',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveModel
      const { harness, accounts } = yield* connect('chat')
      const added = yield* accounts.Add({
        adapter: 'openai',
        name: 'local',
        label: 'Local',
        settings: { apiUrl: `${model.url}/v1` },
        apiKey: Redacted.make('sk-test'),
      })
      expect(added.status).toBe('ready')

      const root = yield* harness.root()
      const submission = yield* root.submit({ type: 'input', content: 'Hi' })
      yield* submission.wait
      expect(yield* lastAnswer(root)).toBe('Hello')
      // Nothing is left live, so the object no longer keeps itself awake.
      yield* root.waitForIdle
      expect((yield* harness.inspect).tasks).toEqual([])
    }),
  ),
  { timeout: 60_000 },
)

test(
  'resumes a turn after the object is reset in the middle of it',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveModel
      const { harness, accounts, control } = yield* connect('restart')
      yield* accounts.Add({
        adapter: 'openai',
        name: 'local',
        label: 'Local',
        settings: { apiUrl: `${model.url}/v1` },
        apiKey: Redacted.make('sk-test'),
      })
      const root = yield* harness.root()
      // The first request hangs until the object is gone; the one after the reset answers.
      yield* model.hold
      const submission = yield* root.submit({ type: 'input', content: 'Hi' })
      yield* Deferred.await(model.requested)
      yield* Effect.ignore(control.Reset())
      yield* model.release

      const found = yield* untilConnected(harness.submission(submission.id))
      yield* untilConnected(Effect.flatMap(Effect.fromNullishOr(found), (again) => again.wait))
      expect(yield* untilConnected(lastAnswer(root))).toBe('Hello')
      // The held request never answered: the reset object asked again.
      expect(yield* model.responses).toBe(2)
    }),
  ),
  { timeout: 90_000 },
)
