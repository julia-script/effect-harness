/* oxlint-disable max-lines-per-function -- each test is one story */
/**
 * Sessions longer than a greeting against the harness in a Durable Object, run by Alchemy in a
 * local workerd, with a simulated model that calls the object's tools: many turns of tool rounds
 * and compactions, resets in the middle of a tool round, a compaction and a retry backoff, steers
 * and follow-ups while busy, aborts, clients that leave, bursts of input, conversations side by
 * side, several objects at once, and a watch that outlives a reset.
 */
import { expect } from 'bun:test'
import * as Cloudflare from 'alchemy/Cloudflare'
import * as Test from 'alchemy/Test/Bun'
import { Effect, Layer, Redacted, Schedule, Stream } from 'effect'
import { RpcClient } from 'effect/rpc'
import { AccountsRpcs } from '../../accounts/index.ts'
import { HarnessClient, HarnessTransport } from '../../client/index.ts'
import type { HarnessClient as Client } from '../../client/index.ts'
import type { Reply, Request } from '../../openai/test/simulator.ts'
import { serveSimulator } from '../../openai/test/simulator.ts'
import { TestRpcs } from './stack/agent.ts'
import Stack from './stack/stack.ts'

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

/** The clients of a fresh object, with an account at the simulator. */
const open = (name: string, url: string) =>
  Effect.gen(function* () {
    const clients = yield* connect(name)
    yield* clients.accounts.Add({
      adapter: 'openai',
      name: 'local',
      label: 'Local',
      settings: { apiUrl: `${url}/v1` },
      apiKey: Redacted.make('sk-test'),
    })
    return { ...clients, root: yield* clients.harness.root() }
  })

/** Every entry of a conversation, oldest first. */
const allEntries = (conversation: Client.Conversation) =>
  Effect.gen(function* () {
    let page = yield* conversation.entries({ order: 'ascending' }, 200)
    const entries = [...page.items]
    while (page.next !== undefined) {
      page = yield* conversation.entries({ order: 'ascending' }, 200, page.next)
      entries.push(...page.items)
    }
    return entries
  })

/** Calls fail while the client reconnects; try again until the object answers. */
const untilConnected = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.retry(
    effect,
    Schedule.spaced('200 millis').pipe(Schedule.upTo({ duration: '30 seconds' })),
  )

/** Wait until `check` holds. */
const until = (check: () => boolean, duration: `${number} seconds` = '20 seconds') =>
  Effect.retry(
    Effect.suspend(() => (check() ? Effect.void : Effect.fail('not yet'))),
    Schedule.spaced('100 millis').pipe(Schedule.upTo({ duration })),
  )

/** Reacquire a submission after the object reset, and wait for it to settle. */
const settle = (harness: Client.HarnessClient['Service'], id: Client.Submission['id']) =>
  untilConnected(
    Effect.flatMap(
      Effect.flatMap(harness.submission(id), Effect.fromNullishOr),
      (submission) => submission.wait,
    ),
  )

/** Waits until a tool call with an argument named `argument` is executing. */
const untilExecuting = (harness: Client.HarnessClient['Service'], argument: string) =>
  Effect.retry(
    Effect.flatMap(harness.inspect, ({ tasks }) =>
      tasks.some(
        ({ record }) =>
          record.kind === '$tool' &&
          JSON.stringify(record.state).includes('"execute"') &&
          JSON.stringify(record.state).includes(`"${argument}"`),
      )
        ? Effect.void
        : Effect.fail('not yet'),
    ),
    Schedule.spaced('100 millis').pipe(Schedule.upTo({ duration: '20 seconds' })),
  )

/** The tool outputs a request showed the model, as `name: output`. */
const outputsSeen = (request: Request | undefined) =>
  (request?.rounds ?? []).flat().map(({ name, output }) => `${name}: ${output}`)

const usersSeen = (request: Request | undefined) =>
  (request?.items ?? []).flatMap((item) => (item.type === 'user' ? [item.text] : []))

/** A verbose answer, as models give. */
const filler = (turn: number) =>
  Array.from(
    { length: 12 },
    (_, line) => `Detail ${line} for turn ${turn}: reviewed the notes and the fetched page.`,
  ).join('\n')

test(
  'runs a long session of tool rounds, compacting as the context grows',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator()
      model.setBrain((request): Reply => {
        const turn = Number(/Turn (\d+)/u.exec(request.user)?.[1] ?? 0)
        if (request.user.startsWith('Recall'))
          return request.rounds.length === 0
            ? { calls: [{ name: 'note_list', arguments: {} }] }
            : { text: `Notes:\n${outputsSeen(request).join('\n')}` }
        if (request.rounds.length === 0)
          return {
            text: 'Saving and fetching.',
            calls: [
              {
                name: 'note_save',
                arguments: {
                  key: `k${String(turn).padStart(2, '0')}`,
                  value: `FACT: turn ${turn} saw ${turn * 7}`,
                },
              },
              { name: 'http_get', arguments: { url: `${model.url}/page/${turn}` } },
            ],
          }
        if (request.rounds.length === 1 && turn % 5 === 0)
          return { calls: [{ name: 'counter_bump', arguments: {} }] }
        return { text: `FACT: answered turn ${turn}\n${filler(turn)}` }
      })
      const { root, control } = yield* open('long', model.url)

      const turns = 30
      for (let turn = 1; turn <= turns; turn++) {
        const submission = yield* root.submit({ type: 'input', content: `Turn ${turn}: go on` })
        expect((yield* submission.wait).status).toBe('done')
      }
      const recall = yield* root.submit({ type: 'input', content: 'Recall every note' })
      expect((yield* recall.wait).status).toBe('done')

      const entries = yield* allEntries(root)
      const asks = model.requests.filter((request) => !request.summarizing)
      expect(entries.filter((entry) => entry.kind === '$compaction').length).toBeGreaterThan(2)
      // Each request stays near what the compaction policy leaves: about 8,000 tokens.
      expect(Math.max(...asks.map((request) => request.inputTokens))).toBeLessThan(12_000)
      // Every note kept in the object's SQLite; the counter bumped once every fifth turn.
      const stats = yield* control.Stats()
      expect(stats.rows['test_notes']).toBe(turns + 1)
      expect(stats.counter).toBe(turns / 5)
      expect(outputsSeen(asks.at(-1))[0]).toContain('k30: FACT: turn 30 saw 210')
      // The newest summary still holds the facts of turns compacted away long ago.
      expect(usersSeen(asks.at(-1))[0]).toContain('FACT: turn 1 saw 7')
    }),
  ),
  { timeout: 120_000 },
)

test(
  'resumes a tool round after a reset: replays safe calls, reports unsafe ones interrupted',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator()
      model.setBrain((request) =>
        request.rounds.length === 0
          ? {
              calls: [
                { name: 'wait', arguments: { seconds: 30 } },
                { name: 'note_save', arguments: { key: 'a', value: 'FACT: a' } },
                { name: 'http_get', arguments: { url: `${model.url}/page/reset` } },
              ],
            }
          : { text: 'Round done' },
      )
      const { harness, root, control } = yield* open('tool-reset', model.url)
      const submission = yield* root.submit({ type: 'input', content: 'Do three things' })
      yield* untilExecuting(harness, 'seconds')
      yield* Effect.ignore(control.Reset())

      expect((yield* settle(harness, submission.id)).status).toBe('done')
      const seen = outputsSeen(model.requests.at(-1))
      expect(seen.find((line) => line.startsWith('wait:'))).toContain('interrupted')
      expect(seen.find((line) => line.startsWith('note_save:'))).toBe('note_save: Saved a')
      expect(seen.find((line) => line.startsWith('http_get:'))).toContain('# Page reset')
      // One request asked for the round, one answered it: the reset asked nothing twice.
      expect(model.requests.length).toBe(2)
    }),
  ),
  { timeout: 60_000 },
)

test(
  'finishes a compaction a reset interrupted, once',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator((request) => ({
        text: `FACT: reply to ${request.user}\n${filler(1).repeat(4)}`,
      }))
      const { root, control } = yield* open('compaction-reset', model.url)
      const summarizing = model.hold((request) => request.summarizing)
      let compacting = false
      void summarizing.then(() => (compacting = true))
      for (let turn = 1; !compacting && turn <= 40; turn++) {
        const submission = yield* root.submit({ type: 'input', content: `Turn ${turn}` })
        yield* Effect.raceFirst(
          Effect.asVoid(submission.wait),
          until(() => compacting, '60 seconds'),
        )
      }
      expect(compacting).toBe(true)
      yield* Effect.ignore(control.Reset())
      model.release()

      yield* untilConnected(root.waitForIdle)
      const after = yield* untilConnected(root.submit({ type: 'input', content: 'After' }))
      expect((yield* after.wait).status).toBe('done')
      const entries = yield* allEntries(root)
      expect(entries.filter((entry) => entry.kind === '$compaction').length).toBe(1)
      // The held summary request died with the object; the resumed compaction asked again.
      expect(model.requests.filter((request) => request.summarizing).length).toBe(2)
      expect(usersSeen(model.requests.at(-1))[0]).toContain('FACT: reply to Turn 1')
    }),
  ),
  { timeout: 120_000 },
)

test(
  'places a steer after the tool round and a follow-up after the answer',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator((request) =>
        request.user === 'Start' && request.rounds.length === 0
          ? { calls: [{ name: 'wait', arguments: { seconds: 2 } }] }
          : { text: `Answering: ${request.user}` },
      )
      const { harness, root } = yield* open('steer', model.url)
      const first = yield* root.submit({ type: 'input', content: 'Start' })
      yield* untilExecuting(harness, 'seconds')
      const followUp = yield* root.submit({ type: 'input', content: 'Later', whenBusy: 'followUp' })
      const steer = yield* root.submit({
        type: 'input',
        content: 'Change course',
        whenBusy: 'steer',
      })
      for (const submission of [first, steer, followUp])
        expect((yield* submission.wait).status).toBe('done')
      expect(model.requests.map((request) => request.user)).toEqual([
        'Start',
        'Change course',
        'Later',
      ])
      // The steer comes after the wait's result, in the same run.
      expect(model.requests[1]?.items.map((item) => item.type).slice(-3)).toEqual([
        'call',
        'output',
        'user',
      ])
    }),
  ),
  { timeout: 60_000 },
)

test(
  'retries an overloaded model, also across a reset in the backoff',
  Effect.scoped(
    Effect.gen(function* () {
      let failures = 0
      const model = yield* serveSimulator(() => {
        if (failures >= 2) return { text: 'Finally' }
        failures++
        return { status: 503 }
      })
      const { harness, root, control } = yield* open('retry', model.url)
      const submission = yield* root.submit({ type: 'input', content: 'Hello?' })
      yield* until(() => failures >= 1)
      yield* Effect.ignore(control.Reset())
      expect((yield* settle(harness, submission.id)).status).toBe('done')
      expect(model.requests.length).toBe(3)
    }),
  ),
  { timeout: 60_000 },
)

test(
  'aborts a long tool and keeps answering afterwards',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator((request) =>
        request.user === 'Wait a minute' && request.rounds.length === 0
          ? { calls: [{ name: 'wait', arguments: { seconds: 60 } }] }
          : { text: `Answering: ${request.user}` },
      )
      const { harness, root } = yield* open('abort', model.url)
      const long = yield* root.submit({ type: 'input', content: 'Wait a minute' })
      yield* untilExecuting(harness, 'seconds')
      yield* root.abort()
      expect(yield* long.wait).toMatchObject({ status: 'unanswered', reason: 'aborted' })
      const next = yield* root.submit({ type: 'input', content: 'Still there?' })
      expect((yield* next.wait).status).toBe('done')
      expect(outputsSeen(model.requests.at(-1))).toEqual([])
      expect((yield* harness.inspect).tasks).toEqual([])
    }),
  ),
  { timeout: 60_000 },
)

test(
  'keeps a run going while its client is away, and lets another reacquire it',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator((request) =>
        request.rounds.length === 0
          ? { calls: [{ name: 'wait', arguments: { seconds: 2 } }] }
          : { text: 'Done waiting' },
      )
      yield* open('away', model.url)
      const id = yield* Effect.scoped(
        Effect.gen(function* () {
          const { harness } = yield* connect('away')
          const root = yield* harness.root()
          return (yield* root.submit({ type: 'input', content: 'Take your time' })).id
        }),
      )
      // The first client is gone; a second one reacquires the submission.
      const { harness } = yield* connect('away')
      expect((yield* settle(harness, id)).status).toBe('done')
    }),
  ),
  { timeout: 60_000 },
)

test(
  'queues a burst of input and answers each in order',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator((request) => ({
        text: `Answer to ${request.user}`,
        delayMs: 50,
      }))
      const { root } = yield* open('burst', model.url)
      const questions = Array.from({ length: 10 }, (_, index) => `Question ${index}`)
      const submissions = yield* Effect.forEach(questions, (content) =>
        root.submit({ type: 'input', content }),
      )
      for (const submission of submissions) expect((yield* submission.wait).status).toBe('done')
      expect(model.requests.map((request) => request.user)).toEqual(questions)
    }),
  ),
  { timeout: 60_000 },
)

test(
  'runs conversations of one object side by side, and forks one',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator((request) =>
        request.rounds.length === 0
          ? { calls: [{ name: 'wait', arguments: { seconds: 1 } }] }
          : { text: `Done: ${request.user}` },
      )
      const { harness } = yield* open('side-by-side', model.url)
      const conversations = yield* Effect.forEach(Array.from({ length: 4 }), () =>
        harness.createConversation({ ownership: { kind: 'ownerless' } }),
      )
      const started = Date.now()
      const settled = yield* Effect.forEach(
        conversations,
        (conversation, index) =>
          Effect.flatMap(
            conversation.submit({ type: 'input', content: `Job ${index}` }),
            (submission) => submission.wait,
          ),
        { concurrency: 'unbounded' },
      )
      expect(settled.map(({ status }) => status)).toEqual(['done', 'done', 'done', 'done'])
      // The waits overlapped: four one-second tools took about one second, not four.
      expect(Date.now() - started).toBeLessThan(3_500)

      const [first] = settled
      const [conversation] = conversations
      if (first?.type !== 'input' || first.status !== 'done' || conversation === undefined)
        return yield* Effect.die('expected an answer')
      const fork = yield* conversation.fork(first.answer, { ownership: { kind: 'ownerless' } })
      const forked = yield* fork.submit({ type: 'input', content: 'In the fork' })
      expect((yield* forked.wait).status).toBe('done')
      expect(usersSeen(model.requests.at(-1))).toEqual(['Job 0', 'In the fork'])
    }),
  ),
  { timeout: 60_000 },
)

test(
  'runs several objects at once, each with its own store',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator((request) =>
        request.rounds.length === 0
          ? { calls: [{ name: 'note_save', arguments: { key: request.user, value: 'FACT' } }] }
          : { text: `Saved ${request.user}` },
      )
      const names = Array.from({ length: 6 }, (_, index) => `tenant${index}`)
      const notes = yield* Effect.forEach(
        names,
        (name) =>
          Effect.gen(function* () {
            const { root, control } = yield* open(name, model.url)
            for (let turn = 0; turn < 8; turn++) {
              const submission = yield* root.submit({ type: 'input', content: `${name}-${turn}` })
              expect((yield* submission.wait).status).toBe('done')
            }
            return (yield* control.Stats()).rows['test_notes']
          }),
        { concurrency: 'unbounded' },
      )
      expect(notes).toEqual(names.map(() => 8))
    }),
  ),
  { timeout: 120_000 },
)

test(
  'opens a watch again after a reset, from a snapshot, and follows the next run',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator((request) =>
        request.rounds.length === 0
          ? { calls: [{ name: 'note_save', arguments: { key: 'w', value: 'FACT: w' } }] }
          : { text: 'Watched' },
      )
      const { harness, root, control } = yield* open('watch', model.url)
      const { batches } = yield* root.events
      const seen: Array<string> = []
      yield* batches.pipe(
        Stream.flattenIterable,
        Stream.runForEach((event) => Effect.sync(() => seen.push(event.type))),
        Effect.forkScoped,
      )
      const held = model.hold()
      const submission = yield* root.submit({ type: 'input', content: 'Watch me' })
      yield* Effect.promise(() => held)
      yield* Effect.ignore(control.Reset())
      model.release()
      expect((yield* settle(harness, submission.id)).status).toBe('done')
      // The watch opens again on its own backoff, after the connection: what happened in between
      // is in its new snapshot, not in events.
      yield* until(() => seen.includes('snapshot'))
      const next = yield* root.submit({ type: 'input', content: 'Again' })
      expect((yield* next.wait).status).toBe('done')
      yield* until(() => seen.slice(seen.lastIndexOf('snapshot')).includes('run_end'))
      expect(seen.slice(seen.lastIndexOf('snapshot'))).toContain('tool_execution_end')
    }),
  ),
  { timeout: 60_000 },
)

test(
  'keeps what the model sees of a huge tool result, and stores no more',
  Effect.scoped(
    Effect.gen(function* () {
      const model = yield* serveSimulator()
      model.setBrain((request) =>
        request.rounds.length === 0
          ? { calls: [{ name: 'http_get', arguments: { url: `${model.url}/big/3000000` } }] }
          : { text: 'Got it' },
      )
      const { root, control } = yield* open('big-output', model.url)
      const submission = yield* root.submit({ type: 'input', content: 'Fetch the big page' })
      expect((yield* submission.wait).status).toBe('done')
      // 3 MB fetched, 50 KB kept: far under the 2 MB Cloudflare allows a stored value.
      expect((yield* control.Stats()).largestValue).toBeLessThan(100_000)
      expect(model.requests.at(-1)?.inputTokens).toBeLessThan(20_000)
    }),
  ),
  { timeout: 60_000 },
)
