/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * The client against a loop across the memory transport, as values and encoded: the encoded run
 * checks that the protocol's Schemas carry each request, result and error across a process.
 */
import { describe, expect, test } from 'bun:test'
import { Effect, Fiber, Layer, Queue, Schema } from 'effect'
import { HarnessServer, Initializer } from '../../harness/index.ts'
import { Model } from '../../model/index.ts'
import type { Step } from '../../harness/test/support.ts'
import { standIn } from '../../harness/test/support.ts'
import * as Settings from '../../harness/Settings.ts'
import { Storage } from '../../storage/Storage.ts'
import { ConversationId, SubmissionId } from '../../storage/schema/Ids.ts'
import { settle } from '../../task/test/support.ts'
import { HarnessClient, HarnessTransport, Init } from '../index.ts'
import type { HarnessClient as Client } from '../index.ts'
import { open, runTest } from './support.ts'

const Seed = Init.make('seed', Schema.Struct({ text: Schema.String }))
const seed = Initializer.make(Seed, (tx, conversationId, { text }) =>
  Effect.asVoid(tx.appendEntry(conversationId, { kind: 'note', data: { text } })),
)
const Broken = Init.make('broken', Schema.Null)
const broken = Initializer.make(Broken, () => Effect.fail('no'))

const kinds = (conversation: Client.Conversation) =>
  Effect.map(Effect.orDie(conversation.entries({ order: 'ascending' }, 100)), (page) =>
    page.items.map((entry) => entry.kind),
  )

const transports = [
  ['memory', HarnessTransport.makeMemory],
  ['encoded memory', HarnessTransport.makeMemoryEncoded],
] as const

for (const [name, transport] of transports) {
  describe(`client over ${name}`, () => {
    test('runs a turn: submit, wait, and read the transcript', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { client, answer } = yield* open(storage, {}, transport)
          const root = yield* client.root()
          const submission = yield* root.submit({ type: 'input', content: 'hi' })
          yield* answer('hello')
          const settled = yield* submission.wait
          expect(settled).toMatchObject({ id: submission.id, type: 'input', status: 'done' })
          expect(yield* kinds(root)).toEqual(['pi.user', 'pi.assistant'])
          expect(yield* submission.status).toEqual(settled)
        }),
      ))

    test('configures a conversation and reads its agent', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { client } = yield* open(storage, {}, transport)
          const root = yield* client.root()
          yield* root.configure({
            model: { provider: 'faux', modelId: 'faux-1' },
            reasoning: 'high',
            instructions: 'Be brief.',
          })
          expect(yield* root.agent).toEqual({
            model: { provider: 'faux', modelId: 'faux-1' },
            reasoning: 'high',
            extensions: [],
            tools: [],
            sections: ['instructions'],
            instructions: 'Be brief.',
          })
          yield* root.configure({ reasoning: null })
          expect((yield* root.agent).reasoning).toBe('off')
        }),
      ))

    test('carries typed errors across the transport', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { client, answer } = yield* open(storage, {}, transport)
          const root = yield* client.root()
          const running = yield* root.submit({ type: 'input', content: 'a' })
          expect(
            yield* Effect.flip(root.submit({ type: 'input', content: 'x', whenBusy: 'reject' })),
          ).toMatchObject({ _tag: 'ConversationBusy', conversationId: root.id })
          expect(
            yield* Effect.flip(
              root.submit({ type: 'input', content: 'x', entry: { kind: 'pi.custom' } }),
            ),
          ).toMatchObject({ _tag: 'InvalidSubmission' })
          const unknown = SubmissionId.make(999_999)
          const missing = yield* client.submission(unknown)
          expect(missing).toBeUndefined()
          expect(yield* client.abortSubmission(unknown)).toBe('not_found')
          expect(yield* client.conversation(ConversationId.make(999_999))).toBeUndefined()
          yield* answer('done')
          yield* running.wait
        }),
      ))

    test('runs named inits in the creating commit, and fails unknown or failing ones', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { client } = yield* open(storage, { initializers: [seed, broken] }, transport)
          const root = yield* client.root({ init: Init.call(Seed, { text: 'first' }) })
          expect(yield* kinds(root)).toEqual(['note'])
          // The root exists already, so its init does not run again.
          yield* client.root({ init: Init.call(Seed, { text: 'again' }) })
          expect(yield* kinds(root)).toEqual(['note'])

          const ownerless = { kind: 'ownerless' } as const
          const failed = yield* Effect.flip(
            client.createConversation({ ownership: ownerless, init: Init.call(Broken, null) }),
          )
          expect(failed).toMatchObject({ _tag: 'InitFailed', init: 'broken' })
          const unknown = Init.make('unknown', Schema.Null)
          expect(
            yield* Effect.flip(
              client.createConversation({ ownership: ownerless, init: Init.call(unknown, null) }),
            ),
          ).toMatchObject({ _tag: 'InitFailed', init: 'unknown' })
          const inspection = yield* client.inspect
          expect(inspection.submissions).toEqual([])
        }),
      ))

    test('forks into a new handle, and withdraws a queued submission', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { client, answer } = yield* open(storage, { initializers: [seed] }, transport)
          const root = yield* client.root({ init: Init.call(Seed, { text: 'kept' }) })
          const [first] = (yield* root.entries({ order: 'ascending' }, 1)).items
          const fork = yield* root.fork(first!.id, {
            ownership: { kind: 'ownerless' },
            init: Init.call(Seed, { text: 'forked' }),
          })
          expect(fork.id).not.toBe(root.id)
          expect(yield* kinds(fork)).toEqual(['note', 'note'])

          const running = yield* fork.submit({ type: 'input', content: 'a' })
          const queued = yield* fork.submit({ type: 'input', content: 'b' })
          expect((yield* client.inspect).submissions.map(({ status }) => status)).toEqual([
            'placed',
            'queued',
          ])
          const reacquired = yield* client.submission(queued.id)
          expect(yield* reacquired!.abort).toBe('aborted')
          expect(yield* queued.wait).toMatchObject({ status: 'unanswered', reason: 'aborted' })
          yield* answer('done')
          yield* running.wait
          yield* fork.waitForIdle
        }),
      ))

    test('interrupting a wait leaves the submission, and abort ends the run', () =>
      runTest((storage) =>
        Effect.gen(function* () {
          const { client } = yield* open(storage, {}, transport)
          const root = yield* client.root()
          const submission = yield* root.submit({ type: 'input', content: 'a' })
          const waiting = yield* Effect.forkChild(submission.wait)
          yield* settle
          yield* Fiber.interrupt(waiting)
          expect((yield* submission.status).status).toBe('placed')
          yield* root.abort()
          expect(yield* submission.wait).toMatchObject({ status: 'unanswered', reason: 'aborted' })
        }),
      ))
  })
}

describe('client', () => {
  test('runs as calling code does: the loop launched, the client from a Layer', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const steps = yield* Queue.unbounded<Step>()
        const loop = HarnessServer.layer({
          tasks: [standIn(steps, Effect.succeed(Settings.defaults))],
        }).pipe(Layer.provide([Model.layer([]), Layer.succeed(Storage, storage)]))
        const program = Effect.gen(function* () {
          yield* Layer.launch(loop).pipe(Effect.forkScoped)
          const client = yield* HarnessClient.HarnessClient
          const root = yield* client.root()
          const submission = yield* root.submit({ type: 'input', content: 'hi' })
          yield* Queue.offer(steps, { kind: 'answer', text: 'hello' })
          return yield* submission.wait
        })
        // The client and the loop share one transport; providing it here is the entry point.
        const layer = HarnessClient.layer.pipe(Layer.provideMerge(HarnessTransport.layerMemory))
        // oxlint-disable-next-line effecttsgo/strict-effect-provide -- the program's entry point
        const settled = yield* program.pipe(Effect.provide(layer), Effect.scoped)
        expect(settled.status).toBe('done')
      }),
    ))
})
