import { Effect } from 'effect'
import { expect, test } from 'vitest'
import { Lab, type Snapshot } from '../src/Lab.js'

const until = (lab: Effect.Success<typeof Lab>, predicate: (snapshot: Snapshot) => boolean) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      const snapshot = yield* lab.read
      if (predicate(snapshot)) return snapshot
      yield* Effect.sleep('10 millis')
    }
    return yield* Effect.die('Demo did not reach the expected state')
  })

const complete = (lab: Effect.Success<typeof Lab>) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 150; attempt += 1) {
      const snapshot = yield* lab.read
      if (snapshot.status === 'InputDone') return snapshot
      yield* lab.step
      yield* Effect.sleep('10 millis')
    }
    return yield* Effect.die('Demo did not settle')
  })

test('the provider is gated; retries reuse admissions; a fork inherits history', async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const lab = yield* Lab
      yield* lab.submit('Why does the login test fail?')
      const waiting = yield* until(lab, (state) => state.pending === 'Tool intent')
      expect(waiting.toolCalls).toBe(0)
      expect(waiting.emitted).toBe('')
      yield* lab.retry
      const retried = yield* lab.read
      expect(retried.reused).toBe(true)
      expect(retried.submissions).toBe(1)
      yield* lab.step
      const tool = yield* until(lab, (state) => state.pending === 'Tool result')
      expect(tool.toolCalls).toBe(1)
      expect(tool.entries.some((entry) => entry.text.includes('inspect_test'))).toBe(true)
      const done = yield* complete(lab)
      expect(done.emitted).toBe(
        'The login test reads the session before authentication finishes. Await signIn() before asserting the user.',
      )
      expect(done.entries.some((entry) => entry.text === done.emitted)).toBe(true)
      expect(done.messages.map((message) => message.role)).toEqual(['user', 'tool', 'assistant'])
      expect(done.messages.at(-1)?.text).toBe(done.emitted)
      expect(done.answering).toBe(false)
      const originalIds = done.entries.map((entry) => entry.id)
      yield* lab.fork
      const fork = yield* lab.read
      expect(fork.branch).toBe('fork-1')
      expect(fork.entries.map((entry) => entry.id)).toEqual(expect.arrayContaining(originalIds))
      yield* lab.submit('Show me the fix')
      const childDone = yield* complete(lab)
      expect(childDone.toolCalls).toBe(2)
      expect(
        childDone.messages
          .filter((message) => message.role === 'user')
          .map((message) => message.text),
      ).toEqual(['Why does the login test fail?', 'Show me the fix'])
      expect(childDone.messages.at(-1)?.text).toContain(
        'await signIn()\nexpect(session.user).toBeDefined()',
      )
      yield* lab.returnToRoot
      const original = yield* lab.read
      expect(original.branch).toBe('root')
      expect(original.entries.map((entry) => entry.id)).toEqual(originalIds)
    }).pipe(Effect.provide(Lab.layer)),
  )
})

test('reopening a stopped stream recovers without repeating the completed tool', async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const lab = yield* Lab
      yield* lab.submit('Why does the login test fail?')
      yield* until(lab, (state) => state.pending === 'Tool intent')
      yield* lab.step
      yield* until(lab, (state) => state.pending === 'Tool result')
      yield* lab.step
      yield* until(lab, (state) => state.pending === 'Next text chunk')
      yield* lab.step
      const streamed = yield* until(lab, (state) => state.emitted.length > 0)
      expect(streamed.toolCalls).toBe(1)
      yield* lab.stop
      expect((yield* lab.read).running).toBe(false)
      yield* lab.start
      const recovered = yield* complete(lab)
      expect(recovered.toolCalls).toBe(1)
      expect(recovered.submissions).toBe(1)
      expect(recovered.entries.some((entry) => entry.text.includes('Await signIn()'))).toBe(true)
    }).pipe(Effect.provide(Lab.layer)),
  )
})

test('an interrupted safe tool repeats on reopen; abort and reset release pending gates', async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const lab = yield* Lab
      yield* lab.submit('Why does the login test fail?')
      yield* until(lab, (state) => state.pending === 'Tool intent')
      yield* lab.step
      yield* until(lab, (state) => state.pending === 'Tool result')
      yield* lab.stop
      yield* lab.start
      const restarted = yield* until(lab, (state) => state.pending === 'Tool result')
      expect(restarted.toolCalls).toBe(2)
      yield* lab.abort
      const aborted = yield* until(lab, (state) => state.status === 'InputUnanswered')
      expect(aborted.tasks).toHaveLength(0)
      yield* lab.reset
      const reset = yield* lab.read
      expect(reset.entries).toHaveLength(0)
      expect(reset.requests).toBe(0)
      expect(reset.running).toBe(false)
      yield* lab.submit('Why does the login test fail?')
      const fresh = yield* complete(lab)
      expect(fresh.submissions).toBe(1)
      expect(fresh.toolCalls).toBe(1)
    }).pipe(Effect.provide(Lab.layer)),
  )
})
