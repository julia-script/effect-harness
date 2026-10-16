import * as TemporaryStorage from './fixtures/Storage.js'
import { assert, describe, it } from '@effect/vitest'
import * as Cause from 'effect/Cause'
import * as Clock from 'effect/Clock'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as Option from 'effect/Option'
import * as Queue from 'effect/Queue'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as TestClock from 'effect/testing/TestClock'
import * as Prompt from 'effect/ai/Prompt'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import * as ToolProgress from '../src/ToolProgress.ts'
import * as Transaction from 'effect-harness/Transaction'

const root = Record.ROOT_CONVERSATION_ID
const retention = { retain: 'tail' as const, maxBytes: 32, maxLines: 2 }
const policy = { minInterval: 1000, bytesPerSecond: Number.MAX_VALUE }
const target = (callId: string) => ({
  scope: { _tag: 'conversation' as const, conversationId: root },
  key: callId,
})
const turns = Effect.forEach(Array.from({ length: 12 }), () => Effect.yieldNow, { discard: true })
const open = Effect.gen(function* () {
  const session = yield* Session.make()
  yield* Session.commit(session, Transaction.ensureRoot)
  const gate: ToolProgress.Gate = { commit: (change) => Session.commit(session, change) }
  return { session, gate }
})
const saved = Effect.fnUntraced(function* (session: Session.Session, callId: string) {
  const found = yield* Session.snapshot(session, ToolProgress.document, target(callId))
  assert.isTrue(Option.isSome(found))
  return yield* Effect.fromOption(found).pipe(Effect.map((value) => value.value))
})

describe('durable coalesced tool progress', () => {
  it.effect(
    'invalid policies and skipped windows are rejected without replacing accepted progress',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          for (const minInterval of [-1, 0.5, Infinity])
            assert.strictEqual(
              (yield* ToolProgress.make(gate, {
                conversationId: root,
                callId: 'invalid',
                attempt: 0,
                policy: { minInterval, bytesPerSecond: 1 },
              }).pipe(Effect.flip))._tag,
              'ToolProgressError',
            )
          for (const bytesPerSecond of [0, -1, Infinity, NaN])
            assert.strictEqual(
              (yield* ToolProgress.make(gate, {
                conversationId: root,
                callId: 'invalid',
                attempt: 0,
                policy: { minInterval: 0, bytesPerSecond },
              }).pipe(Effect.flip))._tag,
              'ToolProgressError',
            )
          assert.isTrue(
            Option.isNone(
              yield* Session.snapshot(session, ToolProgress.document, target('invalid')),
            ),
          )
          const writer = yield* ToolProgress.make(gate, {
            conversationId: root,
            callId: 'skip-validation',
            attempt: 0,
            retention,
            policy,
          })
          yield* writer.output('accepted')
          const before = yield* writer.snapshot
          assert.strictEqual(
            (yield* writer
              .output('discarded', { bytes: 1, newlines: 0, endsWithNewline: false })
              .pipe(Effect.flip))._tag,
            'OutputRetentionError',
          )
          assert.deepStrictEqual(yield* writer.snapshot, before)
          yield* writer.settle('completed', () => Effect.void)
          assert.strictEqual((yield* saved(session, 'skip-validation')).output.text, 'accepted')
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect(
    'settlement drains the existing write, includes later reports, and acknowledges only after its atomic batch',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const finalEntered = yield* Deferred.make<void, ToolProgress.Failure>()
          const finalRelease = yield* Deferred.make<void>()
          let block = false
          const observed: ToolProgress.Gate = {
            commit: (change) =>
              Effect.gen(function* () {
                if (block) {
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                }
                return yield* gate.commit(change)
              }),
          }
          const writer = yield* ToolProgress.make(observed, {
            conversationId: root,
            callId: 'drain',
            attempt: 0,
            retention,
            policy,
          })
          block = true
          const first = yield* writer.details({ step: 1 }).pipe(Effect.forkChild)
          yield* Deferred.await(entered)
          yield* writer.output('last accepted output')
          const last = yield* writer.details({ step: 2 }).pipe(Effect.forkChild)
          yield* turns
          const closing = yield* writer
            .settle('cancelled', (tx, state) =>
              Effect.gen(function* () {
                assert.strictEqual(state.output.text, 'last accepted output')
                assert.deepStrictEqual(state.details, { step: 2 })
                assert.isUndefined(last.pollUnsafe())
                yield* Transaction.appendEntry(tx, root, { kind: 'drain.receipt', data: state })
                yield* Deferred.succeed(finalEntered, undefined)
                yield* Deferred.await(finalRelease)
              }),
            )
            .pipe(
              Effect.tapCause((cause) => Deferred.failCause(finalEntered, cause)),
              Effect.forkChild,
            )
          yield* turns
          assert.isUndefined(closing.pollUnsafe())
          assert.isUndefined(first.pollUnsafe())
          assert.strictEqual(
            (yield* writer.output('too late').pipe(Effect.flip))._tag,
            'ToolProgressError',
          )
          block = false
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(first)
          yield* Deferred.await(finalEntered)
          assert.isUndefined(last.pollUnsafe())
          yield* Deferred.succeed(finalRelease, undefined)
          yield* Fiber.join(closing)
          yield* Fiber.join(last)
          assert.strictEqual((yield* saved(session, 'drain')).status, 'cancelled')
          assert.deepStrictEqual((yield* saved(session, 'drain')).details, { step: 2 })
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect(
    'reopening a committed tail or empty window continues exact counters without replaying retained text',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          for (const maxBytes of [0, 4]) {
            const callId = `resume-${maxBytes}`
            const limits = { retain: 'tail' as const, maxBytes, maxLines: 1 }
            yield* Effect.scoped(
              Effect.gen(function* () {
                const writer = yield* ToolProgress.make(gate, {
                  conversationId: root,
                  callId,
                  attempt: 0,
                  retention: limits,
                  policy,
                })
                yield* writer.output('α\nβ\n😀abc')
                yield* writer.details({ committed: true })
              }),
            )
            const writer = yield* ToolProgress.make(gate, {
              conversationId: root,
              callId,
              attempt: 0,
              retention: limits,
              policy,
            })
            yield* writer.output('d\n')
            yield* writer.settle('completed', () => Effect.void)
            const value = yield* saved(session, callId)
            assert.strictEqual(value.output.totalBytes, 15)
            assert.strictEqual(value.output.totalLines, 3)
            assert.strictEqual(value.output.text, maxBytes === 0 ? '' : 'bcd\n')
            assert.strictEqual(value.output.skippedBytes, maxBytes === 0 ? 15 : 11)
            assert.isTrue(value.endsWithNewline)
          }
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect('accepted details are detached from later caller mutation', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, gate } = yield* open
        const writer = yield* ToolProgress.make(gate, {
          conversationId: root,
          callId: 'owned',
          attempt: 0,
          retention,
          policy,
        })
        yield* writer.details({ seed: true })
        const value = { nested: { phase: 1 } }
        const pending = yield* writer.details(value).pipe(Effect.forkChild)
        yield* turns
        value.nested.phase = 2
        yield* writer.settle('completed', () => Effect.void)
        yield* Fiber.join(pending)
        assert.deepStrictEqual((yield* saved(session, 'owned')).details, { nested: { phase: 1 } })
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect(
    'first dirty state writes immediately; a Unicode flood coalesces at the minimum interval',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          let commits = 0
          const committed = yield* Queue.unbounded<void>()
          const observed: ToolProgress.Gate = {
            commit: (change) =>
              gate.commit(change).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    commits++
                  }).pipe(Effect.andThen(Queue.offer(committed, undefined))),
                ),
              ),
          }
          const writer = yield* ToolProgress.make(observed, {
            conversationId: root,
            callId: 'flood',
            attempt: 0,
            retention,
            policy,
          })
          yield* Queue.take(committed)
          commits = 0
          yield* writer.output('first\n')
          yield* Queue.take(committed)
          assert.strictEqual(commits, 1)
          assert.strictEqual((yield* saved(session, 'flood')).output.text, 'first\n')
          for (let index = 0; index < 10000; index++) yield* writer.output('漢')
          yield* writer.output('\nlast 😀\n')
          yield* turns
          assert.strictEqual(commits, 1)
          yield* TestClock.adjust(999)
          assert.strictEqual(commits, 1)
          yield* TestClock.adjust(1)
          yield* Queue.take(committed)
          assert.strictEqual(commits, 2)
          const value = yield* saved(session, 'flood')
          assert.strictEqual(value.output.text, 'last 😀\n')
          assert.strictEqual(value.output.totalBytes, 30017)
          assert.strictEqual(value.output.totalLines, 3)
          assert.strictEqual(value.output.retainedBytes, 10)
          assert.strictEqual(value.output.skippedBytes, 30007)
          yield* writer.settle('completed', () => Effect.void)
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect('the complete encoded snapshot buys byte-rate time from the start of its write', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, gate } = yield* open
        let writes = 0
        const committed = yield* Queue.unbounded<void>()
        const writer = yield* ToolProgress.make(
          {
            commit: (change) =>
              gate.commit(change).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    writes++
                  }).pipe(Effect.andThen(Queue.offer(committed, undefined))),
                ),
              ),
          },
          {
            conversationId: root,
            callId: 'rate',
            attempt: 0,
            retention,
            policy: { minInterval: 0, bytesPerSecond: 1000 },
          },
        )
        yield* Queue.take(committed)
        writes = 0
        yield* writer.details({ payload: 'x'.repeat(600) })
        yield* Queue.take(committed)
        const first = yield* saved(session, 'rate')
        const json = yield* Schema.encodeEffect(Schema.fromJsonString(ToolProgress.Snapshot))(first)
        const pause = new TextEncoder().encode(json).length
        yield* writer.output('next')
        yield* TestClock.adjust(pause - 1)
        assert.strictEqual(writes, 1)
        yield* TestClock.adjust(1)
        yield* Queue.take(committed)
        assert.strictEqual(writes, 2)
        assert.strictEqual((yield* saved(session, 'rate')).output.text, 'next')
        // The rate still applies with a zero minimum interval.
        yield* writer.settle('completed', () => Effect.void)
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect(
    'a slow write has one owner; later details wait for the snapshot that includes them',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          let block = false
          let active = 0
          let peak = 0
          const observed: ToolProgress.Gate = {
            commit: (change) =>
              Effect.gen(function* () {
                active++
                peak = Math.max(peak, active)
                if (block) {
                  yield* Deferred.succeed(entered, undefined)
                  yield* Deferred.await(release)
                }
                return yield* gate.commit(change)
              }).pipe(
                Effect.ensuring(
                  Effect.sync(() => {
                    active--
                  }),
                ),
              ),
          }
          const writer = yield* ToolProgress.make(observed, {
            conversationId: root,
            callId: 'inflight',
            attempt: 0,
            retention,
            policy,
          })
          block = true
          const first = yield* writer.details({ step: 1 }).pipe(Effect.forkChild)
          yield* Deferred.await(entered)
          const second = yield* writer.details({ step: 2 }).pipe(Effect.forkChild)
          yield* turns
          yield* writer.output('while pending')
          assert.strictEqual(peak, 1)
          assert.isUndefined(first.pollUnsafe())
          assert.isUndefined(second.pollUnsafe())
          block = false
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(first)
          assert.isUndefined(second.pollUnsafe())
          assert.deepStrictEqual((yield* saved(session, 'inflight')).details, { step: 1 })
          yield* TestClock.adjust(1000)
          yield* Fiber.join(second)
          assert.deepStrictEqual((yield* saved(session, 'inflight')).details, { step: 2 })
          assert.strictEqual((yield* saved(session, 'inflight')).output.text, 'while pending')
          assert.strictEqual(peak, 1)
          yield* writer.settle('completed', () => Effect.void)
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect(
    'cancelling a details waiter retains its accepted update, and bounded admissions apply backpressure',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          const writer = yield* ToolProgress.make(gate, {
            conversationId: root,
            callId: 'cancel-waiter',
            attempt: 0,
            retention,
            policy,
            maxPendingDetails: 1,
          })
          yield* writer.details({ seed: true })
          const first = yield* writer.details({ accepted: 1 }).pipe(Effect.forkChild)
          yield* turns
          const second = yield* writer.details({ accepted: 2 }).pipe(Effect.forkChild)
          yield* turns
          assert.deepStrictEqual((yield* writer.snapshot).details, { accepted: 1 })
          yield* Fiber.interrupt(second)
          assert.deepStrictEqual((yield* writer.snapshot).details, { accepted: 1 })
          yield* Fiber.interrupt(first)
          assert.deepStrictEqual((yield* writer.snapshot).details, { accepted: 1 })
          yield* TestClock.adjust(1000)
          assert.deepStrictEqual((yield* saved(session, 'cancel-waiter')).details, { accepted: 1 })
          yield* writer.settle('completed', () => Effect.void)
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect(
    'storage failure reaches every waiting caller and later reports, with no false acknowledgement',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const actual = yield* Storage.Storage
          const io = new StorageError({
            reason: 'io',
            operation: 'progress.test',
            message: 'Rejected storage write',
          })
          let reject = false
          const broken: Storage.Storage['Service'] = {
            ...actual,
            commit: (writes) => (reject ? Effect.fail(io) : actual.commit(writes)),
          }
          yield* Effect.scoped(
            Effect.gen(function* () {
              const { session, gate } = yield* open
              const writer = yield* ToolProgress.make(gate, {
                conversationId: root,
                callId: 'failed',
                attempt: 0,
                retention,
                policy,
              })
              yield* writer.details({ before: true })
              reject = true
              const one = yield* writer.details({ after: true }).pipe(Effect.forkChild)
              const two = yield* writer.details({ later: true }).pipe(Effect.forkChild)
              yield* turns
              yield* TestClock.adjust(1000)
              for (const fiber of [one, two]) {
                const result = yield* Fiber.await(fiber)
                assert.isTrue(Exit.isFailure(result))
                if (Exit.isFailure(result))
                  assert.deepStrictEqual(Cause.findErrorOption(result.cause), Option.some(io))
              }
              assert.deepStrictEqual(yield* writer.output('must fail').pipe(Effect.flip), io)
              assert.deepStrictEqual(
                yield* writer.settle('cancelled', () => Effect.void).pipe(Effect.flip),
                io,
              )
              assert.deepStrictEqual((yield* saved(session, 'failed')).details, { before: true })
              assert.isTrue(Option.isSome(yield* writer.failure))
            }),
          ).pipe(Effect.provideService(Storage.Storage, broken))
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect(
    'scope closure ends timers, revokes reports and fails outstanding acknowledgements',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          const scope = yield* Scope.make()
          const writer = yield* Scope.provide(
            ToolProgress.make(gate, {
              conversationId: root,
              callId: 'scope',
              attempt: 0,
              retention,
              policy,
            }),
            scope,
          )
          yield* writer.details({ seed: true })
          const pending = yield* writer.details({ uncommitted: true }).pipe(Effect.forkChild)
          yield* turns
          yield* Scope.close(scope, Exit.void)
          const closed = yield* Fiber.await(pending)
          assert.isTrue(Exit.isFailure(closed))
          yield* TestClock.adjust(10000)
          assert.deepStrictEqual((yield* saved(session, 'scope')).details, { seed: true })
          assert.strictEqual(
            (yield* writer.output('late').pipe(Effect.flip))._tag,
            'ToolProgressError',
          )
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect(
    'split UTF8, skips, zero limits and final incomplete bytes retain exact persisted counters',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          for (const maxBytes of [0, 4]) {
            const callId = `unicode-${maxBytes}`
            const writer = yield* ToolProgress.make(gate, {
              conversationId: root,
              callId,
              attempt: 0,
              retention: { retain: 'tail', maxBytes, maxLines: 1 },
              policy,
            })
            yield* writer.output(new Uint8Array([0xf0, 0x9f]))
            yield* writer.output(new Uint8Array([0x98, 0x80]))
            yield* writer.output('終\n', { bytes: 6, newlines: 2, endsWithNewline: true })
            yield* writer.settle('cancelled', () => Effect.void)
            const value = yield* saved(session, callId)
            assert.strictEqual(value.output.text, maxBytes === 0 ? '' : '終\n')
            assert.strictEqual(value.output.totalBytes, 14)
            assert.strictEqual(value.output.totalLines, 3)
            assert.strictEqual(value.output.skippedBytes, maxBytes === 0 ? 14 : 10)
            assert.strictEqual(value.output.skippedLines, maxBytes === 0 ? 3 : 2)
          }
          const partial = yield* ToolProgress.make(gate, {
            conversationId: root,
            callId: 'partial',
            attempt: 0,
            retention,
            policy,
          })
          yield* partial.output(new Uint8Array([0xf0, 0x9f]))
          yield* partial.settle('interrupted', () => Effect.void)
          assert.strictEqual((yield* saved(session, 'partial')).output.text, '�')
          assert.strictEqual((yield* saved(session, 'partial')).output.totalBytes, 3)
          const full = yield* ToolProgress.make(gate, {
            conversationId: root,
            callId: 'full',
            attempt: 0,
            retention: { retain: 'full', maxBytes: 2, maxLines: 1 },
            policy,
          })
          yield* full.output('too long')
          const failure = yield* full.settle('completed', () => Effect.void).pipe(Effect.flip)
          assert.strictEqual(failure._tag, 'OutputRetentionError')
          assert.strictEqual((yield* saved(session, 'full')).status, 'running')
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect('refuses flush outside settlement and stale competing writers', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { session, gate } = yield* open
        const a = yield* ToolProgress.make(gate, {
          conversationId: root,
          callId: 'fence',
          attempt: 0,
          retention,
          policy,
        })
        const b = yield* ToolProgress.make(gate, {
          conversationId: root,
          callId: 'fence',
          attempt: 0,
          retention,
          policy,
        })
        assert.strictEqual(
          (yield* Session.commit(session, (tx) => a.flush(tx)).pipe(Effect.flip))._tag,
          'ToolProgressError',
        )
        yield* a.details({ owner: 'a' })
        assert.strictEqual(
          (yield* b.details({ owner: 'b' }).pipe(Effect.flip))._tag,
          'ToolProgressError',
        )
        assert.deepStrictEqual((yield* saved(session, 'fence')).details, { owner: 'a' })
        yield* a.settle('completed', () => Effect.void)
      }),
    ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
  it.effect(
    'the joined post-hook text is retained while bytes, options and structured/details channels survive',
    () =>
      Effect.gen(function* () {
        const file = Prompt.makePart('file', {
          mediaType: 'image/png',
          data: new Uint8Array([0, 1, 255]),
          options: { openai: { imageDetail: 'high' } },
        })
        const first = Prompt.makePart('text', {
          text: 'αα',
          options: { openai: { itemId: 'retained-text' } },
        })
        const last = Prompt.makePart('text', {
          text: '終😀',
          options: { anthropic: { cacheControl: { type: 'ephemeral' } } },
        })
        const details = { ui: 'kept' }
        const structuredOutput = { exact: [1, 2] }
        const result = {
          content: [first, file, last],
          details,
          structuredOutput,
          isError: true,
          diagnostics: [{ severity: 'info' as const, message: 'original' }],
        }
        const bounded = yield* ToolProgress.retainResult(result, {
          retain: 'tail',
          maxBytes: 7,
          maxLines: 1,
        })
        assert.strictEqual(bounded.content[0], file)
        assert.deepStrictEqual(bounded.content[1]?.options, last.options)
        const text = bounded.content[1]
        assert.strictEqual(text?.type === 'text' ? text.text : undefined, '終😀')
        assert.strictEqual(bounded.details, details)
        assert.strictEqual(bounded.structuredOutput, structuredOutput)
        assert.strictEqual(bounded.isError, true)
        assert.deepStrictEqual(bounded.diagnostics?.[0], result.diagnostics[0])
        assert.strictEqual(bounded.diagnostics?.[1]?.code, 'output_truncated')
        assert.strictEqual(
          yield* ToolProgress.retainResult(result, { retain: 'head', maxBytes: 99, maxLines: 1 }),
          result,
        )
      }),
  )
  it.effect(
    'an arbitrarily small positive byte rate cannot overflow its timer into an early commit',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const { session, gate } = yield* open
          const writer = yield* ToolProgress.make(gate, {
            conversationId: root,
            callId: 'tiny-rate',
            attempt: 0,
            retention,
            policy: { minInterval: 0, bytesPerSecond: Number.MIN_VALUE },
          })
          yield* writer.details({ initial: true })
          yield* writer.output('held')
          yield* TestClock.adjust(0x7fffffff + 1000)
          assert.strictEqual((yield* saved(session, 'tiny-rate')).output.text, '')
          const started = yield* Clock.monotonicTimeNanos
          yield* writer.settle('completed', () => Effect.void)
          assert.strictEqual(yield* Clock.monotonicTimeNanos, started)
          assert.strictEqual((yield* saved(session, 'tiny-rate')).output.text, 'held')
        }),
      ).pipe(Effect.provide(TemporaryStorage.layer)),
  )
})
