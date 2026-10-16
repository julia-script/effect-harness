import * as TemporaryStorage from './fixtures/Storage.js'
import { NodeServices } from '@effect/platform-node'
import { assert, describe, it } from '@effect/vitest'
import * as Context from 'effect/Context'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Exit from 'effect/Exit'
import * as Fiber from 'effect/Fiber'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Scope from 'effect/Scope'
import * as Stream from 'effect/Stream'
import * as ContextProjection from 'effect-harness/ContextProjection'
import * as Document from 'effect-harness/Document'
import * as Identity from 'effect-harness/Identity'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import { StorageError } from 'effect-harness/StorageError'
import * as Transaction from 'effect-harness/Transaction'
import * as Usage from 'effect-harness/Usage'

const root = Record.ROOT_CONVERSATION_ID
const decodeAttempt = Schema.decodeSync(Usage.AttemptId)
const decodeRequest = Schema.decodeSync(Identity.RequestId)
const decodeCall = Schema.decodeSync(Usage.NestedCallId)
const request = (
  id = 'prepared:task-1:logical-0:config-v1',
  attempt = 0,
  conversationId = root,
): Usage.Receipt => ({
  conversationId,
  identity: {
    _tag: 'requestAttempt',
    requestId: decodeRequest(id),
    purpose: 'generation',
    attempt: decodeAttempt(attempt),
  },
  outcome: { _tag: 'failed', error: { reason: 'provider stopped after billing' } },
  usage: {
    _tag: 'model',
    model: { provider: 'fixture', modelId: 'one' },
    tokens: { input: 10, output: 2, reasoning: 1 },
    cost: { amount: 0.3, currency: 'USD', source: 'provider-v1' },
  },
})
const taskDraft = (conversationId = root): Transaction.TaskCreate => ({
  conversationId,
  kind: 'receipt.fixture',
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
  state: { status: 'running', checkpoint: { phase: 'work' } },
})
const taskDocument = Document.define({
  kind: 'receipt.task.result',
  version: 1,
  scope: 'task',
  schema: Schema.Struct({ value: Schema.Natural }),
  initial: () => ({ value: 0 }),
})
const temporary = (id: Record.TaskId): Document.Target => ({ scope: { _tag: 'task', taskId: id } })
const nested = (taskId: Record.TaskId, conversationId = root): Usage.Receipt => ({
  conversationId,
  identity: {
    _tag: 'nestedCall',
    taskId,
    callId: decodeCall('__proto__/call:1'),
    attempt: decodeAttempt(0),
  },
  outcome: { _tag: 'completed', value: { result: 42 } },
  usage: {
    _tag: 'tool',
    name: '__proto__',
    tokens: { totalTokens: 7, reasoning: 2 },
    cost: { amount: 2, currency: 'EUR', source: 'nested-v1', components: { reasoning: 0.5 } },
  },
})
const emptyModels = (summary: Usage.Summary) => assert.deepEqual(summary.models, [])
const withStorage = <A, E, R>(program: Effect.Effect<A, E, R>) =>
  Effect.scoped(program).pipe(Effect.provide(TemporaryStorage.layer))
const assertConflict = (failure: Session.Failure) => {
  assert.strictEqual(failure._tag, 'SessionError')
  if (failure._tag === 'SessionError') {
    assert.strictEqual(failure.reason, 'conflict')
    assert.strictEqual(failure.operation, 'usage.receipt')
  }
}

for (const backend of ['temporary', 'jsonl'] as const) {
  describe(`usage receipts: ${backend}`, () => {
    it.effect(
      'persists failed and nested receipts through retirement, reset, projection, fork and cold reopen',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: 'usage-receipts-' })
            const disk = Storage.layerJsonl({ filePath: `${directory}/state.jsonl` })
            const open = Effect.gen(function* () {
              const storage =
                backend === 'temporary'
                  ? yield* Storage.Storage
                  : Context.get(yield* Layer.build(Layer.fresh(disk)), Storage.Storage)
              return yield* Session.make().pipe(Effect.provideService(Storage.Storage, storage))
            })
            const scope = yield* Scope.make()
            yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void))
            const session = yield* Scope.provide(open, scope)
            let calls = 0
            const initialized = yield* Session.commit(session, (tx) =>
              Effect.gen(function* () {
                yield* Transaction.ensureRoot(tx)
                const task = yield* Transaction.createTask(tx, taskDraft())
                yield* Transaction.ensureDocument(tx, taskDocument, temporary(task.id))
                yield* Usage.record(tx, request())
                const receipt = nested(task.id)
                const recorded = yield* Usage.settle(tx, receipt, (child) =>
                  Effect.gen(function* () {
                    calls++
                    yield* Transaction.setDocument(child, taskDocument, temporary(task.id), {
                      value: 42,
                    })
                    yield* Transaction.putTask(child, {
                      ...task,
                      state: {
                        status: 'terminal',
                        outcome: { status: 'completed', result: { value: 42 } },
                      },
                    })
                  }),
                )
                assert.isTrue(recorded.inserted)
                assert.isFalse(
                  (yield* Usage.settle(tx, receipt, () => Effect.die('must not replay outcome')))
                    .inserted,
                )
                yield* Usage.record(tx, {
                  conversationId: root,
                  identity: {
                    _tag: 'requestAttempt',
                    requestId: decodeRequest('prepared:summary:config-v1'),
                    purpose: 'compaction',
                    attempt: decodeAttempt(0),
                  },
                  outcome: { _tag: 'interrupted', reason: 'no summary entry' },
                  usage: {
                    _tag: 'model',
                    model: { provider: 'fixture', modelId: 'summary' },
                    tokens: { input: 4 },
                    cost: { amount: 0.4, currency: 'GBP', source: 'provider-v1' },
                  },
                })
                return {
                  task,
                  receipt,
                  entry: yield* Transaction.appendEntry(tx, root, { kind: 'receipt.anchor' }),
                }
              }),
            )
            assert.strictEqual(calls, 1)
            assert.isTrue(
              Option.isNone(
                yield* Session.snapshot(session, taskDocument, temporary(initialized.task.id)),
              ),
            )
            const baseline = yield* Session.usage(session)
            assert.strictEqual(baseline.models[0]?.responses, 1)
            assert.strictEqual(baseline.tools[0]?.tokens?.totalTokens, 7)
            assert.deepEqual(baseline.costs, [
              { currency: 'USD', model: 0.3, tool: 0 },
              { currency: 'EUR', model: 0, tool: 2, toolComponents: { reasoning: 0.5 } },
              { currency: 'GBP', model: 0.4, tool: 0 },
            ])
            // A real reset head omits prior context without altering either accounting domain.
            yield* Session.commit(session, (tx) =>
              Transaction.appendEntry(tx, root, { kind: 'receipt.reset', head: 'self' }),
            )
            assert.deepEqual(yield* Session.usage(session), baseline)
            assert.strictEqual(
              (yield* ContextProjection.read(session, root)).activeEntries.length,
              1,
            )
            assert.deepEqual(yield* Session.usage(session), baseline)
            const fork = yield* Session.commit(session, (tx) =>
              Transaction.forkConversation(tx, root, initialized.entry.id, {
                ownership: { _tag: 'ownerless' },
              }),
            )
            emptyModels(yield* Session.usage(session, fork.id))
            assert.deepEqual(yield* Session.usage(session), baseline)
            // Same prepared ID belongs to separate conversation domains; fork inherits no charge.
            yield* Session.commit(session, (tx) => Usage.record(tx, request(undefined, 0, fork.id)))
            assert.strictEqual((yield* Session.usage(session, fork.id)).models[0]?.responses, 1)
            assert.strictEqual((yield* Session.usage(session)).models[0]?.responses, 2)
            yield* Scope.close(scope, Exit.void)
            const reopened = yield* open
            const current = yield* Session.usage(reopened)
            assert.strictEqual(current.models[0]?.responses, 2)
            assert.strictEqual(current.tools[0]?.results, 1)
            assert.isFalse(
              (yield* Session.commit(reopened, (tx) => Usage.record(tx, initialized.receipt)))
                .inserted,
            )
            assert.deepEqual(yield* Session.usage(reopened), current)
            const stored = yield* Session.commit(reopened, (tx) => Usage.receipts(tx, root))
            assert.deepEqual(
              stored.map((receipt) => receipt.outcome._tag),
              ['failed', 'completed', 'interrupted'],
            )
            assert.strictEqual(
              (yield* Session.task(reopened, initialized.task.id)).pipe(Option.getOrThrow).state
                .status,
              'terminal',
            )
          }),
        ).pipe(Effect.provide(Layer.merge(NodeServices.layer, TemporaryStorage.layer))),
    )

    it.effect(
      'rolls back receipt and outcome on persistence rejection, then records only the successful batch',
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem
            const directory = yield* fs.makeTempDirectoryScoped({
              prefix: 'usage-receipt-rollback-',
            })
            let layer: Layer.Layer<
              Storage.Storage | import('effect-harness/StorageFormat').StorageFormat,
              StorageError | import('effect/sql/SqlError').SqlError,
              FileSystem.FileSystem
            > = TemporaryStorage.layer

            if (backend === 'jsonl')
              layer = Storage.layerJsonl({ filePath: `${directory}/state.jsonl` })
            const storage = Context.get(yield* Layer.build(layer), Storage.Storage)
            let fail = false
            const session = yield* Session.make().pipe(
              Effect.provideService(Storage.Storage, {
                ...storage,
                commit: (writes) =>
                  fail
                    ? Effect.fail(
                        new StorageError({
                          reason: 'io',
                          operation: 'fixture.commit',
                          message: 'batch rejected',
                        }),
                      )
                    : storage.commit(writes),
              }),
            )
            yield* Session.commit(session, Transaction.ensureRoot)
            fail = true
            const failed = yield* Session.commit(session, (tx) =>
              Usage.settle(tx, request(), (child) =>
                Transaction.appendEntry(child, root, { kind: 'receipt.outcome' }).pipe(
                  Effect.asVoid,
                ),
              ),
            ).pipe(Effect.exit)
            assert.isTrue(Exit.isFailure(failed))
            emptyModels(yield* Session.usage(session))
            assert.deepEqual(yield* Session.commit(session, (tx) => Usage.receipts(tx, root)), [])
            fail = false
            yield* Session.commit(session, (tx) =>
              Usage.settle(tx, request(), (child) =>
                Transaction.appendEntry(child, root, { kind: 'receipt.outcome' }).pipe(
                  Effect.asVoid,
                ),
              ),
            )
            assert.strictEqual((yield* Session.usage(session)).models[0]?.responses, 1)
            const entries = yield* Session.scanEntries(session, { conversationId: root }).pipe(
              Stream.runCollect,
            )
            assert.strictEqual(entries.length, 1)
          }),
        ).pipe(Effect.provide(NodeServices.layer)),
    )
  })
}

describe('usage receipt transaction boundaries', () => {
  it.effect('publishes entry and no-entry charge domains under one atomic session query', () =>
    withStorage(
      Effect.gen(function* () {
        const session = yield* Session.make()
        yield* Session.commit(session, Transaction.ensureRoot)
        const begun = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const observed = yield* Deferred.make<void>()
        const write = yield* Session.commit(session, (tx) =>
          Usage.settle(tx, request(), (child) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(begun, undefined)
              yield* Deferred.await(release)
              yield* Transaction.appendEntry(child, root, {
                kind: 'assistant',
                usage: request('second-distinct-charge').usage!,
              })
            }),
          ),
        ).pipe(Effect.forkChild)
        yield* Deferred.await(begun)
        const query = yield* Session.usage(session).pipe(
          Effect.tap(() => Deferred.succeed(observed, undefined)),
          Effect.forkChild({ startImmediately: true }),
        )
        assert.isFalse(yield* Deferred.isDone(observed))
        yield* Deferred.succeed(release, undefined)
        yield* Fiber.join(write)
        const summary = yield* Fiber.join(query)
        assert.strictEqual(summary.models[0]?.responses, 2)
        assert.strictEqual(summary.costs[0]?.model, 0.6)
      }),
    ),
  )
  it.effect(
    'caught callback failure preserves earlier drafts and discards new entries, receipt, task replacement and retirement',
    () =>
      withStorage(
        Effect.gen(function* () {
          const session = yield* Session.make()
          let escaped: Transaction.Transaction | undefined
          const task = yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              yield* Transaction.ensureRoot(tx)
              const task = yield* Transaction.createTask(tx, taskDraft())
              yield* Transaction.ensureDocument(tx, taskDocument, temporary(task.id))
              yield* Transaction.setDocument(tx, taskDocument, temporary(task.id), { value: 7 })
              const result = yield* Usage.settle(tx, nested(task.id), (child) =>
                Effect.gen(function* () {
                  escaped = child
                  yield* Transaction.setDocument(child, taskDocument, temporary(task.id), {
                    value: 99,
                  })
                  yield* Transaction.putTask(child, {
                    ...task,
                    state: { status: 'terminal', outcome: { status: 'completed', result: null } },
                  })
                  yield* Transaction.appendEntry(child, root, { kind: 'receipt.must.rollback' })
                  return yield* Effect.fail('outcome rejected')
                }),
              ).pipe(Effect.result)
              assert.isTrue(result._tag === 'Failure')
              assert.deepEqual(yield* Usage.receipts(tx, root), [])
              assert.strictEqual(
                Option.getOrThrow(yield* Transaction.snapshot(tx, taskDocument, temporary(task.id)))
                  .value.value,
                7,
              )
              return task
            }),
          )
          assert.strictEqual(
            Option.getOrThrow(yield* Session.task(session, task.id)).state.status,
            'running',
          )
          assert.strictEqual(
            Option.getOrThrow(yield* Session.snapshot(session, taskDocument, temporary(task.id)))
              .value.value,
            7,
          )
          assert.deepEqual(
            yield* Session.scanEntries(session, { conversationId: root }).pipe(Stream.runCollect),
            [],
          )
          assert.isDefined(escaped)
          if (escaped !== undefined) {
            const revoked = yield* Transaction.conversation(escaped, root).pipe(Effect.flip)
            assert.strictEqual(revoked._tag, 'SessionError')
            if (revoked._tag === 'SessionError') assert.strictEqual(revoked.reason, 'revoked')
          }
        }),
      ),
  )

  it.effect(
    'interrupted and defective callbacks leave no accounting or outcome drafts even when the exit is caught',
    () =>
      withStorage(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          yield* Session.commit(session, (tx) =>
            Effect.gen(function* () {
              const begun = yield* Deferred.make<void>()
              const fiber = yield* Usage.settle(tx, request(), (child) =>
                Effect.gen(function* () {
                  yield* Transaction.appendEntry(child, root, { kind: 'receipt.interrupted' })
                  yield* Deferred.succeed(begun, undefined)
                  return yield* Effect.never
                }),
              ).pipe(Effect.forkChild)
              yield* Deferred.await(begun)
              yield* Fiber.interrupt(fiber)
              assert.deepEqual(yield* Usage.receipts(tx, root), [])
              assert.isTrue(
                Exit.isFailure(
                  yield* Usage.settle(tx, request(), () => Effect.die('fixture defect')).pipe(
                    Effect.exit,
                  ),
                ),
              )
              assert.deepEqual(yield* Usage.receipts(tx, root), [])
              yield* Transaction.appendEntry(tx, root, { kind: 'receipt.after' })
            }),
          )
          emptyModels(yield* Session.usage(session))
          assert.strictEqual(
            (yield* Session.scanEntries(session, { conversationId: root }).pipe(Stream.runCollect))
              .length,
            1,
          )
        }),
      ),
  )

  it.effect(
    'serializes duplicate writers and rejects changed usage, outcome and linked identity',
    () =>
      withStorage(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          let changes = 0
          const fact = request()
          const observed = yield* Session.commit(session, (tx) =>
            Effect.forEach(
              [fact, fact, fact],
              (receipt) =>
                Usage.settle(tx, receipt, () =>
                  Effect.sync(() => {
                    changes++
                  }),
                ),
              { concurrency: 'unbounded' },
            ),
          )
          assert.strictEqual(changes, 1)
          assert.strictEqual(observed.filter((value) => value.inserted).length, 1)
          for (const changed of [
            { ...fact, outcome: { _tag: 'unknown' as const } },
            { ...fact, usage: { _tag: 'model' as const, tokens: { input: 1 } } },
          ])
            assertConflict(
              yield* Session.commit(session, (tx) => Usage.record(tx, changed)).pipe(Effect.flip),
            )
          assert.strictEqual((yield* Session.usage(session)).models[0]?.responses, 1)
          yield* Session.commit(session, (tx) => Usage.record(tx, request(undefined, 1)))
          assert.strictEqual((yield* Session.usage(session)).models[0]?.responses, 2)
        }),
      ),
  )

  it.effect(
    'counts entry-backed usage once and rejects aliases, differing charges and inherited links',
    () =>
      withStorage(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          const fact = request()
          const entry = yield* Session.commit(session, (tx) =>
            Transaction.appendEntry(tx, root, { kind: 'assistant', usage: fact.usage! }),
          )
          const linked = { ...fact, linkedEntryId: entry.id }
          yield* Session.commit(session, (tx) => Usage.record(tx, linked))
          assert.strictEqual((yield* Session.usage(session)).models[0]?.responses, 1)
          assert.strictEqual((yield* Session.usage(session)).costs[0]?.model, 0.3)
          assertConflict(
            yield* Session.commit(session, (tx) =>
              Usage.record(tx, { ...linked, identity: request('other').identity }),
            ).pipe(Effect.flip),
          )
          assertConflict(
            yield* Session.commit(session, (tx) =>
              Usage.record(tx, { ...linked, usage: Usage.model({ input: 20 }) }),
            ).pipe(Effect.flip),
          )
          const child = yield* Session.commit(session, (tx) =>
            Transaction.forkConversation(tx, root, entry.id, { ownership: { _tag: 'ownerless' } }),
          )
          assertConflict(
            yield* Session.commit(session, (tx) =>
              Usage.record(tx, { ...linked, conversationId: child.id }),
            ).pipe(Effect.flip),
          )
          emptyModels(yield* Session.usage(session, child.id))
          assert.strictEqual((yield* Session.usage(session)).models[0]?.responses, 1)
        }),
      ),
  )

  it.effect(
    'supplies a legacy entry missing its accounting field without retaining an unknown duplicate',
    () =>
      withStorage(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          const entry = yield* Session.commit(session, (tx) =>
            Transaction.appendEntry(tx, root, { kind: 'assistant' }),
          )
          yield* Session.commit(session, (tx) =>
            Usage.record(tx, { ...request(), linkedEntryId: entry.id }),
          )
          const summary = yield* Session.usage(session)
          assert.strictEqual(summary.legacyRecords, 0)
          assert.strictEqual(summary.models[0]?.responses, 1)
          assert.strictEqual(summary.costs[0]?.model, 0.3)
        }),
      ),
  )

  it.effect(
    'keeps missing external charges unknown, retains zero counters and copies caller values',
    () =>
      withStorage(
        Effect.gen(function* () {
          const session = yield* Session.make()
          yield* Session.commit(session, Transaction.ensureRoot)
          const tokens = { input: 0, output: 0 }
          const fact = { ...request(), usage: { _tag: 'model' as const, tokens } }
          yield* Session.commit(session, (tx) => Usage.record(tx, fact))
          tokens.input = 100
          yield* Session.commit(session, (tx) =>
            Usage.record(tx, {
              conversationId: root,
              identity: request('unknown').identity,
              outcome: { _tag: 'unknown' },
            }),
          )
          yield* Session.commit(session, (tx) =>
            Usage.record(tx, { ...request('partial'), usage: Usage.model({ input: 4 }) }),
          )
          const summary = yield* Session.usage(session)
          assert.isUndefined(summary.models[0]?.tokens.output)
          assert.strictEqual(summary.models[0]?.tokens.input, 4)
          assert.deepEqual(summary.models[0]?.tokenCoverage?.output, { known: 0, unknown: 1 })
          assert.strictEqual(summary.unknownAttempts, 1)
          assert.strictEqual(summary.unpricedModels, 2)
          assert.deepEqual(summary.costs, [])
        }),
      ),
  )

  it.effect('rejects invalid receipt payloads before creating any durable member', () =>
    withStorage(
      Effect.gen(function* () {
        const session = yield* Session.make()
        yield* Session.commit(session, Transaction.ensureRoot)
        for (const number of [Infinity, NaN, -1, 0.5]) {
          const failed = yield* Session.commit(session, (tx) =>
            Usage.record(tx, { ...request(), usage: Usage.model({ input: number }) }),
          ).pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(failed))
        }
        const task = yield* Session.commit(session, (tx) => Transaction.createTask(tx, taskDraft()))
        assertConflict(
          yield* Session.commit(session, (tx) =>
            Usage.record(tx, { ...nested(task.id), usage: Usage.model({ input: 1 }) }),
          ).pipe(Effect.flip),
        )
        const child = yield* Session.commit(session, (tx) =>
          Transaction.createConversation(tx, { ownership: { _tag: 'ownerless' } }),
        )
        assertConflict(
          yield* Session.commit(session, (tx) => Usage.record(tx, nested(task.id, child.id))).pipe(
            Effect.flip,
          ),
        )
        assert.deepEqual(yield* Session.commit(session, (tx) => Usage.receipts(tx, root)), [])
        assert.deepEqual(
          yield* Session.scanDocuments(session, {
            scope: { _tag: 'conversation', conversationId: root },
            kind: 'harness.usage.receipt',
          }).pipe(Stream.runCollect),
          [],
        )
      }),
    ),
  )
})

describe('rich accounting codecs', () => {
  const tiered: Usage.TieredPricing = {
    inputUncachedToken: 1,
    cacheReadToken: 0.1,
    cacheWriteToken: 2,
    cacheWrite1hToken: 4,
    outputToken: 3,
    reasoningToken: 5,
    currency: 'USD',
    source: 'fixture-tiers-v1',
  }
  it.effect(
    'prices disjoint cache and reasoning tiers and keeps totalTokens independently reported',
    () =>
      Effect.gen(function* () {
        const record = yield* Usage.modelTiered(
          {
            inputUncached: 10,
            cacheRead: 20,
            cacheWrite: 6,
            cacheWrite1h: 2,
            output: 9,
            reasoning: 3,
            totalTokens: 45,
          },
          undefined,
          tiered,
        )
        assert.deepEqual(record.cost?.components, {
          input: 10,
          cacheRead: 2,
          cacheWrite: 8,
          cacheWrite1h: 8,
          output: 18,
          reasoning: 15,
        })
        assert.strictEqual(record.cost?.amount, 61)
        const total = Usage.aggregate([{ id: 1, kind: 'assistant', usage: record }])
        assert.strictEqual(total.models[0]?.tokens.totalTokens, 45)
        assert.strictEqual(total.costs[0]?.model, 61)
      }),
  )
  it.effect(
    'does not price missing splits and rejects impossible subsets and numeric overflow',
    () =>
      Effect.gen(function* () {
        assert.isUndefined(
          (yield* Usage.modelTiered(
            { inputUncached: 1, cacheRead: 0, cacheWrite: 3, output: 5 },
            undefined,
            tiered,
          )).cost,
        )
        for (const tokens of [
          { cacheWrite: 1, cacheWrite1h: 2 },
          { output: 1, reasoning: 2 },
        ]) {
          const exit = yield* Usage.modelTiered(tokens, undefined, tiered).pipe(Effect.exit)
          assert.isTrue(Exit.isFailure(exit))
        }
        const exit = yield* Usage.modelTiered(
          {
            inputUncached: 10,
            cacheRead: 0,
            cacheWrite: 0,
            cacheWrite1h: 0,
            output: 0,
            reasoning: 0,
          },
          undefined,
          { ...tiered, inputUncachedToken: Number.MAX_VALUE },
        ).pipe(Effect.exit)
        assert.isTrue(Exit.isFailure(exit))
      }),
  )
  it.effect('retains legacy flat decoding, reported components and honest tool coverage', () =>
    Effect.gen(function* () {
      const legacy = yield* Schema.decodeEffect(Usage.RecordSchema)({
        _tag: 'model',
        tokens: { input: 10, output: 5 },
        pricing: { inputToken: 0.01, outputToken: 0.02, currency: 'USD', source: 'legacy' },
      })
      assert.strictEqual(legacy._tag, 'model')
      const flat = Usage.model({ input: 10, output: 5, reasoning: 2 }, undefined, {
        inputToken: 0.01,
        outputToken: 0.02,
        currency: 'USD',
        source: 'legacy',
      })
      assert.strictEqual(flat.cost?.amount, 0.2)
      const rich: Usage.Record = {
        _tag: 'tool',
        name: 'toString',
        tokens: { input: 0, cacheWrite1h: 2 },
        cost: {
          amount: 1,
          currency: 'JPY',
          source: 'reported',
          components: { input: 0, cacheWrite1h: 0.5 },
        },
      }
      const decoded = yield* Schema.decodeEffect(Usage.RecordSchema)(rich)
      assert.deepEqual(decoded, rich)
      const summary = Usage.aggregate([
        { id: 1, kind: 'tool.result', usage: decoded },
        { id: 2, kind: 'tool.result', usage: { _tag: 'tool', name: 'toString' } },
      ])
      assert.isUndefined(summary.tools[0]?.tokens?.input)
      assert.deepEqual(summary.tools[0]?.tokenCoverage?.input, { known: 0, unknown: 1 })
      assert.strictEqual(summary.unpricedTools, 1)
      assert.deepEqual(summary.costs, [
        { currency: 'JPY', model: 0, tool: 1, toolComponents: { input: 0, cacheWrite1h: 0.5 } },
      ])
      assert.deepEqual(
        Usage.aggregate([{ id: 1, kind: 'tool.result', usage: { _tag: 'tool', name: 'legacy' } }])
          .tools,
        [{ name: 'legacy', results: 1 }],
      )
      const later = Usage.aggregate([
        { id: 1, kind: 'tool.result', usage: { _tag: 'tool', name: 'toString' } },
        { id: 2, kind: 'tool.result', usage: rich },
      ])
      assert.isUndefined(later.tools[0]?.tokens?.input)
      assert.deepEqual(later.tools[0]?.tokenCoverage?.input, { known: 0, unknown: 1 })
    }),
  )
})
