import { RestartWorker } from '../restart/RestartWorker.ts'
import * as Option from 'effect/Option'
import { assert, describe, it } from '@effect/vitest'
import * as NodeServices from '@effect/platform-node/NodeServices'
import * as SqliteClient from '@effect/sql-sqlite-node/SqliteClient'
import * as Effect from 'effect/Effect'
import * as Console from 'effect/Console'
import * as FileSystem from 'effect/FileSystem'
import * as Layer from 'effect/Layer'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as SqlClient from 'effect/sql/SqlClient'
import * as Spawner from 'effect/process/ChildProcessSpawner'
import * as Record from '@effect-harness/durable/Record'
import * as Session from '@effect-harness/durable/Session'
import * as Conversation from '@effect-harness/durable/Conversation'
import * as SqlStore from '@effect-harness/durable/storage/SqliteStore'
import * as Usage from '@effect-harness/durable/Usage'
import * as Inbox from '@effect-harness/durable/Inbox'

const marker = (handle: Spawner.ChildProcessHandle, prefix: string) =>
  handle.stdout.pipe(
    Stream.decodeText,
    Stream.splitLines,
    Stream.filter((line) => line.startsWith(prefix)),
    Stream.runHead,
    Effect.flatMap((result) =>
      result._tag === 'Some'
        ? Effect.succeed(result.value)
        : Effect.die(`Worker exited before ${prefix}`),
    ),
    Effect.timeout('15 seconds'),
  )
const inspect = (filename: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient
    // A separate writable transaction fences native notification vs physical COMMIT.
    const physical = yield* sql.withTransaction(
      Effect.gen(function* () {
        const rows = yield* sql<{
          state: string
        }>`SELECT state FROM durable_state WHERE singleton=1`
        const state = yield* Schema.decodeEffect(Schema.fromJsonString(Record.State))(
          rows[0]?.state ?? '{}',
        )
        const fs = yield* FileSystem.FileSystem
        const text = yield* fs.readFileString(filename + '.audit.jsonl')
        const Audit = Schema.Struct({
          phase: Schema.String,
          kind: Schema.String,
          data: Schema.String,
        })
        const audit = yield* Effect.forEach(text.split('\n').filter(Boolean), (line) =>
          Schema.decodeEffect(Schema.fromJsonString(Audit))(line),
        )
        return { state, audit }
      }),
    )
    const documents = yield* Effect.gen(function* () {
      const session = yield* Session.Session
      return {
        usage: (yield* session
          .snapshot(Usage.UsageDoc, { owner: Record.ROOT_CONVERSATION_ID })
          .pipe(Effect.map(Option.getOrUndefined)))?.value,
        live: (yield* session
          .snapshot(Inbox.LiveDoc, { owner: Record.ROOT_CONVERSATION_ID })
          .pipe(Effect.map(Option.getOrUndefined)))?.value,
      }
    }).pipe(Effect.provide(Session.layer.pipe(Layer.provide(SqlStore.layer))))
    return { ...physical, ...documents }
  }).pipe(Effect.provide(SqliteClient.layer({ filename })))
type Snapshot = Effect.Success<ReturnType<typeof inspect>>
const logs = (snapshot: Snapshot, kind: string) => snapshot.audit.filter((row) => row.kind === kind)
const data = (row: { data: string } | undefined) => row?.data ?? ''
const run = <E, R>(
  scenario: string,
  verify: (before: Snapshot, after: Snapshot) => Effect.Effect<void, E, R>,
) =>
  Effect.gen(function* () {
    const worker = yield* RestartWorker
    const filename = worker.filename
    const first = yield* worker.spawn('start')
    assert.strictEqual(yield* marker(first, 'HARNESS_READY'), 'HARNESS_READY')
    yield* first.kill({ killSignal: 'SIGKILL' })
    assert.strictEqual((yield* Effect.result(first.exitCode))._tag, 'Failure')
    assert.strictEqual(yield* first.isRunning, false)
    const before = yield* inspect(filename)
    assert.isAbove(before.state.nextSeq, 1)
    if (scenario === 'tool-deselected' || scenario === 'compaction-stale')
      yield* Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const agent = yield* tx.doc(Conversation.AgentDoc, {
              owner: Record.ROOT_CONVERSATION_ID,
            })
            if (scenario === 'tool-deselected') agent.tools = []
            else
              yield* tx.appendEntry(
                Record.ROOT_CONVERSATION_ID,
                yield* Conversation.resetDraft('new boundary'),
              )
          }),
        )
      }).pipe(
        Effect.provide(
          Session.layer.pipe(
            Layer.provide(SqlStore.layer),
            Layer.provide(SqliteClient.layer({ filename })),
          ),
        ),
      )
    const second = yield* worker.spawn('resume')
    yield* marker(second, 'HARNESS_DONE:').pipe(
      Effect.tapError(() =>
        second.kill({ killSignal: 'SIGKILL' }).pipe(
          Effect.andThen(inspect(filename)),
          Effect.flatMap((snapshot) =>
            Console.log(`Failed resume ${scenario}: ${JSON.stringify(snapshot)}`),
          ),
        ),
      ),
    )
    assert.strictEqual(yield* second.exitCode, 0)
    const after = yield* inspect(filename)
    assert.isAbove(after.state.nextSeq, before.state.nextSeq)
    yield* verify(before, after)
    const third = yield* worker.spawn('verify')
    yield* marker(third, 'HARNESS_DONE:')
    assert.strictEqual(yield* third.exitCode, 0)
    const cached = yield* inspect(filename)
    assert.deepStrictEqual(cached.state, after.state)
    assert.deepStrictEqual(cached.audit, after.audit)
    assert.deepStrictEqual(cached.usage, after.usage)
  }).pipe(
    Effect.provide(
      RestartWorker.layer({
        fixture: new URL('../restart/HarnessFixture.ts', import.meta.url).pathname,
        databaseEnv: 'HARNESS_RESTART_DB',
        phaseEnv: 'HARNESS_RESTART_PHASE',
        extraEnv: { HARNESS_RESTART_SCENARIO: scenario },
      }).pipe(Layer.provideMerge(NodeServices.layer)),
    ),
  )
const completed = (snapshot: Snapshot) => {
  const primary = snapshot.state.submissions.find(
    (submission) => submission.requestId === 'primary',
  )
  assert.strictEqual(primary?.status, 'done')
  assert.isTrue(snapshot.state.tasks.every((task) => task.state.status === 'terminal'))
}

describe('GenerationHarnessRestart', () => {
  for (const scenario of ['prepare', 'request', 'partial', 'retry', 'deferred'] as const) {
    // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
    it.live(`recovers ${scenario} through actual Generation and Submission executors`, () =>
      run(scenario, (before, after) =>
        Effect.sync(() => {
          completed(after)
          const tokens = Object.values(after.usage?.models ?? {}).reduce(
            (sum, item) => sum + item.totalTokens,
            0,
          )
          assert.strictEqual(tokens, scenario === 'retry' ? 30 : 15)
          if (scenario !== 'prepare') {
            const pinned = before.state.documents.find(
              (document) => document.record.kind === 'harness.model-request',
            )
            assert.isDefined(pinned)
            assert.match(JSON.stringify(pinned), /pinned preamble/)
            assert.match(JSON.stringify(pinned), /1234/)
          }
          const requests = logs(after, 'request')
          assert.strictEqual(
            requests.length,
            scenario === 'prepare' || scenario === 'deferred' ? 1 : 2,
          )
          assert.strictEqual(
            logs(after, 'section').length,
            scenario === 'prepare' || scenario === 'retry' ? 2 : 1,
          )
          if (scenario === 'request' || scenario === 'partial') {
            assert.strictEqual(data(requests[0]), data(requests[1]))
            const options = logs(after, 'options')
            assert.match(data(options[0]), /1234/)
            assert.match(data(options[1]), /1234/)
            assert.strictEqual(logs(after, 'beforeRequest').length, 2)
          }
          if (scenario === 'partial') {
            const aborted = after.state.entries.filter((item) =>
              JSON.stringify(item.entry.data).includes('"status":"aborted"'),
            )
            assert.strictEqual(aborted.length, 1)
            assert.match(JSON.stringify(aborted[0]?.entry.model), /committed partial/)
            assert.notMatch(data(requests[1]), /committed partial/)
          }
          if (scenario === 'retry') {
            assert.isDefined(before.live?.generation?.retry)
            assert.match(before.live?.generation?.retry?.error ?? '', /503/)
            assert.isAbove(before.live?.generation?.retry?.at ?? 0, 0)
            assert.match(data(logs(after, 'options').at(-1)), /999/)
            assert.strictEqual(logs(after, 'afterResponse').length, 2)
          }
          if (scenario === 'deferred') {
            assert.match(JSON.stringify(before.state.documents), /pinned-job/)
            assert.isAbove(before.live?.generation?.deferred?.pollAt ?? 0, 0)
            assert.strictEqual(logs(after, 'fetch').length, 1)
            assert.match(data(logs(after, 'fetch')[0]), /pinned-job/)
            assert.strictEqual(logs(after, 'afterResponse').length, 1)
          }
          const identities = logs(after, 'options').map(
            (row) => /"sessionId":"([^"]+)"/.exec(row.data)?.[1],
          )
          assert.isDefined(identities[0])
          assert.strictEqual(new Set(identities).size, 1)
        }),
      ),
    )
  }
  for (const scenario of [
    'tool-safe',
    'tool-unsafe',
    'tool-safe-unsafe',
    'tool-unsafe-safe',
    'tool-missing',
    'tool-deselected',
  ] as const) {
    // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
    it.live(`recovers committed intent with both-safe policy (${scenario})`, () =>
      run(scenario, (before, after) =>
        Effect.sync(() => {
          completed(after)
          assert.match(JSON.stringify(before.state.documents), /harness.tool-intent/)
          assert.match(before.live?.tools?.[0]?.output ?? '', /old output/)
          assert.match(JSON.stringify(before.live?.tools?.[0]?.details), /start/)
          assert.match(JSON.stringify(before.live?.tools?.[0]?.diagnostics), /diagnostic-start/)
          assert.strictEqual(
            Object.values(after.usage?.models ?? {}).reduce(
              (sum, item) => sum + item.totalTokens,
              0,
            ),
            30,
          )
          assert.strictEqual(logs(after, 'tool').length, scenario === 'tool-safe' ? 2 : 1)
          assert.strictEqual(logs(after, 'repair').length, 1)
          assert.strictEqual(logs(after, 'beforeTool').length, 1)
          const tool = after.state.entries.find((item) => item.entry.kind === 'harness.tool')?.entry
          assert.isDefined(tool)
          if (scenario === 'tool-safe') {
            assert.match(data(logs(after, 'tool')[1]), /"text":"pinned"/)
            assert.match(data(logs(after, 'tool')[1]), /\/after/)
            assert.match(JSON.stringify(tool?.model), /new output/)
            assert.notMatch(JSON.stringify(tool?.model), /old output/)
            assert.notMatch(JSON.stringify(tool?.data), /diagnostic-start/)
            assert.strictEqual(logs(after, 'afterTool').length, 1)
          } else {
            assert.match(JSON.stringify(tool?.model), /old output/)
            assert.match(JSON.stringify(tool?.data), /interrupted|unavailable/)
            assert.strictEqual(logs(after, 'afterTool').length, 0)
          }
        }),
      ),
    )
  }
  // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
  it.live(
    'compaction resends pinned immutable selection and places exactly one summary after restart',
    () =>
      run('compaction', (before, after) =>
        Effect.sync(() => {
          assert.strictEqual(logs(after, 'summary').length, 2)
          assert.strictEqual(data(logs(after, 'summary')[0]), data(logs(after, 'summary')[1]))
          assert.strictEqual(logs(after, 'beforeCompact').length, 1)
          const summaries = after.state.entries.filter(
            (item) => item.entry.kind === 'harness.compaction',
          )
          assert.strictEqual(summaries.length, 1)
          assert.match(JSON.stringify(summaries[0]?.entry.model), /persisted summary/)
          assert.strictEqual(
            after.state.submissions.filter(
              (submission) => submission.type === 'write' && submission.status === 'done',
            ).length,
            1,
          )
          assert.isAbove(after.state.entries.length, before.state.entries.length)
        }),
      ),
  )
  for (const scenario of ['compaction-queued', 'compaction-stale'] as const) {
    // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
    it.live(`compaction passive placement survives or rejects a newer reset (${scenario})`, () =>
      run(scenario, (before, after) =>
        Effect.sync(() => {
          assert.strictEqual(
            logs(after, 'summary').length,
            scenario === 'compaction-queued' ? 1 : 2,
          )
          assert.strictEqual(logs(after, 'beforeCompact').length, 1)
          const writes = after.state.submissions.filter((submission) => submission.type === 'write')
          assert.strictEqual(writes.length, 1)
          if (scenario === 'compaction-queued') {
            completed(after)
            assert.isTrue(
              before.state.submissions.some(
                (submission) => submission.type === 'write' && submission.status === 'queued',
              ),
            )
            assert.strictEqual(writes[0]?.status, 'done')
            assert.strictEqual(
              after.state.entries.filter((item) => item.entry.kind === 'harness.compaction').length,
              1,
            )
          } else {
            assert.strictEqual(writes[0]?.status, 'unanswered')
            if (writes[0]?.status === 'unanswered') assert.strictEqual(writes[0].reason, 'stale')
            assert.strictEqual(
              after.state.entries.filter((item) => item.entry.kind === 'harness.compaction').length,
              0,
            )
          }
        }),
      ),
    )
  }
  // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
  it.live(
    'queued steering and follow-up receipts survive the worker and are drained in order',
    () =>
      run('inbox', (before, after) =>
        Effect.sync(() => {
          assert.strictEqual(before.state.submissions.length, 3)
          assert.isTrue(
            before.state.submissions.some((submission) => submission.status === 'queued'),
          )
          assert.strictEqual(after.state.submissions.length, 3)
          assert.isTrue(after.state.submissions.every((submission) => submission.status === 'done'))
          const text = JSON.stringify(after.state.entries)
          assert.match(text, /steering/)
          assert.match(text, /follow-up/)
          assert.strictEqual(
            after.state.entries.filter((item) => item.entry.kind === 'harness.user').length,
            3,
          )
        }),
      ),
  )
  // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
  it.live(
    'a crash before tool intent repeats repairs/hooks and pins the newly admitted arguments',
    () =>
      run('tool-before-intent', (before, after) =>
        Effect.sync(() => {
          completed(after)
          assert.notMatch(JSON.stringify(before.state.documents), /harness.tool-intent/)
          assert.strictEqual(logs(after, 'repair').length, 2)
          assert.strictEqual(logs(after, 'beforeTool').length, 2)
          assert.strictEqual(logs(after, 'tool').length, 1)
          assert.match(data(logs(after, 'tool')[0]), /changed/)
        }),
      ),
  )
  for (const scenario of ['request-missing-model', 'deferred-missing-model'] as const) {
    // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
    it.live(`missing pinned catalogue entry settles unanswered (${scenario})`, () =>
      run(scenario, (_before, after) =>
        Effect.sync(() => {
          const primary = after.state.submissions.find(
            (submission) => submission.requestId === 'primary',
          )
          assert.strictEqual(primary?.status, 'unanswered')
          if (primary?.status === 'unanswered') assert.strictEqual(primary.reason, 'no_model')
          assert.strictEqual(logs(after, 'request').length, 1)
          assert.strictEqual(logs(after, 'fetch').length, 0)
        }),
      ),
    )
  }
  for (const scenario of [
    'compaction-select',
    'compaction-retry',
    'compaction-blocking',
  ] as const) {
    // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
    it.live(`compaction recovers its native boundary (${scenario})`, () =>
      run(scenario, (before, after) =>
        Effect.sync(() => {
          assert.strictEqual(
            logs(after, 'beforeCompact').length,
            scenario === 'compaction-select' ? 2 : 1,
          )
          assert.strictEqual(
            logs(after, 'summary').length,
            scenario === 'compaction-select' ? 1 : 2,
          )
          assert.strictEqual(
            after.state.entries.filter((item) => item.entry.kind === 'harness.compaction').length,
            1,
          )
          if (scenario === 'compaction-retry') {
            assert.isDefined(before.live?.compactions?.[0]?.retry)
            assert.isAbove(before.live?.compactions?.[0]?.retry?.at ?? 0, 0)
          }
          if (scenario === 'compaction-blocking') {
            completed(after)
            const compaction = before.state.tasks.find((task) => task.kind === 'harness.compaction')
            assert.isDefined(compaction?.owner)
          }
        }),
      ),
    )
  }
  // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
  it.live(
    'held actual tool outcome and memo survive SIGKILL until an owned native child drains',
    () =>
      run('hold', (before, after) =>
        Effect.sync(() => {
          completed(after)
          const held = before.state.tasks.find((task) => task.kind === 'harness.tool')
          assert.strictEqual(held?.state.status, 'completing')
          assert.deepStrictEqual(held?.memos, { memo: 'kept' })
          const child = before.state.tasks.find((task) => task.owner === held?.id)
          assert.isDefined(child)
          assert.strictEqual(logs(after, 'tool').length, 1)
          assert.strictEqual(logs(after, 'afterTool').length, 1)
          assert.strictEqual(logs(after, 'memo').length, 1)
          assert.strictEqual(logs(after, 'child').length, 2)
          assert.isUndefined(after.state.tasks.find((task) => task.id === held?.id)?.memos)
          assert.strictEqual(
            after.state.tasks.find((task) => task.id === child?.id)?.state.status,
            'terminal',
          )
        }),
      ),
  )
  for (const scenario of ['abort', 'abort-deferred'] as const) {
    // SIGKILL, child stdout and independent SQL COMMIT progress use host processes outside TestClock.
    it.live(
      `durable abort marks fence restarted work and native Abort reconciles receipts (${scenario})`,
      () =>
        run(scenario, (before, after) =>
          Effect.sync(() => {
            assert.isTrue(before.state.tasks.every((task) => task.abortRequested))
            const primary = after.state.submissions.find(
              (submission) => submission.requestId === 'primary',
            )
            assert.strictEqual(primary?.status, 'unanswered')
            if (primary?.status === 'unanswered') assert.strictEqual(primary.reason, 'aborted')
            assert.isTrue(after.state.tasks.every((task) => task.state.status === 'terminal'))
            assert.strictEqual(logs(after, 'tool').length, scenario === 'abort' ? 1 : 0)
            assert.strictEqual(logs(after, 'fetch').length, 0)
            if (scenario === 'abort') {
              const tool = after.state.entries.find(
                (item) => item.entry.kind === 'harness.tool',
              )?.entry
              assert.match(JSON.stringify(tool?.model), /old output/)
              assert.match(JSON.stringify(tool?.data), /diagnostic-start/)
              assert.match(JSON.stringify(tool?.data), /"phase":"start"/)
            }
            assert.strictEqual(
              logs(after, 'cancel-deferred').length,
              scenario === 'abort-deferred' ? 1 : 0,
            )
          }),
        ),
    )
  }
})
