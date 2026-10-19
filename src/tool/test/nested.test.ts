/* oxlint-disable max-lines-per-function -- describe blocks */
/**
 * Nested calls: a running tool calling another through `Call.execute`, ported in spirit from pi's
 * `harness-nested-tools.test.ts` and `harness-nested-tools-restart.test.ts`.
 */
import { describe, expect, test } from 'bun:test'
import { Deferred, Effect, Exit, Layer, Schema } from 'effect'
import { conversation, open, openClosable, runTest } from '../../task/test/support.ts'
import { Call, Tool, Toolkit, ToolTask } from '../index.ts'
import { NestedCallRefused } from '../ToolError.ts'
import type { Script } from './support.ts'
import { answer, call, script } from './support.ts'

const Double = Tool.make('double', {
  description: 'Double a number',
  parameters: Schema.Struct({ n: Schema.Finite }),
  success: Schema.Finite,
  failure: Schema.Struct({ reason: Schema.String }),
})
const ModelOnly = Tool.make('model_only', {
  description: 'Only the model calls it',
  parameters: Schema.Struct({}),
  callers: ['model'],
})
const Wait = Tool.make('wait', { description: 'Wait', parameters: Schema.Struct({}) })
const Request = Schema.Struct({
  name: Schema.String,
  args: Schema.Record(Schema.String, Schema.Json),
  key: Schema.optionalKey(Schema.String),
})
/** Makes the nested calls it is given, one after another, and returns their outcomes. */
const Caller = Tool.make('caller', {
  description: 'Call tools',
  parameters: Schema.Struct({ calls: Schema.Array(Request) }),
  success: Schema.Array(Call.Outcome),
  failure: Schema.String,
})
/** `caller`, replay-safe, waiting for `release` after its calls. */
const SafeCaller = Tool.make('safe_caller', {
  description: 'Call tools, then wait',
  parameters: Schema.Struct({ calls: Schema.Array(Request) }),
  success: Schema.Array(Call.Outcome),
  failure: Schema.String,
  replay: 'safe',
})

const toolkit = Toolkit.make(Double, ModelOnly, Wait, Caller, SafeCaller)

const makeCalls = (calls: ReadonlyArray<typeof Request.Type>) =>
  Effect.forEach(calls, ({ name, args, key }) =>
    Call.execute(name, args, key === undefined ? {} : { key }),
  ).pipe(
    Effect.mapError((error) =>
      error instanceof NestedCallRefused ? error.message : 'runtime failure',
    ),
  )

const handlers = (current: Script) =>
  toolkit.toLayer({
    double: ({ n }) => {
      current.runs.push('double')
      return n < 0 ? Effect.fail({ reason: 'negative' }) : Effect.succeed(n * 2)
    },
    model_only: () => Effect.void,
    wait: () => Effect.andThen(Deferred.done(current.started, Exit.void), Effect.never),
    caller: ({ calls }) => makeCalls(calls),
    safe_caller: ({ calls }) =>
      Effect.tap(makeCalls(calls), () =>
        Effect.andThen(Deferred.done(current.started, Exit.void), Deferred.await(current.release)),
      ),
  })

const tasks = (current: Script) =>
  Effect.map(Layer.build(handlers(current)), (implementations) =>
    ToolTask.make(toolkit, implementations),
  )

/** The outcomes a caller returned, from its result entry. */
const Result = Schema.Struct({
  content: Schema.Tuple([
    Schema.Struct({ text: Schema.fromJsonString(Schema.Array(Call.Outcome)) }),
  ]),
})
const outcomesOf = (message: Record<string, unknown> | undefined) =>
  Schema.decodeUnknownSync(Result)(message).content[0].text

describe('nested calls', () => {
  test("return the called tool's encoded success, failure, or why it could not run", () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* script
        const { session, scheduler } = yield* open(storage, [yield* tasks(current)])
        const conversationId = yield* conversation(session)
        const id = yield* call(session, conversationId, 'caller', {
          calls: [
            { name: 'double', args: { n: 21 } },
            { name: 'double', args: { n: -1 } },
            { name: 'double', args: { n: 'x' } },
            { name: 'model_only', args: {} },
            { name: 'missing', args: {} },
          ],
        })
        const { state } = yield* scheduler.waitForTask(id)
        const { message } = yield* answer(session, state.outcome)
        const outcomes = outcomesOf(message)
        expect(outcomes[0]).toEqual({ isError: false, value: 42, diagnostics: [] })
        expect(outcomes[1]).toEqual({
          isError: true,
          value: { reason: 'negative' },
          diagnostics: [],
        })
        expect(outcomes.slice(2).map((each) => each.diagnostics[0]?.code)).toEqual([
          'invalid_arguments',
          'tool_unavailable',
          'tool_unavailable',
        ])
        // Nested results are not transcript entries.
        const entries = yield* Effect.orDie(
          session.commit((tx) => tx.scanEntries({ conversationId, order: 'ascending' }, 100)),
        )
        expect(entries.items.map((entry) => entry.kind)).toEqual(['pi.assistant', 'pi.tool-result'])
      }),
    ))

  test('refuse a key reused for other arguments', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* script
        const { session, scheduler } = yield* open(storage, [yield* tasks(current)])
        const id = yield* call(session, yield* conversation(session), 'caller', {
          calls: [
            { name: 'double', args: { n: 1 }, key: 'k' },
            { name: 'double', args: { n: 2 }, key: 'k' },
          ],
        })
        const { state } = yield* scheduler.waitForTask(id)
        const { message } = yield* answer(session, state.outcome)
        expect(JSON.stringify(message?.['content'])).toContain('already made with another tool')
      }),
    ))

  test('still running when the caller settles are aborted', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const current = yield* script
        const { session, scheduler } = yield* open(storage, [yield* tasks(current)])
        const id = yield* call(session, yield* conversation(session), 'caller', {
          calls: [{ name: 'wait', args: {} }],
        })
        yield* Deferred.await(current.started)
        yield* scheduler.abort(id)
        const { state } = yield* scheduler.waitForTask(id)
        expect(state.outcome.status).toBe('aborted')
        const all = yield* Effect.orDie(session.commit((tx) => tx.scanTasks({}, 100)))
        expect(all.items.map((record) => record.state.status)).toEqual(['terminal', 'terminal'])
      }),
    ))

  test('outside a tool call are refused', () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const refused = yield* Effect.flip(Call.execute('double', { n: 1 }))
        expect(refused).toBeInstanceOf(NestedCallRefused)
      }),
    ))
})

describe('nested call recovery', () => {
  test('a replay-safe caller reattaches to the calls it made, without running them again', () =>
    runTest((storage) =>
      Effect.gen(function* () {
        const first = yield* script
        const before = yield* openClosable(storage, [yield* tasks(first)])
        const id = yield* call(before.session, yield* conversation(before.session), 'safe_caller', {
          calls: [{ name: 'double', args: { n: 4 } }],
        })
        yield* Deferred.await(first.started)
        expect(first.runs).toEqual(['double'])
        yield* before.close
        const second = yield* script
        yield* Deferred.done(second.release, Exit.void)
        const { session, scheduler } = yield* open(storage, [yield* tasks(second)])
        const { state } = yield* scheduler.waitForTask(id)
        expect(state.outcome.status).toBe('completed')
        const { message } = yield* answer(session, state.outcome)
        expect(outcomesOf(message)).toEqual([{ isError: false, value: 8, diagnostics: [] }])
        expect(second.runs).toEqual([])
      }),
    ))
})
