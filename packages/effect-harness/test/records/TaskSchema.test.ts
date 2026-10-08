import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Record from '../../src/Record.ts'
import * as Task from '../../src/Task.ts'

const identity = {
  id: 2,
  conversationId: 1,
  kind: 'test/schema-task',
  version: 1,
  input: null,
  background: false,
  abortRequested: false,
}
const validStates = [
  { status: 'pending', checkpoint: { phase: 'prepare', data: [1, 2] } },
  { status: 'running', checkpoint: { phase: 'request' } },
  { status: 'waiting', checkpoint: { phase: 'join' }, on: [3, 4], policy: 'allSettled' },
  { status: 'waiting', checkpoint: { phase: 'join' }, on: [], policy: 'failFast' },
  { status: 'completing', outcome: { status: 'completed', result: { answer: 4 } } },
  { status: 'terminal', outcome: { status: 'completed', result: null } },
  {
    status: 'terminal',
    outcome: { status: 'failed', error: { message: 'tool failed' }, result: false },
  },
  { status: 'terminal', outcome: { status: 'aborted', reason: 'cancelled' } },
  { status: 'terminal', outcome: { status: 'orphaned', reason: 'missing_task' } },
  { status: 'terminal', outcome: { status: 'faulted', error: { message: 'unexpected defect' } } },
]
const invalidStates = [
  { status: 'pending' },
  { status: 'running' },
  { status: 'pending', checkpoint: null },
  { status: 'running', checkpoint: [] },
  { status: 'running', checkpoint: { data: 1 } },
  { status: 'running', checkpoint: { phase: '' } },
  { status: 'running', checkpoint: { phase: 'run' }, on: [3] },
  { status: 'pending', checkpoint: { phase: 'run' }, policy: 'allSettled' },
  {
    status: 'pending',
    checkpoint: { phase: 'run' },
    outcome: { status: 'completed', result: null },
  },
  { status: 'waiting', checkpoint: { phase: 'join' }, on: [3] },
  { status: 'waiting', checkpoint: { phase: 'join' }, policy: 'allSettled' },
  { status: 'waiting', checkpoint: { phase: 'join' }, on: [3], policy: 'invalid' },
  { status: 'waiting', checkpoint: { phase: 'join' }, on: [0], policy: 'allSettled' },
  {
    status: 'waiting',
    checkpoint: { phase: 'join' },
    on: [3],
    policy: 'allSettled',
    outcome: { status: 'aborted' },
  },
  { status: 'completing' },
  { status: 'terminal' },
  { status: 'terminal', outcome: null },
  { status: 'terminal', outcome: { status: 'completed' } },
  { status: 'terminal', outcome: { status: 'failed' } },
  { status: 'terminal', outcome: { status: 'orphaned' } },
  { status: 'terminal', outcome: { status: 'faulted', error: 'defect' } },
  { status: 'terminal', checkpoint: { phase: 'run' }, outcome: { status: 'aborted' } },
  { status: 'completing', on: [], outcome: { status: 'aborted' } },
  { status: 'terminal', policy: 'failFast', outcome: { status: 'aborted' } },
]

describe('task record wire schemas', () => {
  it.effect('round-trips every durable task state and supported outcome', () =>
    Effect.gen(function* () {
      for (const state of validStates) {
        const decoded = yield* Schema.decodeUnknownEffect(Record.Task)({ ...identity, state })
        const serialized = yield* Schema.encodeEffect(Schema.fromJsonString(Record.Task))(decoded)
        assert.deepEqual(
          yield* Schema.decodeEffect(Schema.fromJsonString(Record.Task))(serialized),
          decoded,
        )
      }
    }),
  )

  it.effect('rejects missing and forbidden phase-state field combinations', () =>
    Effect.gen(function* () {
      for (const state of invalidStates) {
        const decoded = yield* Effect.result(
          Schema.decodeUnknownEffect(Record.Task)({ ...identity, state }),
        )
        assert.isTrue(Result.isFailure(decoded), `Invalid ${state.status} state was accepted`)
      }
    }),
  )

  it.effect('drops live memos only at completing or terminal boundaries', () =>
    Effect.gen(function* () {
      for (const status of ['pending', 'running', 'waiting'] as const) {
        const state =
          status === 'waiting'
            ? { status, checkpoint: { phase: 'join' }, on: [3], policy: 'allSettled' }
            : { status, checkpoint: { phase: 'run' } }
        const decoded = yield* Schema.decodeUnknownEffect(Record.Task)({
          ...identity,
          state,
          memos: { decision: true },
        })
        assert.deepEqual(decoded.memos, { decision: true })
      }
      for (const status of ['completing', 'terminal'] as const) {
        assert.isTrue(
          Result.isFailure(
            yield* Effect.result(
              Schema.decodeEffect(Record.Task)({
                ...identity,
                state: { status, outcome: { status: 'aborted' } },
                memos: { decision: true },
              }),
            ),
          ),
        )
      }
    }),
  )

  it.effect('shares the concrete outcome codec with the task definition boundary', () =>
    Effect.gen(function* () {
      assert.strictEqual(Task.Outcome, Record.TaskOutcome)
      const outcome = yield* Schema.decodeEffect(Task.Outcome)({
        status: 'completed',
        result: { value: 42 },
      })
      assert.deepEqual(outcome, { status: 'completed', result: { value: 42 } })
    }),
  )
})
