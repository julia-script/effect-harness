import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import type * as Stream from 'effect/Stream'
import type * as Harness from 'effect-harness/Harness'
import * as Graph from 'effect-harness/TaskGraph'
import type * as Observation from 'effect-harness/Observation'
import type { ObservationError } from 'effect-harness/ObservationError'
import * as Record from 'effect-harness/Record'

declare const harness: Harness.HarnessService
declare const initial: Graph.Snapshot
declare const frame: Graph.Frame
test('concrete graph boundary and reducer', () => {
  expect(Graph.snapshot(harness)).type.toBe<Effect.Effect<Graph.Snapshot, Observation.Failure>>()
  expect(Graph.watch(harness)).type.toBe<Stream.Stream<Graph.Frame, Observation.Failure>>()
  expect(Graph.reduce(initial, frame)).type.toBe<Effect.Effect<Graph.Snapshot, ObservationError>>()
  expect<Record.TaskId>().type.not.toBeAssignableTo<Record.ConversationId>()
  expect(Record.ROOT_CONVERSATION_ID).type.toBe<Record.ConversationId>()
})
