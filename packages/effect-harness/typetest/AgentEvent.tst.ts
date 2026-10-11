import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import type * as Stream from 'effect/Stream'
import type * as Conversation from 'effect-harness/Conversation'
import * as Events from 'effect-harness/AgentEvent'
import type * as Observation from 'effect-harness/Observation'
import type { ObservationError } from 'effect-harness/ObservationError'

declare const conversation: Conversation.Conversation
declare const snapshot: Events.Snapshot
declare const frame: Events.Frame
test('event protocol has concrete failure and exhaustive frame discriminants', () => {
  expect(Events.watch(conversation)).type.toBe<Stream.Stream<Events.Frame, Observation.Failure>>()
  expect(Events.reduce(snapshot, frame)).type.toBe<
    Effect.Effect<Events.Snapshot, ObservationError>
  >()
  expect<Events.Frame['_tag']>().type.toBe<'Snapshot' | 'Rebase' | 'Batch'>()
  expect<Extract<Events.Event, { _tag: 'NestedEnd' }>['nested']['availability']>().type.toBe<
    'pending' | 'available' | 'unavailable'
  >()
})
