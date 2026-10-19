import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Stream from 'effect/Stream'
import type * as Conversation from 'effect-harness/Conversation'
import * as View from 'effect-harness/ConversationView'
import type * as Observation from 'effect-harness/Observation'
import type { ObservationError } from 'effect-harness/ObservationError'

class Enrichment extends Context.Service<Enrichment, string>()('observation/enrichment') {}
declare const conversation: Conversation.Conversation
declare const snapshot: View.Snapshot
declare const frame: View.Frame
test('callback consumer services and failures remain visible', () => {
  expect(View.snapshot(conversation)).type.toBe<Effect.Effect<View.Snapshot, Observation.Failure>>()
  expect(View.reduce(snapshot, frame)).type.toBe<Effect.Effect<View.Snapshot, ObservationError>>()
  const observed = View.watchWith(conversation, () =>
    Enrichment.pipe(
      Effect.flatMap((text) => Effect.fail({ _tag: 'EnrichmentFailure' as const, text })),
    ),
  )
  expect(observed).type.toBe<
    Stream.Stream<
      never,
      Observation.Failure | { _tag: 'EnrichmentFailure'; text: string },
      Enrichment
    >
  >()
  expect(observed).type.not.toBeAssignableTo<Stream.Stream<never, Observation.Failure>>()
})
