import { expect, test } from 'tstyche'
import type * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import type * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import type * as Document from 'effect-harness/Document'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type { HarnessError } from 'effect-harness/HarnessError'
import * as LiveState from 'effect-harness/LiveState'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import type { Storage } from 'effect-harness/Storage'
import type { StorageFormat } from 'effect-harness/StorageFormat'

declare const session: Session.Session

test('portable progress preserves document failures without adding services', () => {
  const target = LiveState.target(Record.ROOT_CONVERSATION_ID)
  expect(Session.snapshot(session, LiveState.document, target)).type.toBe<
    Effect.Effect<
      Option.Option<Document.Snapshot<typeof LiveState.SnapshotSchema>>,
      Session.Failure
    >
  >()
  expect(Session.watch(session, LiveState.document, target)).type.toBe<
    Stream.Stream<Document.Snapshot<typeof LiveState.SnapshotSchema>, Session.Failure>
  >()
})

test('native streaming retains the public scoped runtime acquisition contract', () => {
  expect(HarnessRuntime.make()).type.toBe<
    Effect.Effect<
      HarnessRuntime.HarnessRuntimeService,
      HarnessError,
      Scope.Scope | Storage | StorageFormat | LanguageModel.LanguageModel
    >
  >()
})
