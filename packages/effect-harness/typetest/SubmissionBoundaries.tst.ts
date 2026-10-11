import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as Conversation from 'effect-harness/Conversation'
import type * as Harness from 'effect-harness/Harness'
import type { HarnessError } from 'effect-harness/HarnessError'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type * as Storage from 'effect-harness/Storage'
import type { StorageFormat } from 'effect-harness/StorageFormat'
import * as Submission from 'effect-harness/Submission'
import * as Tool from 'effect-harness/Tool'
import * as Toolkit from 'effect-harness/Toolkit'

class Owner extends Context.Service<Owner, string>()('submission.types/Owner') {}
declare const self: Conversation.Conversation
declare const saved: Submission.Submission
declare const harness: Harness.HarnessService

test('serializable writes and busy inputs retain honest client error and receipt union', () => {
  expect(
    Conversation.submit(self, { type: 'input', content: 'hello', whenBusy: 'steer' }),
  ).type.toBe<Effect.Effect<Submission.Submission, HarnessError>>()
  expect(
    Conversation.submit(self, { type: 'write', entry: { kind: 'audit', data: { value: 1 } } }),
  ).type.toBe<Effect.Effect<Submission.Submission, HarnessError>>()
  expect(Conversation.submit).type.not.toBeCallableWith(self, {
    type: 'input',
    content: 'hi',
    whenBusy: 'interrupt',
  })
  expect(Conversation.submit).type.not.toBeCallableWith(self, {
    type: 'write',
    execute: () => Effect.void,
  })
  expect(Conversation.reset(self, { handoff: 'continue', requestId: 'reset' })).type.toBe<
    Effect.Effect<Submission.Submission, HarnessError>
  >()
  expect(self.pipe(Conversation.reset())).type.toBe<
    Effect.Effect<Submission.Submission, HarnessError>
  >()
  expect(self.pipe(Conversation.abort({ includeBackground: true }))).type.toBe<
    Effect.Effect<void, HarnessError>
  >()
  expect(Submission.read(saved)).type.toBe<Effect.Effect<Submission.Record, HarnessError>>()
  expect(Submission.wait(saved)).type.toBe<Effect.Effect<Submission.Settled, HarnessError>>()
  expect(harness.submission(saved.id)).type.toBe<
    Effect.Effect<Submission.Submission, HarnessError>
  >()
})

test('per-toolkit service capture is preserved through the submission runtime', () => {
  const tool = Tool.make('owned', {
    parameters: Schema.Struct({}),
    success: Schema.String,
  }).addDependency(Owner)
  const tools = Toolkit.make(tool)
  const handlers = tools.toLayer({ owned: () => Owner })
  const program = HarnessRuntime.make({ tools }).pipe(Effect.provide(handlers))
  expect(program).type.toBe<
    Effect.Effect<
      HarnessRuntime.HarnessRuntimeService,
      HarnessError,
      Scope.Scope | Storage.Storage | StorageFormat | LanguageModel.LanguageModel | Owner
    >
  >()
  const bound = program.pipe(Effect.provideService(Owner, 'owned'))
  expect(bound).type.toBe<
    Effect.Effect<
      HarnessRuntime.HarnessRuntimeService,
      HarnessError,
      Scope.Scope | Storage.Storage | StorageFormat | LanguageModel.LanguageModel
    >
  >()
  const noTools = HarnessRuntime.make()
  expect(noTools).type.toBe<
    Effect.Effect<
      HarnessRuntime.HarnessRuntimeService,
      HarnessError,
      Scope.Scope | Storage.Storage | StorageFormat | LanguageModel.LanguageModel
    >
  >()
})
