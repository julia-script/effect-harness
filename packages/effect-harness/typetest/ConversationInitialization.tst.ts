import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Layer from 'effect/Layer'
import type * as Scope from 'effect/Scope'
import type * as LanguageModel from 'effect/ai/LanguageModel'
import * as ConversationInitializer from 'effect-harness/ConversationInitializer'
import * as Harness from 'effect-harness/Harness'
import type { HarnessBackend } from 'effect-harness/HarnessBackend'
import type { HarnessError } from 'effect-harness/HarnessError'
import * as HarnessRuntime from 'effect-harness/HarnessRuntime'
import type * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import type { Storage } from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'
import { ConversationInitializer as Namespace } from 'effect-harness'

class Seed extends Context.Service<Seed, number>()('typetest/initialization/Seed') {}
class Audit extends Context.Service<Audit, string>()('typetest/initialization/Audit') {}
const first = ConversationInitializer.make({
  execute: (tx, record) =>
    Effect.gen(function* () {
      expect(tx).type.toBe<Transaction.Transaction>()
      expect(record).type.toBe<Record.Conversation>()
      yield* Seed
      yield* Transaction.conversation(tx, record.id)
    }),
})
const second = Namespace.make({
  execute: () =>
    Effect.gen(function* () {
      yield* Audit
      yield* Effect.addFinalizer(() => Effect.void)
    }),
})
const options = { initializers: [first, second] } as const

test('Session captures callback requirements while keeping callback errors at the creation boundary', () => {
  expect<ConversationInitializer.Requirements<typeof first>>().type.toBe<Seed>()
  expect<ConversationInitializer.Requirements<typeof second>>().type.toBe<Audit>()
  expect(Session.make(options)).type.toBe<
    Effect.Effect<Session.Session, Session.SessionError, Storage | Scope.Scope | Seed | Audit>
  >()
  expect(Session.make()).type.toBe<
    Effect.Effect<Session.Session, Session.SessionError, Storage | Scope.Scope>
  >()
  expect(
    Session.make({
      initializers: [
        ConversationInitializer.make({ execute: () => Effect.fail('callback error') }),
      ],
    }),
  ).type.toBe<Effect.Effect<Session.Session, Session.SessionError, Storage | Scope.Scope>>()
})

test('initializer requirements reach runtime construction and both public Layers', () => {
  expect(HarnessRuntime.make(options)).type.toBe<
    Effect.Effect<
      HarnessRuntime.HarnessRuntimeService,
      HarnessError,
      Storage | LanguageModel.LanguageModel | Scope.Scope | Seed | Audit
    >
  >()
  expect(HarnessRuntime.layer(options)).type.toBe<
    Layer.Layer<
      HarnessRuntime.HarnessRuntime | HarnessBackend,
      HarnessError,
      Storage | LanguageModel.LanguageModel | Seed | Audit
    >
  >()
  expect(Harness.layerLocal(options)).type.toBe<
    Layer.Layer<Harness.Harness, HarnessError, Storage | LanguageModel.LanguageModel | Seed | Audit>
  >()
  expect(HarnessRuntime.layer()).type.toBe<
    Layer.Layer<
      HarnessRuntime.HarnessRuntime | HarnessBackend,
      HarnessError,
      Storage | LanguageModel.LanguageModel
    >
  >()
})
