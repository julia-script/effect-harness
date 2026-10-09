import { expect, test } from 'tstyche'
import * as Context from 'effect/Context'
import * as Effect from 'effect/Effect'
import type * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import type * as Scope from 'effect/Scope'
import type * as Stream from 'effect/Stream'
import * as Document from '../../src/refactor/Document.js'
import * as Session from '../../src/refactor/Session.js'
import type { Storage } from '../../src/refactor/Storage.js'
import * as Transaction from '../../src/refactor/Transaction.js'

class Decoder extends Context.Service<Decoder, string>()('typetest/refactor/Decoder') {}
class Encoder extends Context.Service<Encoder, string>()('typetest/refactor/Encoder') {}
class Application extends Context.Service<Application, number>()('typetest/refactor/Application') {}

const codec = Schema.Struct({ instant: Schema.DateFromString }).pipe(
  Schema.middlewareDecoding((effect) => Effect.flatMap(Decoder, () => effect)),
  Schema.middlewareEncoding((effect) => Effect.flatMap(Encoder, () => effect)),
)
const document = Document.define({
  kind: 'clock',
  scope: 'session',
  version: 1,
  schema: codec,
  initial: () => ({ instant: new Date() }),
})
const target = { scope: { _tag: 'session' } } satisfies Document.Target
declare const session: Session.Session
declare const transaction: Transaction.Transaction

test('Session construction requires only Storage and Scope', () => {
  expect(Session.make()).type.toBe<
    Effect.Effect<Session.Session, Session.SessionError, Storage | Scope.Scope>
  >()
})

test('both dual forms preserve schema requirements and decoded values', () => {
  type Snapshot = Document.Snapshot<typeof codec>
  type Read = Effect.Effect<Option.Option<Snapshot>, Session.Failure, Decoder>
  expect(Session.snapshot(session, document, target)).type.toBe<Read>()
  expect(session.pipe(Session.snapshot(document, target))).type.toBe<Read>()
  expect(Session.watch(session, document, target)).type.toBe<
    Stream.Stream<Snapshot, Session.Failure, Decoder>
  >()
  expect(session.pipe(Session.watch(document, target))).type.toBe<
    Stream.Stream<Snapshot, Session.Failure, Decoder>
  >()
  expect(Transaction.snapshot(transaction, document, target)).type.toBe<Read>()
  expect(transaction.pipe(Transaction.snapshot(document, target))).type.toBe<Read>()
  expect(Transaction.ensureDocument(transaction, document, target)).type.toBe<
    Effect.Effect<Snapshot, Session.Failure, Decoder | Encoder>
  >()
  expect(transaction.pipe(Transaction.ensureDocument(document, target))).type.toBe<
    Effect.Effect<Snapshot, Session.Failure, Decoder | Encoder>
  >()
  expect(Transaction.setDocument(transaction, document, target, { instant: new Date() })).type.toBe<
    Effect.Effect<void, Session.Failure, Encoder>
  >()
  expect(
    transaction.pipe(Transaction.setDocument(document, target, { instant: new Date() })),
  ).type.toBe<Effect.Effect<void, Session.Failure, Encoder>>()
  expect(
    transaction.pipe(
      Transaction.updateDocument(document, target, (value) => {
        expect(value.instant).type.toBe<Date>()
        return value
      }),
    ),
  ).type.toBe<Effect.Effect<void, Session.Failure, Decoder | Encoder>>()
})

test('commit preserves callback success, errors and requirements in both forms', () => {
  const callback = (_tx: Transaction.Transaction) =>
    Application.pipe(
      Effect.flatMap((value) =>
        value > 0 ? Effect.succeed('accepted' as const) : Effect.fail('rejected' as const),
      ),
    )
  type Result = Effect.Effect<'accepted', 'rejected' | Session.Failure, Application>
  expect(Session.commit(session, callback)).type.toBe<Result>()
  expect(session.pipe(Session.commit(callback))).type.toBe<Result>()
  expect(session.pipe(Session.commit(callback, {}))).type.toBe<Result>()
  expect(session.pipe(Session.commits())).type.toBe<
    Stream.Stream<Session.Commit, Session.Failure>
  >()
  expect(Session.commits(session)).type.toBe<Stream.Stream<Session.Commit, Session.Failure>>()
})
