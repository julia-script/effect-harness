import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Option from 'effect/Option'
import * as Schema from 'effect/Schema'
import * as Document from 'effect-harness/Document'
import * as Record from 'effect-harness/Record'
import * as Session from 'effect-harness/Session'
import * as Storage from 'effect-harness/Storage'
import * as Transaction from 'effect-harness/Transaction'

const counter = (kind: string) =>
  Document.define({
    kind,
    scope: 'conversation',
    history: 'rewindable',
    fork: 'asOf',
    version: 1,
    schema: Schema.Struct({ count: Schema.Natural }),
    initial: () => ({ count: 0 }),
  })
const first = counter('first')
const second = counter('second')
const root = Record.ROOT_CONVERSATION_ID
const target = { scope: { _tag: 'conversation', conversationId: root } } as const

describe('fork operation rollback', () => {
  for (const caught of [false, true]) {
    for (const cached of [false, true]) {
      it.effect(
        `${caught ? 'caught' : 'escaping'} failure leaves no copies with a ${cached ? 'cached' : 'cold'} first document`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              const storage = yield* Storage.Storage
              const at = yield* Effect.scoped(
                Effect.gen(function* () {
                  const session = yield* Session.make()
                  return yield* Session.commit(session, (tx) =>
                    Effect.gen(function* () {
                      yield* Transaction.ensureRoot(tx)
                      yield* Transaction.ensureDocument(tx, first, target)
                      yield* Transaction.ensureDocument(tx, second, target)
                      return yield* Transaction.appendEntry(tx, root, { kind: 'cutoff' })
                    }),
                  )
                }),
              )
              const writes: Array<Record.StorageWrite> = []
              const monitored: Storage.Storage['Service'] = {
                ...storage,
                commit: (input) => {
                  const batch = Array.from(input)
                  writes.push(...batch)
                  return storage.commit(batch)
                },
              }
              const session = yield* Session.make().pipe(
                Effect.provideService(Storage.Storage, monitored),
              )
              if (cached) yield* Session.snapshot(session, first, target)
              const result = yield* Session.commit(session, (tx) =>
                Effect.gen(function* () {
                  yield* Transaction.setDocument(tx, second, target, { count: 9 })
                  const fork = Transaction.forkConversation(tx, root, at.id, {
                    ownership: { _tag: 'ownerless' },
                  })
                  if (caught) {
                    const failure = yield* fork.pipe(Effect.result)
                    assert.strictEqual(failure._tag, 'Failure')
                    if (failure._tag === 'Failure') {
                      assert.isTrue(Schema.is(Session.SessionError)(failure.failure))
                      if (Schema.is(Session.SessionError)(failure.failure)) {
                        assert.strictEqual(failure.failure.reason, 'conflict')
                        assert.strictEqual(failure.failure.operation, 'conversation.fork')
                      }
                    }
                  } else yield* fork
                }),
              ).pipe(Effect.result)
              assert.strictEqual(result._tag, caught ? 'Success' : 'Failure')
              assert.deepEqual(
                writes.filter((write) => write._tag === 'document.copy'),
                [],
              )
              assert.deepEqual(
                writes.filter((write) => write._tag === 'conversation'),
                [],
              )
              const source = yield* Session.snapshot(session, second, target)
              assert.isTrue(Option.isSome(source))
              if (Option.isSome(source)) {
                assert.strictEqual(source.value.value.count, caught ? 9 : 0)
              }
            }),
          ).pipe(Effect.provide(Storage.layerMemory)),
      )
    }
  }
})
