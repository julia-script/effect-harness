import { assert, describe, it } from '@effect/vitest'
import * as Data from 'effect/Data'
import * as Deferred from 'effect/Deferred'
import * as Effect from 'effect/Effect'
import * as Fiber from 'effect/Fiber'
import type * as Scope from 'effect/Scope'
import * as Schema from 'effect/Schema'
import * as Stream from 'effect/Stream'
import * as Document from '../../src/Document.ts'
import * as Record from '../../src/Record.ts'
import * as Session from '../../src/Session.ts'
import { Store } from '../../src/Store.ts'
import { rejected } from '../../src/StorageError.ts'
import * as Memory from '../../src/storage/Memory.ts'
import { sessionLayer } from '../../src/testing/Storage.ts'
const root = Record.ROOT_CONVERSATION_ID
const token = Document.defineUnsafe({
  kind: 'readonlyschema',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({
    count: Schema.Finite,
    nested: Schema.Struct({ items: Schema.Array(Schema.Finite) }),
  }),
  initial: () => ({ count: 0, nested: { items: [0] } }),
})
const fail = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.flip, Effect.orDie)
const provide = <A, E>(effect: Effect.Effect<A, E, Session.Session | Store | Scope.Scope>) =>
  Effect.scoped(effect.pipe(Effect.provide(sessionLayer(Memory.layer))))
class DomainError extends Data.TaggedError('DomainError')<{ readonly message: string }> {}
describe('transaction adversarial boundaries', () => {
  it.effect('preserves user typed errors and interrupts without poison or partial adoption', () =>
    provide(
      Effect.gen(function* () {
        const session = yield* Session.Session
        const store = yield* Store
        const error = new DomainError({ message: 'domain validation' })
        assert.strictEqual(
          yield* fail(
            session.transaction(
              Effect.fnUntraced(function* (tx) {
                yield* tx.ensureRoot
                yield* tx.doc(token)
                return yield* error
              }),
            ),
          ),
          error,
        )
        assert.deepStrictEqual((yield* store.read).conversations, [])
        const started = yield* Deferred.make<void>()
        const fiber = yield* session
          .transaction(
            Effect.fnUntraced(function* (tx) {
              yield* tx.ensureRoot
              yield* tx.doc(token)
              yield* Deferred.succeed(started, undefined)
              return yield* Effect.never
            }),
          )
          .pipe(Effect.forkScoped)
        yield* Deferred.await(started)
        yield* Fiber.interrupt(fiber)
        assert.deepStrictEqual((yield* store.read).conversations, [])
        yield* session.root()
        assert.deepStrictEqual((yield* store.read).conversations, [{ id: root }])
      }),
    ),
  )
  it.effect('makes nested Schema readonly structures mutable inside revocable drafts', () =>
    provide(
      Effect.gen(function* () {
        const session = yield* Session.Session
        let retained: Document.Draft<typeof token.definition.schema.Type> | undefined
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const d = yield* tx.doc(token)
            retained = d
            d.count = 1
            d.nested.items.push(2, 3)
            d.nested.items.splice(0, 1)
            return null
          }),
        )
        assert.deepStrictEqual((yield* session.snapshot(token))?.value, {
          count: 1,
          nested: { items: [2, 3] },
        })
        assert.throws(() => retained?.nested.items.push(5), /revoked/)
        assert.throws(() => Object.keys(retained ?? {}), /revoked/)
        assert.throws(() => Reflect.deleteProperty(retained ?? {}, 'count'), /revoked/)
      }),
    ),
  )
  it.effect('maps only private synchronous draft validation failures to typed errors', () =>
    provide(
      Effect.gen(function* () {
        const session = yield* Session.Session
        const error = yield* fail(
          session.transaction(
            Effect.fnUntraced(function* (tx) {
              const d = yield* tx.doc(token)
              Reflect.set(d, 'count', undefined)
              return null
            }),
          ),
        )
        assert.strictEqual(error.reason._tag, 'Invalid')
        assert.strictEqual(yield* session.snapshot(token), undefined)
        const exit = yield* session
          .transaction(() => Effect.die(new TypeError('unrelated defect')))
          .pipe(Effect.exit)
        assert.strictEqual(exit._tag, 'Failure')
      }),
    ),
  )
  it.effect('validates postmutation schemas and checkpoint predicates before adoption', () =>
    provide(
      Effect.gen(function* () {
        const session = yield* Session.Session
        const store = yield* Store
        yield* session.transaction((tx) => tx.doc(token).pipe(Effect.as(null)))
        const before = yield* store.read
        yield* fail(
          session.transaction(
            Effect.fnUntraced(function* (tx) {
              const d = yield* tx.doc(token)
              Reflect.set(d, 'count', 'invalid')
              return null
            }),
          ),
        )
        assert.deepStrictEqual(yield* store.read, before)
        const checkpoint = Document.defineUnsafe({
          ...token.definition,
          checkpointWhen: () => {
            throw new Error('checkpoint predicate')
          },
        })
        yield* fail(
          session.transaction(
            Effect.fnUntraced(function* (tx) {
              const d = yield* tx.doc(checkpoint)
              d.count = 2
              return null
            }),
          ),
        )
        assert.deepStrictEqual(yield* store.read, before)
      }),
    ),
  )
  it.effect('persists array length-only mutations and descriptor truncation', () =>
    provide(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.transaction((tx) => tx.doc(token).pipe(Effect.asVoid))
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const d = yield* tx.doc(token)
            d.nested.items.length = 0
          }),
        )
        assert.deepStrictEqual((yield* session.snapshot(token))?.value.nested.items, [])
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const d = yield* tx.doc(token)
            d.nested.items.push(1, 2)
            Object.defineProperty(d.nested.items, 'length', { value: 1 })
          }),
        )
        assert.deepStrictEqual((yield* session.snapshot(token))?.value.nested.items, [1])
      }),
    ),
  )
  it.effect(
    'detaches nested proxy assignments, copied arrays and splice survivors at normal JSON boundaries',
    () =>
      provide(
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const nested = Document.defineUnsafe({
            kind: 'nestedproxies',
            version: 1,
            scope: 'session',
            schema: Schema.Struct({
              items: Schema.Array(Schema.Struct({ label: Schema.String })),
              copied: Schema.Struct({ label: Schema.String }),
            }),
            initial: () => ({
              items: [{ label: 'first' }, { label: 'second' }, { label: 'third' }],
              copied: { label: 'initial' },
            }),
          })
          const entry = yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const d = yield* tx.doc(nested)
              const second = d.items[1]
              assert.ok(second)
              Object.assign(d.copied, second)
              d.items.splice(0, 1)
              const survivor = d.items[0]
              assert.ok(survivor)
              survivor.label = 'changed'
              return yield* tx.appendEntry(root, {
                kind: 'proxy-entry',
                data: { copied: d.copied, items: d.items },
              })
            }),
          )
          assert.deepStrictEqual((yield* session.snapshot(nested))?.value, {
            items: [{ label: 'changed' }, { label: 'third' }],
            copied: { label: 'second' },
          })
          assert.deepStrictEqual((yield* session.entry(entry.id))?.entry.data, {
            items: [{ label: 'changed' }, { label: 'third' }],
            copied: { label: 'second' },
          })
        }),
      ),
  )
  it.effect('tracks data descriptors and keeps prototype-like draft keys as ordinary data', () =>
    provide(
      Effect.gen(function* () {
        const session = yield* Session.Session
        const dynamic = Document.defineUnsafe({
          kind: 'dynamic',
          version: 1,
          scope: 'session',
          schema: Schema.JsonObject,
          initial: (): Record.JsonObject => ({}),
        })
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const d = yield* tx.doc(dynamic)
            assert.strictEqual(Reflect.get(d, '__proto__'), undefined)
            Object.defineProperty(d, 'count', {
              value: 2,
              configurable: true,
              enumerable: true,
              writable: true,
            })
            Reflect.set(d, '__proto__', { safe: true })
            return null
          }),
        )
        const snapshot = yield* session.snapshot(dynamic)
        assert.ok(snapshot)
        assert.strictEqual(snapshot.value.count, 2)
        assert.deepStrictEqual(Reflect.get(snapshot.value, '__proto__'), { safe: true })
        assert.strictEqual(Object.getPrototypeOf(snapshot.value), Object.prototype)
        yield* fail(
          session.transaction(
            Effect.fnUntraced(function* (tx) {
              Object.setPrototypeOf(yield* tx.doc(dynamic), {})
              return null
            }),
          ),
        )
      }),
    ),
  )
  it.effect('deduplicates concurrent acquisition and keeps empty family keys independent', () =>
    provide(
      Effect.gen(function* () {
        const session = yield* Session.Session
        const family = Document.familyUnsafe({ ...token.definition, kind: 'family' })
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const [a, b] = yield* Effect.all(
              [tx.doc(family, { key: '' }), tx.doc(family, { key: '' })],
              { concurrency: 'unbounded' },
            )
            assert.strictEqual(a, b)
            a.count = 5
            const sibling = yield* tx.doc(family, { key: '\ud800' })
            sibling.count = 6
            return null
          }),
        )
        assert.strictEqual((yield* session.snapshot(family, { key: '' }))?.value.count, 5)
        assert.strictEqual((yield* session.snapshot(family, { key: '\ud800' }))?.value.count, 6)
        yield* fail(session.transaction((tx) => tx.doc(family).pipe(Effect.as(null))))
        yield* fail(session.snapshot(token, { key: '' }))
      }),
    ),
  )
  it.effect('memoizes read-only migration without memoizing domain truth or double-migrating', () =>
    provide(
      Effect.gen(function* () {
        const session = yield* Session.Session
        const store = yield* Store
        yield* session.transaction((tx) => tx.doc(token).pipe(Effect.as(null)))
        let calls = 0
        const newer = Document.defineUnsafe({
          ...token.definition,
          version: 2,
          migrate: (value) => {
            calls++
            return { count: Number(value.count) + 1, nested: { items: [1] } }
          },
        })
        yield* session.snapshot(newer)
        yield* session.snapshot(newer)
        assert.strictEqual(calls, 1)
        yield* fail(
          session.transaction(
            Effect.fnUntraced(function* (tx) {
              const d = yield* tx.doc(newer)
              d.count = 100
              return yield* rejected('rollback')
            }),
          ),
        )
        assert.strictEqual((yield* session.snapshot(newer))?.value.count, 1)
        assert.strictEqual(calls, 1)
        assert.strictEqual((yield* store.read).documents[0]?.revisions[0]?.content.version, 1)
        yield* session.transaction((tx) => tx.doc(newer).pipe(Effect.as(null)))
        assert.strictEqual((yield* session.snapshot(newer))?.value.count, 1)
        assert.strictEqual(calls, 1)
      }),
    ),
  )
  it.effect(
    'rejects mutation of fork source documents and current-policy creation in fork transaction',
    () =>
      provide(
        Effect.gen(function* () {
          const session = yield* Session.Session
          yield* session.root()
          const history = Document.defineUnsafe({
            ...token.definition,
            kind: 'history',
            scope: 'conversation',
            history: 'rewindable',
            fork: 'asOf',
          })
          const current = Document.defineUnsafe({
            ...history.definition,
            kind: 'current',
            fork: 'current',
          })
          const entry = yield* session.transaction((tx) => tx.appendEntry(root, { kind: 'cutoff' }))
          yield* session.transaction((tx) => tx.doc(history, { owner: root }).pipe(Effect.as(null)))
          yield* fail(
            session.transaction(
              Effect.fnUntraced(function* (tx) {
                yield* tx.forkConversation(root, entry.id, { ownership: { kind: 'ownerless' } })
                yield* tx.doc(current, { owner: root })
                return null
              }),
            ),
          )
          assert.strictEqual((yield* session.scanConversations({}, 10)).items.length, 1)
        }),
      ),
  )
})
describe('creation initializer', () => {
  it.effect('runs for raw root/create/fork once and rejects atomically on hook failure', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const hookDoc = Document.defineUnsafe({
          ...token.definition,
          kind: 'hook',
          scope: 'conversation',
          history: 'rewindable',
          fork: 'initial',
        })
        let calls = 0
        let rejectNext = false
        const hook = Session.CreationHook.of({
          run: (tx, conversation) =>
            Effect.gen(function* () {
              calls++
              const d = yield* tx.doc(hookDoc, { owner: conversation.id })
              d.count = calls
              if (rejectNext) return yield* rejected('hook failed')
            }),
        })
        const store = yield* Memory.make
        const session = yield* Session.make.pipe(
          Effect.provideService(Store, store),
          Effect.provideService(Session.CreationHook, hook),
        )
        yield* session.root()
        yield* session.root()
        const child = yield* session.transaction((tx) =>
          tx.createConversation({ ownership: { kind: 'ownerless' } }),
        )
        const entry = yield* session.transaction((tx) => tx.appendEntry(root, { kind: 'cutoff' }))
        const fork = yield* session.transaction((tx) =>
          tx.forkConversation(root, entry.id, { ownership: { kind: 'ownerless' } }),
        )
        assert.strictEqual(calls, 3)
        for (const owner of [root, child.id, fork.id])
          assert.ok(yield* session.snapshot(hookDoc, { owner }))
        const before = yield* store.read
        rejectNext = true
        yield* fail(
          session.transaction((tx) => tx.createConversation({ ownership: { kind: 'ownerless' } })),
        )
        assert.deepStrictEqual(yield* store.read, before)
      }),
    ),
  )
  it.effect(
    'hydrates copied asOf documents before a fork creation hook and stages hook mutations',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const copied = Document.defineUnsafe({
            ...token.definition,
            kind: 'copied',
            scope: 'conversation',
            history: 'rewindable',
            fork: 'asOf',
          })
          const hook = Session.CreationHook.of({
            run: (tx, conversation) =>
              Effect.gen(function* () {
                const d = yield* tx.doc(copied, { owner: conversation.id })
                if (conversation.parent !== undefined) d.count++
              }),
          })
          const store = yield* Memory.make
          const session = yield* Session.make.pipe(
            Effect.provideService(Store, store),
            Effect.provideService(Session.CreationHook, hook),
          )
          yield* session.root()
          const cutoff = yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const d = yield* tx.doc(copied, { owner: root })
              d.count = 5
              return yield* tx.appendEntry(root, { kind: 'cutoff' })
            }),
          )
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const d = yield* tx.doc(copied, { owner: root })
              d.count = 9
              return null
            }),
          )
          const fork = yield* session.transaction((tx) =>
            tx.forkConversation(root, cutoff.id, { ownership: { kind: 'ownerless' } }),
          )
          assert.strictEqual((yield* session.snapshot(copied, { owner: fork.id }))?.value.count, 6)
          assert.strictEqual((yield* session.snapshot(copied, { owner: root }))?.value.count, 9)
        }),
      ),
  )
  it.effect('rejects callback settlement while a child Tx operation is still executing', () =>
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const hook = Session.CreationHook.of({
          run: () =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
        })
        const store = yield* Memory.make
        const session = yield* Session.make.pipe(
          Effect.provideService(Store, store),
          Effect.provideService(Session.CreationHook, hook),
        )
        const error = yield* fail(
          session.transaction(
            Effect.fnUntraced(function* (tx) {
              yield* tx
                .createConversation({ ownership: { kind: 'ownerless' } })
                .pipe(Effect.forkScoped)
              yield* Deferred.await(entered)
              return null
            }),
          ),
        )
        assert.strictEqual(error.reason._tag, 'Invalid')
        assert.deepStrictEqual((yield* store.read).conversations, [])
        yield* Deferred.succeed(release, undefined)
      }),
    ),
  )
})
describe('watch lifecycle', () => {
  it.live(
    'retains acquisition value until started, isolates listener error and settles stop during in-flight delivery',
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Memory.make
          const session = yield* Session.make.pipe(Effect.provideService(Store, store))
          yield* session.transaction((tx) => tx.doc(token).pipe(Effect.as(null)))
          const first = yield* session.watchDoc(token)
          const second = yield* session.watchDoc(token)
          assert.ok(first)
          assert.ok(second)
          yield* session.transaction(
            Effect.fnUntraced(function* (tx) {
              const d = yield* tx.doc(token)
              d.count = 1
              return null
            }),
          )
          yield* Effect.sleep('30 millis')
          assert.strictEqual(first.value?.count, 0)
          const error = yield* fail(first.listen(() => new DomainError({ message: 'listener' })))
          assert.ok(error instanceof DomainError)
          assert.strictEqual(yield* first.closed, 'listener_error')
          const entered = yield* Deferred.make<void>()
          const release = yield* Deferred.make<void>()
          const running = yield* second
            .listen(() =>
              Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
            )
            .pipe(Effect.forkScoped)
          yield* Deferred.await(entered)
          yield* second.stop
          assert.strictEqual(yield* second.closed, 'stopped')
          yield* Deferred.succeed(release, undefined)
          yield* Fiber.join(running)
          assert.strictEqual((yield* session.snapshot(token))?.value.count, 1)
        }),
      ),
  )
  it.effect('seals a watch on scope disposal and rejects duplicate consumers', () =>
    provide(
      Effect.gen(function* () {
        const session = yield* Session.Session
        yield* session.transaction((tx) => tx.doc(token).pipe(Effect.as(null)))
        const watch = yield* session.watchDoc(token)
        assert.ok(watch)
        yield* session.transaction(
          Effect.fnUntraced(function* (tx) {
            const d = yield* tx.doc(token)
            d.count = 1
            return null
          }),
        )
        yield* watch.changes.pipe(Stream.take(1), Stream.runCollect)
        assert.strictEqual(
          (yield* fail(watch.changes.pipe(Stream.runCollect))).reason._tag,
          'Invalid',
        )
      }),
    ),
  )
})
