// effect-review-allow P8-tests-import-public-specifiers: these adversarial tests exercise private storage validation seams that intentionally have no public package export; all public behavior uses package specifiers.
import { assert, describe, it } from '@effect/vitest'
import * as Effect from 'effect/Effect'
import * as Cause from 'effect/Cause'
import * as Exit from 'effect/Exit'
import * as Result from 'effect/Result'
import * as Schema from 'effect/Schema'
import * as Document from 'effect-harness/durable/Document'
import * as Record from 'effect-harness/durable/Record'
import * as State from '../../src/durable/storage/internal/state.ts'
import * as View from 'effect-harness/durable/View'
import * as Session from 'effect-harness/durable/Session'
import * as Memory from 'effect-harness/durable/storage/Memory'
import { sessionLayer } from 'effect-harness/durable/testing/Storage'

const definition: Document.DefinitionInput<{ count: number }> = {
  kind: 'counter',
  version: 1,
  scope: 'session',
  schema: Schema.Struct({ count: Schema.Finite }),
  initial: () => ({ count: 0 }),
}
const primitiveId = Schema.decodeSync(Record.TaskId)(2)
const view: View.Value = {
  conversation: { id: Record.ROOT_CONVERSATION_ID },
  entries: [],
  docs: {},
}
describe('SafeApi', () => {
  it.effect(
    'preserves proxy clone fallback, primitive brands, cycles and owned prototype keys',
    () =>
      Effect.sync(() => {
        const source: {
          readonly __proto__: { readonly retained: boolean }
          readonly nested: number[]
        } = {
          ['__proto__']: { retained: true },
          nested: [1, 2],
        }
        const clone = Result.getOrThrow(State.detached(new Proxy(source, {})))
        assert.notStrictEqual(clone, source)
        assert.notStrictEqual(clone.nested, source.nested)
        assert.strictEqual(Object.hasOwn(clone, '__proto__'), true)
        assert.deepStrictEqual(clone.__proto__, { retained: true })
        assert.strictEqual(Object.getPrototypeOf(clone), Object.prototype)
        const cycle: { self?: object } = {}
        const cycleProxy = new Proxy(cycle, {})
        cycle.self = cycleProxy
        const cyclic = Result.getOrThrow(State.detached(cycleProxy))
        assert.strictEqual(cyclic.self, cyclic)
        const copied: Document.Draft<Record.TaskId> = Result.getOrThrow(Document.copy(primitiveId))
        assert.strictEqual(copied, primitiveId)
        const applied = Result.getOrThrow(
          View.apply(view, [['set', ['docs', '__proto__'], { retained: true }]]),
        )
        assert.strictEqual(Object.hasOwn(applied.docs, '__proto__'), true)
        assert.strictEqual(Object.getPrototypeOf(applied.docs), Object.prototype)
        assert.strictEqual(Object.hasOwn(view.docs, '__proto__'), false)
      }),
  )

  it.effect('returns typed definition failures instead of throwing expected input errors', () =>
    Effect.sync(() => {
      const singleton = Document.define({ ...definition, kind: '' })
      assert.strictEqual(Result.isFailure(singleton), true)
      const family = Document.family({
        ...definition,
        scope: 'conversation',
        history: 'latest',
        fork: 'asOf',
      })
      assert.strictEqual(Result.isFailure(family), true)
      const entry = Record.defineEntry('', Record.Entry)
      assert.strictEqual(Result.isFailure(entry), true)
    }),
  )
  it.effect('returns typed replay failures for paths, deletions and splice targets', () =>
    Effect.sync(() => {
      for (const op of [
        ['set', ['docs', 'absent', 'field'], 1],
        ['delete', []],
        ['splice', ['conversation'], 0, 1, []],
      ] as unknown as View.Op[])
        assert.strictEqual(Result.isFailure(View.apply(view, [op])), true)
    }),
  )
  it.effect('wraps native clone and fallback getter failures with the actual cause', () =>
    Effect.sync(() => {
      const cause = new Error('getter failed')
      const native = {
        get value(): number {
          throw cause
        },
      }
      const proxy = new Proxy(native, {})
      for (const input of [native, proxy]) {
        const result = State.detached(input)
        assert.strictEqual(Result.isFailure(result), true)
        if (Result.isFailure(result)) assert.strictEqual(result.failure.cause, cause)
      }
      const copy = Document.copy(proxy)
      assert.strictEqual(Result.isFailure(copy), true)
    }),
  )
  it.effect('translates only the private draft sentinel and leaves genuine defects intact', () =>
    Effect.gen(function* () {
      const session = yield* Session.Session
      const token = Document.defineUnsafe(definition)
      const foreign = new Error('foreign getter failure')
      const bad = new Proxy(
        {},
        {
          ownKeys() {
            throw foreign
          },
        },
      )
      for (const operation of [
        (draft: Document.Draft<{ count: number }>) => Reflect.set(draft, 'count', bad),
        (draft: Document.Draft<{ count: number }>) =>
          Object.defineProperty(draft, 'count', {
            value: bad,
            enumerable: true,
            writable: true,
            configurable: true,
          }),
      ]) {
        const error = yield* session
          .transaction((tx) =>
            Effect.gen(function* () {
              const draft = yield* tx.doc(token)
              operation(draft)
            }),
          )
          .pipe(Effect.flip)
        assert.strictEqual(error.reason._tag, 'Invalid')
        assert.strictEqual(error.cause, foreign)
      }
      const invalid = yield* session
        .transaction((tx) =>
          Effect.gen(function* () {
            const draft = yield* tx.doc(token)
            Reflect.set(draft, 'count', 1n)
          }),
        )
        .pipe(Effect.flip)
      assert.strictEqual(invalid.reason._tag, 'Invalid')
      assert.ok(invalid.cause instanceof Schema.SchemaError)
      const defect = new Error('genuine callback defect')
      const exit = yield* session.transaction(() => Effect.die(defect)).pipe(Effect.exit)
      assert.ok(Exit.isFailure(exit))
      assert.strictEqual(Result.getOrThrow(Cause.findDefect(exit.cause)), defect)
      assert.strictEqual((yield* session.committed).nextId, 2)
    }).pipe(Effect.provide(sessionLayer(Memory.layer))),
  )
})
