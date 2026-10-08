import { assertFailure, assertSuccess } from '@effect/vitest/utils'

import * as SchemaIssue from 'effect/SchemaIssue'

import { assert, describe, it } from '@effect/vitest'

import * as Effect from 'effect/Effect'

import * as Schema from 'effect/Schema'

import * as Result from 'effect/Result'

import * as Store from 'effect-harness/durable/Store'

import { StrictReceiptJson } from 'effect-harness/durable/storage/StrictReceiptJson'

describe('StrictReceiptJson', () => {
  it('rejects descriptors without invoking getters and accepts own __proto__ and shared acyclic values', () => {
    let reads = 0
    const getter = {
      get value() {
        reads++
        throw new Error('getter ran')
      },
    }
    assertFailure(
      Result.mapError(Schema.decodeResult(StrictReceiptJson)(getter), (error) => ({
        _tag: error._tag,
        message: error.message,
      })),
      { _tag: 'SchemaError', message: 'Receipt results require enumerable JSON data properties' },
    )
    assert.strictEqual(reads, 0)
    const trapped = new Proxy(
      { value: 1 },
      {
        get: () => {
          reads++
          throw new Error('get trap ran')
        },
      },
    )
    assertSuccess(Schema.decodeResult(StrictReceiptJson)(trapped), { value: 1 })
    assert.strictEqual(reads, 0)
    const own = Object.create(null)
    Object.defineProperty(own, '__proto__', { value: { accepted: true }, enumerable: true })
    const decoded = Schema.decodeResult(StrictReceiptJson)(own)
    assertSuccess(decoded, { ['__proto__']: { accepted: true } })
    const shared = { value: 1 }
    assertSuccess(Schema.decodeResult(StrictReceiptJson)({ a: shared, b: shared }), {
      a: { value: 1 },
      b: { value: 1 },
    })
    const extra = [1]
    Object.defineProperty(extra, 'extra', { value: 'not JSON array data', enumerable: true })
    assertFailure(
      Result.mapError(Schema.decodeResult(StrictReceiptJson)(extra), (error) => ({
        _tag: error._tag,
        message: error.message,
      })),
      { _tag: 'SchemaError', message: 'Receipt arrays cannot have named properties' },
    )
  })

  it('receipt reflection errors obey reportInput without retaining thrown cause inputs', () => {
    const input = new Proxy(
      { secret: 'receipt-secret' },
      {
        ownKeys: () => {
          throw new Error('secret-reflection-cause')
        },
      },
    )
    for (const options of [undefined, { reportInput: false }]) {
      const result = Schema.decodeResult(StrictReceiptJson)(input, options)
      assertFailure(
        Result.mapError(result, (failure) => ({
          _tag: failure._tag,
          message: failure.message,
          retainedInput: JSON.stringify(failure).includes('receipt-secret'),
          retainedCause: JSON.stringify(failure).includes('secret-reflection-cause'),
        })),
        {
          _tag: 'SchemaError',
          message: 'Cannot inspect receipt JSON',
          retainedInput: false,
          retainedCause: false,
        },
      )
    }
    const reported = Schema.decodeResult(StrictReceiptJson)(input, { reportInput: true })
    const hasReportedInput = (issue: SchemaIssue.Issue): boolean => {
      if (SchemaIssue.hasInput(issue) && issue.input === input) return true
      if ('issue' in issue) return hasReportedInput(issue.issue)
      if ('issues' in issue) return issue.issues.some(hasReportedInput)
      return false
    }
    assertFailure(
      Result.mapError(reported, (failure) => ({
        _tag: failure._tag,
        message: failure.message,
        retainedInput: hasReportedInput(failure.issue),
      })),
      { _tag: 'SchemaError', message: 'Cannot inspect receipt JSON', retainedInput: true },
    )
  })

  it('rejects holes, symbols, nonenumerable fields, classes, cycles and throwing reflection as typed schema issues', () => {
    const cycle: { self?: unknown } = {}
    cycle.self = cycle
    const symbol = { [Symbol('hidden')]: 1 }
    const hidden = Object.defineProperty({}, 'hidden', { value: 1 })
    const broken = [
      new Proxy(
        {},
        {
          getPrototypeOf: () => {
            throw new Error('prototype')
          },
        },
      ),
      new Proxy(
        {},
        {
          ownKeys: () => {
            throw new Error('keys')
          },
        },
      ),
      new Proxy(
        { value: 1 },
        {
          getOwnPropertyDescriptor: () => {
            throw new Error('descriptor')
          },
        },
      ),
      Array(2),
      symbol,
      hidden,
      new Date(),
      cycle,
    ]
    const messages = [
      'Cannot inspect receipt JSON',
      'Cannot inspect receipt JSON',
      'Cannot inspect receipt JSON',
      'Receipt arrays cannot have holes',
      'Receipt results require enumerable JSON data properties',
      'Receipt results require enumerable JSON data properties',
      'Receipt results cannot contain service or class instances',
      'Receipt results cannot be cyclic',
    ]
    for (const [index, input] of broken.entries()) {
      const message = messages[index]
      assert.ok(message)
      assertFailure(
        Result.mapError(Schema.decodeResult(StrictReceiptJson)(input), (error) => ({
          _tag: error._tag,
          message: error.message,
        })),
        { _tag: 'SchemaError', message },
      )
    }
  })

  it.effect('rejects a reflected receipt before saving and leaves admission state intact', () =>
    Effect.gen(function* () {
      const store = yield* Store.makeMemory
      const before = yield* store.read
      const result = new Proxy(
        { value: 1 },
        {
          ownKeys: () => {
            throw new Error('reflect failure')
          },
        },
      )
      const failure = yield* store
        .transact((state) => Effect.succeed(Store.makeCandidate({ state, writes: [], result })), {
          key: 'reflection',
        })
        .pipe(Effect.flip)
      assert.strictEqual(failure.reason._tag, 'InvalidError')
      assert.strictEqual(failure.certainty, 'rejected')
      assert.deepStrictEqual(yield* store.read, before)
    }),
  )
})
