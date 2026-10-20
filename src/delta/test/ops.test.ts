import { describe, expect, test } from 'bun:test'
import { DeltaError } from '../index.ts'
import { adoptError, replay, track } from './support.ts'
import type { SyncTracker } from './support.ts'
import type { Tracker } from '../index.ts'

/** Ported from the `track` tests of Chord's `test/delta.test.ts`. */

type JsonValue = null | boolean | number | string | Array<JsonValue> | { [key: string]: JsonValue }

const expectRejected = <T extends object>(
  tracker: SyncTracker<T>,
  prepared: Tracker.Prepared<T>,
  reason: DeltaError.AdoptReason,
  message: RegExp,
) => {
  const error = adoptError(tracker, prepared)
  expect(error).toBeInstanceOf(DeltaError.AdoptRejected)
  expect(error.reason).toBe(reason)
  expect(error.message).toMatch(message)
}

const draftAcrossAwait = async () => {
  const input = { count: 1, nested: { text: 'a' }, values: [1] }
  const tracker = track(input)
  expect(tracker.value).toBe(input)
  expect(Object.isFrozen(tracker.value)).toBe(false)
  expect(Object.isFrozen(tracker.value.nested)).toBe(false)

  const change = tracker.beginChange()
  change.state.count = 2
  await Promise.resolve()
  change.state.nested.text += 'b'
  change.state.values.push(2)
  expect(tracker.value).toEqual(input)

  const prepared = change.prepare()
  expect(prepared.base).toBe(tracker.value)
  expect(prepared.value).toEqual({ count: 2, nested: { text: 'ab' }, values: [1, 2] })
  expect(prepared.ops).toEqual([
    ['s', ['count'], 2],
    ['a', ['nested', 'text'], 'b'],
    ['p', ['values'], 1, 0, [2]],
  ])
  // Never-throw: a settled draft reads as empty.
  expect(change.state.count).toBeUndefined()
  expect(tracker.value).toEqual(input)

  tracker.adopt(prepared)
  expect(tracker.value).toBe(prepared.value)
}

const abortRevokes = () => {
  const tracker = track({ child: { value: 1 } })
  const change = tracker.beginChange()
  const { child } = change.state
  child.value = 2
  change.abort()
  expect(tracker.value.child.value).toBe(1)
  expect(child.value).toBeUndefined()
  expect(() => {
    change.abort()
  }).not.toThrow()
  expect(() => change.prepare()).toThrow(/settled/u)
  const next = tracker.beginChange()
  next.abort()
}

const arrayGrowth = () => {
  const initial: { values: Array<number | null> } = { values: [1, 2] }
  const tracker = track(initial)
  const change = tracker.beginChange()
  change.state.values.length = 4
  const prepared = change.prepare()
  expect(prepared.value.values).toEqual([1, 2, null, null])
  expect(change.state.values).toBeUndefined()
  tracker.adopt(prepared)
}

const deepNoOp = () => {
  const tracker = track({ value: { nested: [1, 2] } })
  const change = tracker.beginChange()
  change.state.value = { nested: [1, 2] }
  const prepared = change.prepare()
  expect(prepared.value).toBe(prepared.base)
  expect(prepared.ops).toEqual([])
  tracker.adopt(prepared)
  expect(tracker.value).toBe(prepared.base)
}

const replacementOwnership = () => {
  const tracker = track({ nested: { value: 1 } })
  const replacement = { nested: { value: 2 } }
  const prepared = tracker.prepareReplace(replacement)
  expect(prepared.value).toBe(replacement)
  expect(prepared.ops).toEqual([['r', replacement]])
  tracker.adopt(prepared)

  const noOp = tracker.prepareReplace({ nested: { value: 2 } })
  expect(noOp.value).toBe(tracker.value)
  expect(noOp.ops).toEqual([])
}

const sharedPlacements = () => {
  const initial: { rows: Array<{ id: number }> } = { rows: [] }
  const tracker = track(initial)
  const change = tracker.beginChange()
  change.state.rows.push({ id: 1 })
  const prepared = change.prepare()
  const splice = prepared.ops[0]
  if (splice?.[0] !== 'p') throw new Error('expected splice')
  expect(splice[4][0]).toBe(prepared.value.rows[0])
  expect(Object.isFrozen(prepared)).toBe(false)
  expect(Object.isFrozen(prepared.ops)).toBe(false)
}

const rejectedPreparations = () => {
  const first = track({ value: 0 })
  const second = track({ value: 0 })
  const change = first.beginChange()
  const competing = first.beginChange()
  change.state.value = 1
  competing.state.value = 2
  const prepared = change.prepare()
  const competingPrepared = competing.prepare()
  expectRejected(second, prepared, 'foreign', /different tracker/u)
  first.adopt(prepared)
  expectRejected(first, prepared, 'consumed', /already been used/u)
  expectRejected(first, competingPrepared, 'stale', /stale/u)

  const stale = first.prepareReplace({ value: 2 })
  const winner = first.prepareReplace({ value: 3 })
  first.adopt(winner)
  expectRejected(first, stale, 'stale', /stale/u)
  expectRejected(first, stale, 'stale', /stale/u)
}

const abortedPreparation = () => {
  const tracker = track({ value: 0 })
  const change = tracker.beginChange()
  change.state.value = 1
  const prepared = change.prepare()
  change.abort()
  expectRejected(tracker, prepared, 'aborted', /aborted/u)
  expect(() => {
    change.abort()
  }).not.toThrow()
}

const noOpStaleness = () => {
  const tracker = track({ value: { count: 1 } })
  const first = tracker.prepareReplace({ value: { count: 1 } })
  const competing = tracker.prepareReplace({ value: { count: 1 } })
  expect(first.value).toBe(first.base)
  expect(competing.value).toBe(competing.base)
  tracker.adopt(first)
  expectRejected(tracker, first, 'consumed', /already been used/u)
  expectRejected(tracker, competing, 'stale', /stale/u)
}

const largeReplacement = () => {
  const rows = Array.from({ length: 10_000 }, (_, value) => ({ value, stable: { value } }))
  const tracker = track({ rows })
  const replacement = { rows: tracker.value.rows.slice() }
  const stable = replacement.rows[5000]?.stable ?? { value: 5000 }
  replacement.rows[5000] = { value: -1, stable }
  const prepared = tracker.prepareReplace(replacement)
  expect(prepared.value).toBe(replacement)
  expect(prepared.ops).toEqual([['r', replacement]])
  expect(replay(prepared.base, prepared.ops)).toEqual(prepared.value)
}

const stringWindows = () => {
  const tracker = track({ text: 'abcdefgh' })
  let change = tracker.beginChange()
  change.state.text += 'ij'
  let prepared = change.prepare()
  expect(prepared.ops).toEqual([['a', ['text'], 'ij']])
  tracker.adopt(prepared)

  change = tracker.beginChange()
  change.state.text = `${change.state.text.slice(3)}xyz`
  prepared = change.prepare()
  expect(prepared.ops).toEqual([
    ['t', ['text'], 3],
    ['a', ['text'], 'xyz'],
  ])
}

/** A configurable setter-only accessor that throws `message`. */
const throwingSetter = (message: string): PropertyDescriptor => ({
  // A getter reading undefined, as an absent one would, so the accessor is a pair.
  get: () => {},
  set: () => {
    throw new Error(message)
  },
  configurable: true,
})

const importedSetters = () => {
  const root: Record<string, JsonValue> = {}
  Object.defineProperty(root, 'trap', {
    value: 1,
    writable: true,
    enumerable: true,
    configurable: true,
  })
  expect(
    Reflect.defineProperty(Object.prototype, 'trap', throwingSetter('inherited setter ran')),
  ).toBe(true)
  try {
    expect(() => track(root)).not.toThrow()
  } finally {
    Reflect.deleteProperty(Object.prototype, 'trap')
  }

  let value: { values: Array<number> } | undefined
  expect(
    Reflect.defineProperty(Array.prototype, '0', throwingSetter('inherited array setter ran')),
  ).toBe(true)
  try {
    value = track({ values: [1, 2] }).value
  } finally {
    Reflect.deleteProperty(Array.prototype, '0')
  }
  expect(value).toEqual({ values: [1, 2] })
}

describe('immutable tracker lifecycle', () => {
  test('keeps a draft alive across await and adopts only a prepared change', draftAcrossAwait)
  test('aborts and revokes without changing the committed value', abortRevokes)
  test('grows arrays with explicit nulls and revokes drafts after preparation', arrayGrowth)
  test('normalizes a deep no-op to exact previous identity', deepNoOp)
  test(
    'takes immutable ownership of replacement input and applies no-op normalization',
    replacementOwnership,
  )
  test('shares immutable operation placements with the prepared candidate', sharedPlacements)
  test(
    'rejects foreign, stale, and repeated preparations while allowing competing changes',
    rejectedPreparations,
  )
  test('invalidates a prepared result when its change is aborted', abortedPreparation)
  test('makes competing same-base preparations stale after adopting a no-op', noOpStaleness)
  test(
    'emits an owned root replacement without traversing large replacement input',
    largeReplacement,
  )
})

describe('canonical strings', () => {
  test('emits append and rolling-window operations', stringWindows)
})

describe('path and prototype safety', () => {
  test('imports own properties without invoking inherited setters', importedSetters)
})
