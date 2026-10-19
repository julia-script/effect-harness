import { describe, expect, test } from 'bun:test'
import { cast } from 'effect/Function'
import { replay, settle, track } from './support.ts'

/** Ported from Chord's `test/delta-tracker/tracker.test.ts`. */

type JsonValue = null | boolean | number | string | Array<JsonValue> | { [key: string]: JsonValue }

const throwingSetter = (): void => {
  throw new Error('inherited setter ran')
}

const inheritedSetters = () => {
  const root: Record<string, JsonValue> = {}
  const tracker = track(root)
  expect(
    Reflect.defineProperty(Object.prototype, 'trap', {
      // A getter reading undefined, as an absent one would, so the accessor is a pair.
      get: () => {},
      set: throwingSetter,
      configurable: true,
    }),
  ).toBe(true)
  try {
    settle(tracker, (draft) => {
      draft['trap'] = 1
    })
    expect(tracker.value).toEqual({ trap: 1 })
  } finally {
    Reflect.deleteProperty(Object.prototype, 'trap')
  }
}

const reservedKeys = () => {
  const initial: { safe: { ['__proto__']: { value: number } } } = cast(
    JSON.parse('{"safe":{"__proto__":{"value":1}}}'),
  )
  const tracker = track(initial)
  const change = tracker.beginChange()
  change.state.safe['__proto__'].value = 2
  const prepared = change.prepare()
  expect(prepared.value.safe['__proto__'].value).toBe(2)
  expect(replay(tracker.value, prepared.ops)).toEqual(prepared.value)
  tracker.adopt(prepared)
  expect(Reflect.get({}, 'value')).toBeUndefined()
}

/** A MemoryStorage-style recursive clone that defines each own property. */
const storageClone = (value: unknown): unknown => {
  if (value === null || typeof value !== 'object') return value
  if (Array.isArray(value)) return value.map((item) => storageClone(item))
  const output: Record<string, unknown> = {}
  for (const key of Object.keys(value)) {
    Object.defineProperty(output, key, {
      value: storageClone(Reflect.get(value, key)),
      writable: true,
      enumerable: true,
      configurable: true,
    })
  }
  return output
}

const recursiveClone = () => {
  const tracker = track({ rows: [{ value: 1 }] })
  const change = tracker.beginChange()
  const first = change.state.rows[0]
  if (first !== undefined) first.value = 2
  change.state.rows.push({ value: 3 })
  const prepared = change.prepare()
  expect(storageClone(prepared.value)).toEqual({ rows: [{ value: 2 }, { value: 3 }] })
  tracker.adopt(prepared)
}

describe('delta tracker security and storage-style cloning', () => {
  test('defines own properties without invoking inherited setters', inheritedSetters)
  test('handles reserved own keys without prototype pollution', reservedKeys)
  test('supports a MemoryStorage-style recursive clone of Prepared.value', recursiveClone)
})
