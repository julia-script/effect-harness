import { describe, expect, test } from 'bun:test'
import { replay, track } from './support.ts'

/** Ported from Chord's `test/delta-clone.test.ts`. */

const importedOwnership = () => {
  const input = {
    point: { x: 3, y: 7, pressure: 0.1 },
    rows: [{ values: [0, false, null, 'text', { n: 1 }] }],
  }
  const tracker = track(input)
  expect(tracker.value).toBe(input)
  expect(tracker.value.point).toBe(input.point)
  expect(tracker.value.rows[0]?.values[4]).toBe(input.rows[0]?.values[4])
  expect(Object.isFrozen(tracker.value.rows[0]?.values)).toBe(false)
}

const nullPrototypes = () => {
  interface Dictionary {
    enabled: boolean
    child: { n: number }
  }
  const dictionary: Dictionary = { enabled: true, child: { n: 1 } }
  Object.setPrototypeOf(dictionary, null)
  const tracker = track({
    dictionary,
    left: { nested: [{ n: 1 }] },
    right: { nested: [{ n: 1 }] },
  })
  expect(Object.getPrototypeOf(tracker.value.dictionary)).toBeNull()
  expect(tracker.value.dictionary).toBe(dictionary)
  expect(tracker.value.left).not.toBe(tracker.value.right)
  expect(tracker.value.left.nested[0]).not.toBe(tracker.value.right.nested[0])
}

const untraversedRoot = () => {
  let reads = 0
  const input = Object.defineProperty({}, 'untrustedAccessor', {
    enumerable: true,
    get() {
      reads += 1
      throw new Error('root was traversed')
    },
  })
  const tracker = track(input)
  expect(tracker.value).toBe(input)
  expect(reads).toBe(0)
}

const immediateCopies = () => {
  const initial: { rows: Array<{ nested: { value: number } }> } = { rows: [] }
  const tracker = track(initial)
  const assigned = { nested: { value: 1 } }
  const change = tracker.beginChange()
  change.state.rows.push(assigned)
  assigned.nested.value = 9
  expect(change.state.rows[0]?.nested.value).toBe(1)
  const prepared = change.prepare()
  expect(prepared.value.rows[0]?.nested.value).toBe(1)
  expect(replay(structuredClone(prepared.base), prepared.ops)).toEqual(prepared.value)
}

describe('tracker ownership', () => {
  test('takes immutable ownership of the imported revision in O(1)', importedOwnership)
  test('preserves null prototypes in an alias-free owned root', nullPrototypes)
  test('does not traverse a trusted root while taking ownership', untraversedRoot)
  test('copies assigned and inserted values immediately', immediateCopies)
})
