import { describe, expect, test } from 'bun:test'
import { cast } from 'effect/Function'
import { DeltaError } from '../index.ts'
import type { Tracker } from '../index.ts'
import { prepareError, replay, track } from './support.ts'

/** Ported from Chord's `test/state-draft.test.ts`. */

type JsonValue = null | boolean | number | string | Array<JsonValue> | { [key: string]: JsonValue }

/** A value the draft must reject, typed as JSON to reach the trap. */
const notJson = (value: unknown): JsonValue => cast(value)

/** `undefined`, read from a missing key. */
const absent: unknown = Reflect.get({}, 'absent')

const changedBranches = () => {
  const tracker = track({
    changed: { count: 1, sibling: { value: 'kept' } },
    untouched: { value: 2 },
  })
  const base = tracker.value
  const change = tracker.beginChange()
  const first = change.state.changed
  expect(change.state.changed).toBe(first)
  change.state.changed.count = 3
  const prepared = change.prepare()
  expect(prepared.value).not.toBe(base)
  expect(prepared.value.changed).not.toBe(base.changed)
  expect(prepared.value.changed.sibling).toBe(base.changed.sibling)
  expect(prepared.value.untouched).toBe(base.untouched)
}

const nativeMutators = () => {
  const initial: { optional?: string; values: Array<number> } = {
    optional: 'remove',
    values: [3, 1, 2],
  }
  const tracker = track(initial)
  const change = tracker.beginChange()
  delete change.state.optional
  change.state.values.push(4)
  expect(change.state.values.pop()).toBe(4)
  change.state.values.unshift(0)
  expect(change.state.values.shift()).toBe(0)
  expect(change.state.values.splice(1, 1, 5, 4)).toEqual([1])
  change.state.values.sort((left, right) => left - right)
  change.state.values.reverse()
  change.state.values.fill(9, 1, 3)
  change.state.values.copyWithin(1, 0, 2)
  const prepared = change.prepare()
  expect(prepared.value).toEqual({ values: [5, 5, 9, 2] })
}

const insertedDraftValues = () => {
  const tracker = track({ values: [{ value: 1 }, { value: 2 }] })
  const change = tracker.beginChange()
  const held = change.state.values[0]
  if (held === undefined) throw new Error('expected a held element')
  change.state.values.unshift(held)
  held.value = 9
  const prepared = change.prepare()
  expect(prepared.value.values).toEqual([{ value: 1 }, { value: 9 }, { value: 2 }])
  expect(prepared.value.values[0]).not.toBe(prepared.value.values[1])
}

const comparatorEdits = () => {
  const tracker = track({
    rows: [
      { rank: 2, comparisons: 0 },
      { rank: 1, comparisons: 0 },
    ],
  })
  const change = tracker.beginChange()
  change.state.rows.sort((left, right) => {
    left.comparisons++
    right.comparisons++
    return left.rank - right.rank
  })
  const prepared = change.prepare()
  expect(prepared.value.rows.map((row) => row.rank)).toEqual([1, 2])
  expect(prepared.value.rows.map((row) => row.comparisons)).toEqual([1, 1])
}

const detachedReferences = () => {
  const initial: { child?: { value: string }; items: Array<{ value: string }> } = {
    child: { value: 'removed' },
    items: [{ value: 'removed' }, { value: 'kept' }],
  }
  const tracker = track(initial)
  const change = tracker.beginChange()
  const { child } = change.state
  const shifted = change.state.items.shift()
  delete change.state.child
  if (child !== undefined) child.value = 'detached child'
  if (shifted !== undefined) shifted.value = 'detached item'
  expect(change.prepare().value).toEqual({ items: [{ value: 'kept' }] })
}

const largeInsertion = (method: 'unshift' | 'splice') => () => {
  const tracker = track({ values: [-1] })
  const change = tracker.beginChange()
  const items = Array.from({ length: 100_000 }, (_, value) => value)
  const values: Tracker.Draft<Array<number>> = change.state.values
  if (method === 'unshift') Reflect.apply(values.unshift, values, items)
  else Reflect.apply(values.splice, values, [1, 0, ...items])
  const prepared = change.prepare()
  const offset = method === 'unshift' ? 0 : 1
  expect(prepared.value.values).toHaveLength(100_001)
  expect(prepared.value.values[offset]).toBe(0)
  expect(prepared.value.values[99_999 + offset]).toBe(99_999)
  expect(replay(prepared.base, prepared.ops)).toEqual(prepared.value)
}

interface Placements {
  number: number
  payload: JsonValue
  values: Array<JsonValue>
}

/** Chord's four rejected placements. One rejected write dooms its change, so each gets its own. */
const invalidPlacements: ReadonlyArray<
  readonly [RegExp, (draft: Tracker.Draft<Placements>) => void]
> = [
  [
    /strict JSON/u,
    (draft) => {
      draft.values[0] = notJson(absent)
    },
  ],
  [
    /strict JSON/u,
    (draft) => {
      draft.values.push(notJson({ nested: absent }))
    },
  ],
  [
    /strict JSON/u,
    (draft) => {
      draft.number = Number.NaN
    },
  ],
  [
    /plain objects/u,
    (draft) => {
      draft.payload = notJson(new Date())
    },
  ],
]

const nonStrictPlacements = () => {
  const initial: Placements = { number: 0, payload: null, values: [1, 2] }
  const tracker = track(initial)
  for (const [message, write] of invalidPlacements) {
    const change = tracker.beginChange()
    write(change.state)
    expect(change.state).toEqual(tracker.value)
    const error = prepareError(change)
    expect(error).toBeInstanceOf(DeltaError.InvalidWrite)
    expect(error.message).toMatch(message)
  }
  expect(tracker.value).toEqual({ number: 0, payload: null, values: [1, 2] })
}

const arrayHoles = () => {
  const tracker = track({ values: [1, 2] })
  const change = tracker.beginChange()
  // Strict-mode `delete` throws exactly when the trap reports false.
  expect(Reflect.deleteProperty(change.state.values, 0)).toBe(true)
  expect(change.state.values).toEqual([1, 2])
  const error = prepareError(change)
  expect(error).toBeInstanceOf(DeltaError.InvalidWrite)
  expect(error.message).toMatch(/dense|holes/u)
  expect(tracker.value.values).toEqual([1, 2])
}

describe('transactional overlay drafts', () => {
  test('copies only changed branches', changedBranches)
  test('supports frozen bases, deletion, and native array mutators', nativeMutators)
  test(
    'copies inserted draft values without aliasing their original reference',
    insertedDraftValues,
  )
  test('keeps comparator edits when sorting object drafts', comparatorEdits)
  test('drops writes through detached child references', detachedReferences)
  for (const method of ['unshift', 'splice'] as const) {
    test(`inserts 100,000 items with ${method} without argument overflow`, largeInsertion(method))
  }
  test('rejects non-strict JSON placements without mutating the draft or base', nonStrictPlacements)
  test('rejects array holes without mutating the committed base', arrayHoles)
})
