/* oxlint-disable max-lines-per-function -- ported describe blocks keep Chord's structure */
import { describe, expect, test } from 'bun:test'
import type { Schema } from 'effect'
import { cast } from 'effect/Function'
import { DeltaError } from '../index.ts'
import type { Tracker } from '../index.ts'
import {
  clone,
  expectAliasFree,
  isProxy,
  prepareError,
  replay,
  track,
  type SyncTracker,
} from './support.ts'

/** Ported from Chord's `test/delta-tracker/tracker.test.ts`. */

type JsonValue = Schema.Json

describe('delta tracker by-value placements', () => {
  test('clones property, index, push, unshift, splice, fill, and copyWithin placements', () => {
    const property = null as { value: number } | null
    const tracker = track({
      property,
      values: [{ value: 0 }, { value: 1 }, { value: 2 }],
    })
    const external = { value: 5 }
    const change = tracker.beginChange()
    change.state.property = external
    change.state.values[0] = external
    change.state.values.push(external)
    change.state.values.unshift(external)
    change.state.values.splice(2, 0, external)
    external.value = 99
    change.state.values.fill(change.state.values[0], 1, 3)
    change.state.values.copyWithin(3, 0, 2)
    const prepared = change.prepare()
    const candidate = clone(prepared.value)
    expect(candidate.property).toEqual({ value: 5 })
    expect(candidate.values.slice(0, 5)).toEqual([
      { value: 5 },
      { value: 5 },
      { value: 5 },
      { value: 5 },
      { value: 5 },
    ])
    tracker.adopt(prepared)
  })

  test('rejects non-strict JSON placements before changing the draft', () => {
    interface Shape {
      optional?: string | undefined
      payload: JsonValue
      number: number
      values: Array<JsonValue>
    }
    const initial: Shape = {
      optional: 'remove',
      payload: null,
      number: 0,
      values: [1, 2],
    }
    const tracker = track(initial)
    const undefinedValue = cast<unknown, JsonValue>(Reflect.get({}, 'absent'))

    // Each rejected write dooms its change, so every invalid write gets its own change.
    const rejects = (mutate: (draft: Tracker.Draft<Shape>) => void, message: RegExp): void => {
      const change = tracker.beginChange()
      mutate(change.state)
      expect(change.state).toEqual(initial)
      const error = prepareError(change)
      expect(error).toBeInstanceOf(DeltaError.InvalidWrite)
      expect(error.message).toMatch(message)
      change.abort()
    }

    rejects((draft) => {
      draft.values[0] = undefinedValue
    }, /strict JSON/u)
    rejects((draft) => {
      draft.values.push({ staged: true }, undefinedValue)
    }, /strict JSON/u)
    rejects((draft) => {
      draft.values.unshift(undefinedValue)
    }, /strict JSON/u)
    rejects((draft) => {
      draft.values.splice(1, 0, undefinedValue)
    }, /strict JSON/u)
    rejects((draft) => {
      draft.values.fill(undefinedValue)
    }, /strict JSON/u)
    rejects((draft) => {
      draft.payload = cast<object, JsonValue>({ nested: undefined })
    }, /strict JSON/u)
    rejects((draft) => {
      draft.payload = cast<object, JsonValue>({ nested: () => 1 })
    }, /strict JSON/u)
    rejects((draft) => {
      draft.payload = cast<object, JsonValue>({ nested: Symbol('invalid') })
    }, /strict JSON/u)
    rejects((draft) => {
      draft.payload = cast<object, JsonValue>({ nested: 1n })
    }, /strict JSON/u)
    rejects((draft) => {
      draft.number = Number.NaN
    }, /strict JSON/u)
    rejects((draft) => {
      draft.number = Number.POSITIVE_INFINITY
    }, /strict JSON/u)
    rejects((draft) => {
      draft.payload = cast<Date, JsonValue>(new Date())
    }, /plain objects/u)

    // oxlint-disable-next-line unicorn/no-new-array -- a hole is the point
    const sparse: Array<JsonValue> = new Array<JsonValue>(1)
    rejects((draft) => {
      draft.payload = sparse
    }, /dense plain arrays/u)
    const cyclic: { self?: unknown } = {}
    cyclic.self = cyclic
    rejects((draft) => {
      draft.payload = cast<object, JsonValue>(cyclic)
    }, /cycles/u)
    let accessorReads = 0
    const accessor = Object.defineProperty({}, 'value', {
      enumerable: true,
      get() {
        accessorReads += 1
        return 1
      },
    })
    rejects((draft) => {
      draft.payload = cast<object, JsonValue>(accessor)
    }, /data properties/u)
    expect(accessorReads).toBe(0)

    const change = tracker.beginChange()
    expect(change.state).toEqual(initial)
    change.state.optional = undefined
    const dictionary: Record<string, JsonValue> = cast(Object.create(null))
    dictionary['valid'] = true
    change.state.payload = dictionary
    change.state.values.push({ valid: true })
    const prepared = change.prepare()
    expect(prepared.value).toEqual({
      payload: { valid: true },
      number: 0,
      values: [1, 2, { valid: true }],
    })
    expect(Object.getPrototypeOf(prepared.value.payload)).toBeNull()
    expect(replay(initial, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('expands repeated source aliases into independent placements', () => {
    interface Row {
      nested: { value: number }
    }
    const left = null as Row | null
    const right = null as Row | null
    const rows: Array<Row> = []
    const tracker = track({ left, right, rows })
    const shared = { nested: { value: 1 } }
    const change = tracker.beginChange()
    change.state.left = shared
    change.state.right = shared
    change.state.rows.push(shared, shared)
    change.state.left.nested.value = 9
    change.state.rows[0]!.nested.value = 8
    const prepared = change.prepare()
    expect(prepared.value).toEqual({
      left: { nested: { value: 9 } },
      right: { nested: { value: 1 } },
      rows: [{ nested: { value: 8 } }, { nested: { value: 1 } }],
    })
    expect(prepared.value.left).not.toBe(prepared.value.right)
    expect(prepared.value.rows[0]).not.toBe(prepared.value.rows[1])
    expect(replay(tracker.value, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('deep-clones draft-sourced placements within and across revisions', () => {
    const b = null as { child: { value: number } } | null
    const tracker = track({
      a: { child: { value: 1 } },
      b,
      rows: [{ child: { value: 1 } }, { child: { value: 2 } }],
    })
    const first = tracker.beginChange()
    first.state.b = first.state.a
    first.state.b.child.value = 3
    first.state.rows[1] = first.state.rows[0]!
    const firstPrepared = first.prepare()
    expect(firstPrepared.value.a.child.value).toBe(1)
    expect(firstPrepared.value.b?.child.value).toBe(3)
    expect(firstPrepared.value.a).not.toBe(firstPrepared.value.b)
    expect(firstPrepared.value.a.child).not.toBe(firstPrepared.value.b?.child)
    expect(firstPrepared.value.rows[0]).not.toBe(firstPrepared.value.rows[1])
    expect(firstPrepared.value.rows[0]!.child).not.toBe(firstPrepared.value.rows[1]!.child)
    expectAliasFree(firstPrepared.value)
    tracker.adopt(firstPrepared)

    const base = clone(tracker.value)
    const second = tracker.beginChange()
    second.state.rows[1]!.child.value = 4
    const secondPrepared = second.prepare()
    expect(secondPrepared.value.rows.map((row) => row.child.value)).toEqual([1, 4])
    expect(replay(base, secondPrepared.ops)).toEqual(secondPrepared.value)
    expectAliasFree(secondPrepared.value)
    tracker.adopt(secondPrepared)
  })

  test('distinguishes raw committed references from draft references', () => {
    const rawCopy = null as { value: number } | null
    const draftCopy = null as { value: number } | null
    const tracker = track({ source: { value: 1 }, rawCopy, draftCopy })
    const raw = tracker.value.source
    const change = tracker.beginChange()
    change.state.source.value = 2
    change.state.rawCopy = raw
    change.state.draftCopy = change.state.source
    const prepared = change.prepare()
    expect(prepared.value).toEqual({
      source: { value: 2 },
      rawCopy: { value: 1 },
      draftCopy: { value: 2 },
    })
    tracker.adopt(prepared)
  })

  test('folds edits to introduced object and array subtrees into placement payloads', () => {
    const nested = null as { rows: Array<{ value: number }> } | null
    const rows: Array<{ values: Array<number> }> = []
    const tracker = track({ nested, rows })
    const change = tracker.beginChange()
    change.state.nested = { rows: [{ value: 1 }] }
    change.state.nested.rows[0]!.value = 2
    change.state.nested.rows.push({ value: 3 })
    change.state.rows.push({ values: [1] })
    change.state.rows[0]!.values.push(2)
    const prepared = change.prepare()
    expect(prepared.ops).toEqual([
      ['s', ['nested'], { rows: [{ value: 2 }, { value: 3 }] }],
      ['p', ['rows'], 0, 0, [{ values: [1, 2] }]],
    ])
    expect(replay(tracker.value, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('folds pathological operation counts without payload-cost estimation', () => {
    const wide: Record<string, number> = Object.fromEntries(
      Array.from({ length: 5000 }, (_, index) => [`field${index}`, 0]),
    )
    const wideTracker: SyncTracker<Record<string, number>> = track(wide)
    const wideBase = clone(wideTracker.value)
    const wideChange = wideTracker.beginChange()
    for (let index = 0; index < 5000; index++) wideChange.state[`field${index}`] = 1
    const widePrepared = wideChange.prepare()
    expect(widePrepared.ops).toHaveLength(1)
    expect(widePrepared.ops[0]?.[0]).toBe('r')
    expect(replay(wideBase, widePrepared.ops)).toEqual(widePrepared.value)
    wideTracker.adopt(widePrepared)

    const sparseTracker = track({ rows: Array.from({ length: 15_000 }, (_, value) => ({ value })) })
    const sparseBase = clone(sparseTracker.value)
    const sparseChange = sparseTracker.beginChange()
    for (let index = 0; index < 15_000; index += 3) {
      sparseChange.state.rows[index]!.value = -index - 1
    }
    const sparsePrepared = sparseChange.prepare()
    expect(sparsePrepared.ops).toHaveLength(1)
    expect(sparsePrepared.ops[0]?.[0]).toBe('r')
    expect(replay(sparseBase, sparsePrepared.ops)).toEqual(sparsePrepared.value)
    sparseTracker.adopt(sparsePrepared)
  }, 30_000)

  test('shares immutable operation placements with the materialized candidate', () => {
    const rows: Array<{ value: number }> = []
    const tracker = track({ rows })
    const change = tracker.beginChange()
    change.state.rows.push({ value: 1 })
    const prepared = change.prepare()
    const operations = prepared.ops
    const pending: Array<unknown> = [...operations]
    while (pending.length > 0) {
      const value = pending.pop()
      if (value === null || typeof value !== 'object') continue
      expect(isProxy(value)).toBe(false)
      const items: Array<unknown> = Object.values(value)
      pending.push(...items)
    }
    const splice = operations[0]
    if (splice?.[0] !== 'p') throw new Error('expected splice')
    expect(splice[4][0]).toBe(prepared.value.rows[0])
    tracker.adopt(prepared)
    expect(tracker.value).toBe(prepared.value)
  })
})
