/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks keep Chord's structure */
import { describe, expect, test } from 'bun:test'
import { cast } from 'effect/Function'
import { DeltaError } from '../index.ts'
import { clone, prepareError, replay, settle, track } from './support.ts'

/** Ported from Chord's `test/delta-tracker/tracker.test.ts`. */

/** An index argument whose coercion runs `effect` first, as a native `valueOf` would. */
const coercing = (effect: () => void, index: number): number =>
  cast({
    valueOf() {
      effect()
      return index
    },
  })

/** Run `mutate` on a native array and through a draft, and expect the same result. */
const compare = (mutate: (values: Array<number>) => void): void => {
  const native = [1, 2, 3]
  mutate(native)
  const tracker = track({ values: [1, 2, 3] })
  settle(tracker, (draft) => {
    mutate(draft.values)
  })
  expect(tracker.value.values).toEqual(native)
}

describe('delta tracker policy view', () => {
  test('supports native reads, descriptors, keys, iteration, map, and JSON', () => {
    const object: { a: number; b?: number } = { a: 1 }
    const tracker = track({ values: [1, 2, 3], object })
    const change = tracker.beginChange()
    change.state.values.splice(1, 1, 4, 5)
    change.state.object.b = 2
    expect(Array.isArray(change.state.values)).toBe(true)
    expect(change.state.values.length).toBe(4)
    expect([...change.state.values]).toEqual([1, 4, 5, 3])
    expect(change.state.values.map((value) => value * 2)).toEqual([2, 8, 10, 6])
    expect(Object.keys(change.state.values)).toEqual(['0', '1', '2', '3'])
    expect(Object.getOwnPropertyDescriptor(change.state.values, '2')?.value).toBe(5)
    expect(Object.keys(change.state.object)).toEqual(['a', 'b'])
    expect(JSON.parse(JSON.stringify(change.state))).toEqual({
      values: [1, 4, 5, 3],
      object: { a: 1, b: 2 },
    })
    change.abort()
  })

  test('keeps document keys distinct from object proxy target fields', () => {
    const tracker = track({
      object: {
        context: 1,
        base: 2,
        parent: 3,
        dirty: 4,
        target: 5,
        proxy: 6,
      },
    })
    const change = tracker.beginChange()
    change.state.object.context = 7
    change.state.object.proxy = 8
    expect(Object.keys(change.state.object)).toEqual([
      'context',
      'base',
      'parent',
      'dirty',
      'target',
      'proxy',
    ])
    const prepared = change.prepare()
    expect(prepared.value.object).toEqual({
      context: 7,
      base: 2,
      parent: 3,
      dirty: 4,
      target: 5,
      proxy: 8,
    })
    tracker.adopt(prepared)
  })

  test('keeps native coercion side effects and string operation forms', () => {
    const tracker = track({ text: 'abcdefgh', values: [1, 2, 3], marker: 0 })
    const change = tracker.beginChange()
    change.state.text = `${change.state.text.slice(3)}xyz`
    change.state.values.splice(
      coercing(() => {
        change.state.marker = 1
      }, 1),
      1,
      4,
    )
    const prepared = change.prepare()
    expect(prepared.value).toEqual({ text: 'defghxyz', values: [1, 4, 3], marker: 1 })
    expect(prepared.ops).toContainEqual(['t', ['text'], 3])
    expect(prepared.ops).toContainEqual(['a', ['text'], 'xyz'])
    // The codec is not ported, so the ops replay directly instead of through encode/decode.
    expect(replay(tracker.value, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('emits deletion and supports root arrays', () => {
    const initial: { keep: number; remove?: number } = { keep: 1, remove: 2 }
    const objectTracker = track(initial)
    settle(objectTracker, (draft) => {
      delete draft.remove
    })
    expect(objectTracker.value).toEqual({ keep: 1 })

    const arrayTracker = track([1, 2, 3])
    settle(arrayTracker, (draft) => {
      draft.reverse()
      draft.push(4)
    })
    expect(arrayTracker.value).toEqual([3, 2, 1, 4])
  })

  test('preserves native object key order after delete and re-add', () => {
    const initial: { first?: number; second: number } = { first: 1, second: 2 }
    const tracker = track(initial)
    const change = tracker.beginChange()
    delete change.state.first
    change.state.first = 1
    expect(Object.keys(change.state)).toEqual(['second', 'first'])
    const prepared = change.prepare()
    tracker.adopt(prepared)
    expect(Object.keys(tracker.value)).toEqual(['second', 'first'])
  })

  test('orders new integer-like object keys before strings for recipe-visible reads', () => {
    const object: Record<string, string> = { label: 'x' }
    const tracker = track({ object, first: '' })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    change.state.object['2'] = 'two'
    change.state.object['1'] = 'one'
    expect(Object.keys(change.state.object)).toEqual(['1', '2', 'label'])
    change.state.first = Object.keys(change.state.object)[0] ?? ''
    const prepared = change.prepare()
    expect(prepared.value.first).toBe('1')
    expect(Object.keys(prepared.value.object)).toEqual(['1', '2', 'label'])
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
    expect(tracker.value.first).toBe('1')
    expect(Object.keys(tracker.value.object)).toEqual(['1', '2', 'label'])
  })

  test('normalizes deeply equal container assignments before direct candidate materialization', () => {
    const initial = { child: { a: 1, b: 2 }, count: 0 }
    const tracker = track(initial)
    const change = tracker.beginChange()
    change.state.child = { b: 2, a: 1 }
    change.state.count = 1
    const prepared = change.prepare()
    const replayed = replay(initial, prepared.ops)
    expect(prepared.ops).toEqual([['s', ['count'], 1]])
    expect(Object.keys(prepared.value.child)).toEqual(['a', 'b'])
    expect(Object.keys(replayed.child)).toEqual(['a', 'b'])
    expect(prepared.value).toEqual(replayed)
    tracker.adopt(prepared)
  })

  test('keeps native integer and string ordering across deletion and re-addition', () => {
    const object: Record<string, string> = { 1: 'one', 2: 'two', first: 'a', second: 'b' }
    const tracker = track({ object })
    const change = tracker.beginChange()
    delete change.state.object['2']
    change.state.object['2'] = 'two'
    delete change.state.object.first
    change.state.object.first = 'a'
    change.state.object['3'] = 'three'
    change.state.object['0'] = 'zero'
    expect(Object.keys(change.state.object)).toEqual(['0', '1', '2', '3', 'second', 'first'])
    const prepared = change.prepare()
    expect(Object.keys(prepared.value.object)).toEqual(['0', '1', '2', '3', 'second', 'first'])
    tracker.adopt(prepared)
    expect(Object.keys(tracker.value.object)).toEqual(['0', '1', '2', '3', 'second', 'first'])
  })

  test('orders integer-like keys on null-prototype objects', () => {
    const object: Record<string, string> = cast<unknown, Record<string, string>>(
      Object.create(null),
    )
    object.label = 'x'
    const tracker = track({ object })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    change.state.object['2'] = 'two'
    change.state.object['1'] = 'one'
    expect(Object.keys(change.state.object)).toEqual(['1', '2', 'label'])
    const prepared = change.prepare()
    expect(Object.getPrototypeOf(prepared.value.object)).toBeNull()
    expect(Object.keys(prepared.value.object)).toEqual(['1', '2', 'label'])
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
    expect(Object.getPrototypeOf(tracker.value.object)).toBeNull()
    expect(Object.keys(tracker.value.object)).toEqual(['1', '2', 'label'])
  })

  test('forwards borrowed mutators to ordinary generic array receivers', () => {
    const tracker = track({ values: [1, 2, 3] })
    const change = tracker.beginChange()
    const receiver = [3, 1, 2]
    const { push, splice, sort, fill, copyWithin } = change.state.values
    expect(Reflect.apply(push, receiver, [4])).toBe(4)
    expect(Reflect.apply(splice, receiver, [1, 1, 5])).toEqual([1])
    expect(Reflect.apply(sort, receiver, [(left: number, right: number) => left - right])).toBe(
      receiver,
    )
    expect(Reflect.apply(fill, receiver, [7, 1, 2])).toBe(receiver)
    expect(Reflect.apply(copyWithin, receiver, [2, 0, 1])).toBe(receiver)
    expect(receiver).toEqual([2, 7, 2, 5])
    expect(change.state.values).toEqual([1, 2, 3])
    change.abort()
  })

  test('matches native structural coercion ordering for splice, fill, and copyWithin', () => {
    compare((values) => {
      values.splice(
        coercing(() => {
          values.push(4)
        }, 1),
        1,
        9,
      )
    })
    compare((values) => {
      values.fill(
        7,
        coercing(() => {
          values.push(4)
        }, 1),
      )
    })
    compare((values) => {
      values.copyWithin(
        coercing(() => {
          values.push(4)
        }, 1),
        0,
        2,
      )
    })
  })

  test('does not report inherited methods as own array properties', () => {
    const tracker = track({ values: [1] })
    const change = tracker.beginChange()
    expect(Object.getOwnPropertyDescriptor(change.state.values, 'map')).toBeUndefined()
    change.abort()
  })

  test('keeps repeated identical writes instead of reverting their pending override', () => {
    const values: Array<number | null> = [0]
    const tracker = track({ value: 0, values })
    settle(tracker, (draft) => {
      draft.value = 1
      draft.value = 1
      draft.values[0] = null
      draft.values[0] = null
      draft.values.push(2)
      draft.values[1] = null
      draft.values[1] = null
    })
    expect(tracker.value).toEqual({ value: 1, values: [null, null] })
  })

  test('rejects array index gaps without mutation while allowing replacement and append', () => {
    const tracker = track({ values: [1, 2] })
    // A rejected write dooms its change, so the gap goes through its own change.
    const rejected = tracker.beginChange()
    expect(() => {
      rejected.state.values[3] = 4
    }).not.toThrow()
    expect(rejected.state.values).toEqual([1, 2])
    expect(tracker.value.values).toEqual([1, 2])
    const error = prepareError(rejected)
    expect(error).toBeInstanceOf(DeltaError.InvalidWrite)
    expect(error.message).toBe('Overlay arrays cannot contain holes')
    expect(tracker.value.values).toEqual([1, 2])

    const change = tracker.beginChange()
    change.state.values[1] = 9
    change.state.values[2] = 3
    const prepared = change.prepare()
    expect(prepared.value.values).toEqual([1, 9, 3])
    expect(replay(tracker.value, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
    expect(tracker.value.values).toEqual([1, 9, 3])
  })

  test('supports optional deletion, explicit null length growth, shrinking, and default sort', () => {
    const initial: { optional?: string | undefined; values: Array<number | null> } = {
      optional: 'remove',
      values: [3, 1, 2],
    }
    const tracker = track(initial)
    settle(tracker, (draft) => {
      draft.optional = undefined
      draft.values.length = 5
      expect(draft.values).toEqual([3, 1, 2, null, null])
      draft.values.length = 4
      // oxlint-disable-next-line typescript/require-array-sort-compare -- default sort is the point
      draft.values.sort()
    })
    expect(tracker.value).toEqual({ values: [1, 2, 3, null] })

    // Deleting an element is rejected; it dooms its change, so it goes through its own.
    const change = tracker.beginChange()
    expect(() => {
      // oxlint-disable-next-line typescript/no-array-delete -- deleting an element is the point
      delete change.state.values[0]
    }).not.toThrow()
    expect(change.state.values).toEqual([1, 2, 3, null])
    const error = prepareError(change)
    expect(error).toBeInstanceOf(DeltaError.InvalidWrite)
    expect(error.message).toMatch(/holes/u)
    expect(tracker.value).toEqual({ values: [1, 2, 3, null] })
  })

  test('enumerates wide objects without duplicate keys', () => {
    const values: Record<string, number> = {}
    const tracker = track({ values })
    const change = tracker.beginChange()
    for (let index = 0; index < 20_000; index++) change.state.values[`field${index}`] = index
    const keys = Object.keys(change.state.values)
    expect(keys).toHaveLength(20_000)
    expect(keys[0]).toBe('field0')
    expect(keys.at(-1)).toBe('field19999')
    change.abort()
  })

  test('does not dirty read-only traversals', () => {
    const tracker = track({ nested: { rows: [{ value: 1 }] } })
    const change = tracker.beginChange()
    expect(change.state.nested.rows[0]?.value).toBe(1)
    const prepared = change.prepare()
    expect(prepared.ops).toEqual([])
    tracker.adopt(prepared)
  })

  test('uses one proxy identity per accessed container', () => {
    const tracker = track({ nested: { value: 1 }, rows: [{ value: 2 }] })
    const change = tracker.beginChange()
    expect(change.state.nested).toBe(change.state.nested)
    expect(change.state.rows).toBe(change.state.rows)
    expect(change.state.rows[0]).toBe(change.state.rows[0])
    change.abort()
  })
})
