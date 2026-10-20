/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks keep Chord's structure */
/* oxlint-disable unicorn/no-array-sort, unicorn/no-array-reverse -- in-place mutators are under test */
import { describe, expect, test } from 'bun:test'
import { cast } from 'effect/Function'
import { clone, expectAliasFree, replay, settle, track } from './support.ts'

/** Ported from Chord's `test/delta-tracker/tracker.test.ts`. */

describe('delta tracker piece arrays', () => {
  test('emits one insert for an append to a long array', () => {
    const tracker = track({ values: Array.from({ length: 10_000 }, (_, index) => index) })
    const change = tracker.beginChange()
    change.state.values.push(-1)
    const prepared = change.prepare()
    expect(prepared.ops).toEqual([['p', ['values'], 10_000, 0, [-1]]])
    tracker.adopt(prepared)
    expect(tracker.value.values.at(-1)).toBe(-1)
    expect(tracker.value.values).toHaveLength(10_001)
  })

  test('emits removals from the end backwards, then inserts, when kept elements keep their order', () => {
    const tracker = track({ values: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] })
    const values = settle(tracker, (draft) => {
      // gap before the first kept element
      draft.values.shift()
      // gap between kept elements, with an insert
      draft.values.splice(1, 2, 20)
      // another gap
      draft.values.splice(5, 1)
      // gap after the last kept element
      draft.values.pop()
      draft.values.push(30)
    })
    expect(values.values).toEqual([1, 20, 4, 5, 6, 8, 30])
    const change = track({ values: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] }).beginChange()
    change.state.values.shift()
    change.state.values.splice(1, 2, 20)
    change.state.values.splice(5, 1)
    change.state.values.pop()
    change.state.values.push(30)
    expect(change.prepare().ops).toEqual([
      ['p', ['values'], 9, 1, []],
      ['p', ['values'], 7, 1, []],
      ['p', ['values'], 2, 2, []],
      ['p', ['values'], 0, 1, []],
      ['p', ['values'], 1, 0, [20]],
      ['p', ['values'], 6, 0, [30]],
    ])
  })

  test('emits a permutation when kept elements change order', () => {
    for (const mutate of [
      (values: Array<number>) => values.reverse(),
      (values: Array<number>) =>
        values.sort((left, right) => {
          const parity = (left % 2) - (right % 2)
          return parity === 0 ? left - right : parity
        }),
    ]) {
      const tracker = track({ values: [0, 1, 2, 3, 4] })
      const expected = [0, 1, 2, 3, 4]
      mutate(expected)
      const change = tracker.beginChange()
      mutate(change.state.values)
      const prepared = change.prepare()
      expect(prepared.ops.some((operation) => operation[0] === 'm')).toBe(true)
      expect(replay(tracker.value, prepared.ops)).toEqual({ values: expected })
    }
  })

  test('keeps adoption independent from mutable detached permutation metadata', () => {
    const tracker = track({ values: [3, 1, 2] })
    const change = tracker.beginChange()
    change.state.values.sort((left, right) => left - right)
    const prepared = change.prepare()
    const permutation = prepared.ops.find((operation) => operation[0] === 'm')
    if (permutation?.[0] !== 'm') throw new Error('expected permutation')
    // The op is typed readonly; mutate it anyway to prove adoption does not depend on it.
    Reflect.apply(Array.prototype.reverse, permutation[2], [])
    tracker.adopt(prepared)
    expect(tracker.value.values).toEqual([1, 2, 3])
  })

  test('matches native splice with zero or one argument', () => {
    const noArguments = track({ values: [1, 2, 3] })
    const noArgumentsChange = noArguments.beginChange()
    expect(
      Reflect.apply(noArgumentsChange.state.values.splice, noArgumentsChange.state.values, []),
    ).toEqual([])
    const noArgumentsPrepared = noArgumentsChange.prepare()
    expect(noArgumentsPrepared.value.values).toEqual([1, 2, 3])
    expect(noArgumentsPrepared.ops).toEqual([])
    noArguments.adopt(noArgumentsPrepared)

    const oneArgument = track({ values: [1, 2, 3, 4] })
    const oneArgumentChange = oneArgument.beginChange()
    expect(oneArgumentChange.state.values.splice(1)).toEqual([2, 3, 4])
    const oneArgumentPrepared = oneArgumentChange.prepare()
    expect(oneArgumentPrepared.value.values).toEqual([1])
    expect(replay(oneArgument.value, oneArgumentPrepared.ops)).toEqual(oneArgumentPrepared.value)
    oneArgument.adopt(oneArgumentPrepared)

    const pastEnd = track({ values: [1, 2, 3] })
    const pastEndChange = pastEnd.beginChange()
    expect(pastEndChange.state.values.splice(3)).toEqual([])
    const pastEndPrepared = pastEndChange.prepare()
    expect(pastEndPrepared.value.values).toEqual([1, 2, 3])
    pastEnd.adopt(pastEndPrepared)
  })

  test('supports all structural mutators and native return values', () => {
    const tracker = track({ values: [3, 1, 2] })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    expect(change.state.values.push(4)).toBe(4)
    expect(change.state.values.pop()).toBe(4)
    expect(change.state.values.unshift(0)).toBe(4)
    expect(change.state.values.shift()).toBe(0)
    expect(change.state.values.splice(1, 1, 5, 4)).toEqual([1])
    expect(change.state.values.sort((left, right) => left - right)).toBe(change.state.values)
    expect(change.state.values.reverse()).toBe(change.state.values)
    change.state.values.fill(9, 1, 3)
    change.state.values.copyWithin(1, 0, 2)
    const prepared = change.prepare()
    expect(prepared.value).toEqual({ values: [5, 5, 9, 2] })
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('tracks held handles after reindex and suppresses detached writes', () => {
    const tracker = track({ values: [{ value: 'a' }, { value: 'b' }, { value: 'c' }] })
    const change = tracker.beginChange()
    const held = change.state.values[1]!
    change.state.values.unshift({ value: 'front' })
    held.value = 'moved'
    expect(change.state.values[2]!.value).toBe('moved')
    change.state.values.splice(2, 1)
    held.value = 'detached'
    const prepared = change.prepare()
    expect(prepared.value).toEqual({
      values: [{ value: 'front' }, { value: 'a' }, { value: 'c' }],
    })
    expect(replay(tracker.value, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('replays introduced, moved, then edited array descendants', () => {
    const values: Array<{ id: number; nested: Array<number> }> = []
    const tracker = track({ values })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    change.state.values.push({ id: 1, nested: [1] })
    const held = change.state.values[0]!
    change.state.values.unshift({ id: 0, nested: [] })
    held.nested.push(2)
    change.state.values.reverse()
    held.nested.push(3)
    const prepared = change.prepare()
    expect(prepared.value).toEqual({
      values: [
        { id: 1, nested: [1, 2, 3] },
        { id: 0, nested: [] },
      ],
    })
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('combines movement and edits with permutation plus final paths', () => {
    const tracker = track({
      values: [
        { rank: 3, edited: 0 },
        { rank: 1, edited: 0 },
        { rank: 2, edited: 0 },
      ],
    })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    const held = change.state.values[0]!
    change.state.values.sort((left, right) => left.rank - right.rank)
    held.edited = 1
    const prepared = change.prepare()
    expect(prepared.ops[0]).toEqual(['m', ['values'], [1, 2, 0]])
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('keeps comparator writes and structurally reentrant appends', () => {
    const tracker = track({
      values: [
        { rank: 2, comparisons: 0 },
        { rank: 1, comparisons: 0 },
      ],
    })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    let appended = false
    change.state.values.sort((left, right) => {
      left.comparisons++
      right.comparisons++
      if (!appended) {
        appended = true
        change.state.values[0] = { rank: 9, comparisons: 0 }
        change.state.values.push({ rank: 3, comparisons: 0 })
      }
      return left.rank - right.rank
    })
    const prepared = change.prepare()
    expect(prepared.value.values.map((value) => value.rank)).toEqual([1, 2, 3])
    expect(prepared.value.values.map((value) => value.comparisons)).toEqual([1, 1, 0])
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('normalizes duplicate entries created by structurally reentrant sort callbacks', () => {
    const tracker = track({
      values: [
        { rank: 2, edited: 0, nested: { value: 0 } },
        { rank: 1, edited: 0, nested: { value: 0 } },
      ],
    })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    let inserted = false
    change.state.values.sort((left, right) => {
      left.edited = left.rank * 10
      left.nested.value = left.rank * 100
      right.edited = right.rank * 10
      right.nested.value = right.rank * 100
      if (!inserted) {
        inserted = true
        change.state.values.unshift({ rank: 4, edited: 0, nested: { value: 0 } })
      }
      return left.rank - right.rank
    })
    const prepared = change.prepare()
    expect(prepared.value.values.map((value) => value.rank)).toEqual([1, 2, 1])
    expect(prepared.value.values.map((value) => value.edited)).toEqual([10, 20, 10])
    expect(prepared.value.values.map((value) => value.nested.value)).toEqual([100, 200, 100])
    expect(prepared.value.values[0]).not.toBe(prepared.value.values[2])
    expect(prepared.value.values[0]!.nested).not.toBe(prepared.value.values[2]!.nested)
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    expectAliasFree(prepared.value)
    tracker.adopt(prepared)
  })

  test('folds dense child and direct-index edits into one full-array operation', () => {
    const rowsTracker = track({ rows: Array.from({ length: 1000 }, (_, value) => ({ value })) })
    const rowsBase = clone(rowsTracker.value)
    const rowsChange = rowsTracker.beginChange()
    for (const row of rowsChange.state.rows) row.value += 1
    const rowsPrepared = rowsChange.prepare()
    expect(rowsPrepared.ops).toHaveLength(1)
    expect(rowsPrepared.ops[0]?.[0]).toBe('p')
    expect(replay(rowsBase, rowsPrepared.ops)).toEqual(rowsPrepared.value)
    rowsTracker.adopt(rowsPrepared)

    const valuesTracker = track({ values: Array.from({ length: 1000 }, (_, value) => value) })
    const valuesBase = clone(valuesTracker.value)
    const valuesChange = valuesTracker.beginChange()
    for (let index = 0; index < 600; index++) valuesChange.state.values[index] = -index - 1
    const valuesPrepared = valuesChange.prepare()
    expect(valuesPrepared.ops).toHaveLength(1)
    expect(valuesPrepared.ops[0]?.[0]).toBe('p')
    expect(replay(valuesBase, valuesPrepared.ops)).toEqual(valuesPrepared.value)
    valuesTracker.adopt(valuesPrepared)
  })

  test('folds only a deeply nested dense array region', () => {
    const tracker = track({
      rows: Array.from({ length: 2000 }, (_, value) => ({ nested: { value } })),
    })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    change.state.rows[0]!.nested.value = -1
    for (let index = 500; index < 1000; index++) change.state.rows[index]!.nested.value = -index
    const prepared = change.prepare()
    const splice = prepared.ops.find((operation) => operation[0] === 'p')
    expect(splice?.slice(0, 4)).toEqual(['p', ['rows'], 500, 500])
    if (splice?.[0] !== 'p') throw new Error('expected regional splice')
    expect(splice[4]).toHaveLength(500)
    expect(prepared.ops).toHaveLength(2)
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('does not cache emission paths for nested reserved-key folds covered by an outer region', () => {
    interface Row {
      flag: number
      special?: { __proto__: { values: Array<number> } }
    }
    const rows: Array<Row> = Array.from({ length: 400 }, () => ({ flag: 0 }))
    rows[100]!.special = cast(
      JSON.parse(
        `{"__proto__":{"values":[${Array.from({ length: 400 }, (_, value) => value).join(',')}]}}`,
      ),
    )
    const tracker = track({ rows })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    for (let index = 0; index < 256; index++) change.state.rows[index]!.flag = 1
    for (let index = 0; index < 256; index++) {
      change.state.rows[100]!.special!.__proto__.values[index] = -index - 1
    }
    const prepared = change.prepare()
    expect(prepared.ops).toHaveLength(1)
    expect(prepared.ops[0]?.slice(0, 4)).toEqual(['p', ['rows'], 0, 256])
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('suppresses nested structural and leaf operations covered by an outer dense region', () => {
    const tracker = track({
      rows: Array.from({ length: 400 }, (_, index) => ({
        flag: 0,
        values: index === 100 ? Array.from({ length: 400 }, (__, value) => ({ value })) : [],
      })),
    })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    for (let index = 0; index < 256; index++) change.state.rows[index]!.flag = 1
    for (let index = 0; index < 256; index++) {
      change.state.rows[100]!.values[index]!.value = -index - 1
    }
    change.state.rows[100]!.values.push({ value: 999 })
    const prepared = change.prepare()
    expect(prepared.ops).toHaveLength(1)
    expect(prepared.ops[0]?.slice(0, 4)).toEqual(['p', ['rows'], 0, 256])
    expect(prepared.value.rows[100]!.values).toHaveLength(401)
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('emits multiple disjoint dense regions and preserves operations outside their boundaries', () => {
    const tracker = track({ values: Array.from({ length: 1400 }, (_, value) => ({ value })) })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    for (const index of [97, 358, 897, 1158]) change.state.values[index]!.value = -index - 1
    for (let index = 100; index < 356; index++) change.state.values[index]!.value = -index - 1
    for (let index = 900; index < 1156; index++) change.state.values[index]!.value = -index - 1
    const prepared = change.prepare()
    const splices = prepared.ops.filter((operation) => operation[0] === 'p')
    expect(splices.map((operation) => operation.slice(0, 4))).toEqual([
      ['p', ['values'], 100, 256],
      ['p', ['values'], 900, 256],
    ])
    for (const index of [97, 358, 897, 1158]) {
      expect(prepared.ops).toContainEqual(['s', ['values', index, 'value'], -index - 1])
    }
    expect(prepared.ops).toHaveLength(6)
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('normalizes a 20,000-operation queue transaction to two operations', () => {
    const tracker = track({ values: Array.from({ length: 20_000 }, (_, value) => ({ value })) })
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    for (let index = 0; index < 10_000; index++) {
      change.state.values.shift()
      change.state.values.push({ value: 20_000 + index })
    }
    const prepared = change.prepare()
    expect(prepared.ops).toHaveLength(2)
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  })

  test('handles adversarial fragmentation and resolves held handles without piece scans', () => {
    const size = 20_000
    const heldIndices = [1, 1001, 5001, 10_001, 15_001, 19_999]
    const tracker = track({ values: Array.from({ length: size }, (_, value) => ({ value })) })
    const expected = clone(tracker.value)
    const base = clone(tracker.value)
    const change = tracker.beginChange()
    const held = heldIndices.map((index) => change.state.values[index]!)
    for (let index = 0; index < size; index += 2) {
      change.state.values.splice(index, 1, { value: -index - 1 })
      expected.values.splice(index, 1, { value: -index - 1 })
    }
    for (const value of held) value.value += 100_000
    for (const index of heldIndices) expected.values[index]!.value += 100_000
    const prepared = change.prepare()
    expect(prepared.value).toEqual(expected)
    expect(replay(base, prepared.ops)).toEqual(prepared.value)
    tracker.adopt(prepared)
  }, 30_000)

  test('normalizes restored overrides and cancelled structural edits to no operations', () => {
    const tracker = track({ values: [1, 2, 3] })
    const change = tracker.beginChange()
    change.state.values[1] = 9
    change.state.values[1] = 2
    change.state.values.reverse()
    change.state.values.reverse()
    change.state.values.push(4)
    expect(change.state.values.pop()).toBe(4)
    const prepared = change.prepare()
    expect(prepared.value).toEqual({ values: [1, 2, 3] })
    expect(prepared.ops).toEqual([])
    tracker.adopt(prepared)
  })

  test('handles null sparse overrides without confusing absence', () => {
    const tracker = track({
      values: Array.from({ length: 10_000 }, (_, value): number | null => value),
    })
    settle(tracker, (draft) => {
      draft.values[17] = null
      draft.values[9000] = null
    })
    expect(tracker.value.values[17]).toBeNull()
    expect(tracker.value.values[9000]).toBeNull()
  })

  test('supports self-overlapping fill and copyWithin by value', () => {
    const tracker = track({ values: [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }] })
    settle(tracker, (draft) => {
      draft.values.copyWithin(1, 0, 3)
      draft.values.fill(draft.values[1]!, 0, 2)
      draft.values[0]!.n = 9
    })
    expect(tracker.value.values.map((value) => value.n)).toEqual([9, 0, 1, 2])
    expect(tracker.value.values[0]).not.toBe(tracker.value.values[1])
  })

  test.each(['unshift', 'splice'] as const)(
    'inserts 100,000 items with %s',
    (method) => {
      const tracker = track({ values: [-1] })
      const base = clone(tracker.value)
      const change = tracker.beginChange()
      const items = Array.from({ length: 100_000 }, (_, value) => value)
      if (method === 'unshift') {
        Reflect.apply(change.state.values.unshift, change.state.values, items)
      } else {
        Reflect.apply(change.state.values.splice, change.state.values, [1, 0, ...items])
      }
      const prepared = change.prepare()
      expect(prepared.value.values).toHaveLength(100_001)
      expect(prepared.value.values[method === 'unshift' ? 99_999 : 100_000]).toBe(99_999)
      expect(replay(base, prepared.ops)).toEqual(prepared.value)
      tracker.adopt(prepared)
    },
    30_000,
  )
})
