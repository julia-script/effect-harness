/* oxlint-disable max-lines, max-lines-per-function -- ported describe blocks keep Chord's structure */
import { describe, expect, test } from 'bun:test'
import { DeltaError } from '../index.ts'
import { adoptError, clone, isProxy, prepareError, run, track } from './support.ts'

/** Ported from Chord's `test/delta-tracker/tracker.test.ts`. */

describe('delta tracker transactional overlay lifecycle', () => {
  test('materializes an immutable next revision and adopts it by pointer swap', () => {
    const initial = { count: 1, nested: { text: 'a' }, values: [1] }
    const tracker = track(initial)
    expect(tracker.value).toBe(initial)
    const change = tracker.beginChange()
    change.state.count = 2
    change.state.nested.text += 'b'
    change.state.values.push(2)
    expect(initial).toEqual({ count: 1, nested: { text: 'a' }, values: [1] })
    const prepared = change.prepare()
    const next = prepared.value
    expect(prepared.baseRevision).toBe(0)
    expect(prepared.base).toBe(initial)
    expect(next).not.toBe(initial)
    expect(isProxy(next)).toBe(false)
    expect(next).toEqual({ count: 2, nested: { text: 'ab' }, values: [1, 2] })
    expect(prepared.ops).toEqual([
      ['s', ['count'], 2],
      ['a', ['nested', 'text'], 'b'],
      ['p', ['values'], 1, 0, [2]],
    ])
    expect(tracker.value).toBe(initial)
    tracker.adopt(prepared)
    expect(initial).toEqual({ count: 1, nested: { text: 'a' }, values: [1] })
    expect(tracker.value).toBe(next)
    expect(prepared.value).toBe(next)
    expect(prepared.base).toBe(initial)
    // Never-throw: a settled handle reads as empty.
    expect(change.state.count).toBeUndefined()
  })

  test('keeps a transaction live across await and seals it only at prepare', async () => {
    const tracker = track({ left: 0, nested: { right: 0 } })
    const change = tracker.beginChange()
    change.state.left = 1
    await Promise.resolve()
    change.state.nested.right = 2
    const prepared = change.prepare()
    expect(prepared.value).toEqual({ left: 1, nested: { right: 2 } })
    expect(() => {
      change.state.left = 3
    }).toThrow(TypeError)
    tracker.adopt(prepared)
  })

  test('allows Promise resolution to inspect a settled draft without reporting a false failure', async () => {
    const initial: { then: string; value: number } = {
      // oxlint-disable-next-line unicorn/no-thenable -- a document key named `then` is the point
      then: 'document-value',
      value: 1,
    }
    const tracker = track(initial)
    const change = tracker.beginChange()
    const draft = change.state
    expect(draft.then).toBe('document-value')
    draft.value = 2
    const prepared = change.prepare()

    const resolved = await Promise.resolve(draft)
    expect(resolved).toBe(draft)
    expect(resolved.then).toBeUndefined()
    expect(resolved.value).toBeUndefined()

    tracker.adopt(prepared)
    expect(tracker.value.then).toBe('document-value')
    expect(tracker.value.value).toBe(2)

    const largeTracker = track({ rows: Array.from({ length: 4100 }, (_, value) => ({ value })) })
    const largeChange = largeTracker.beginChange()
    const largeDraft = largeChange.state
    for (const row of largeDraft.rows) expect(row.value).toBeGreaterThanOrEqual(0)
    const [first] = largeDraft.rows
    expect(first).toBeDefined()
    if (first !== undefined) first.value = -1
    const largePrepared = largeChange.prepare()
    expect(await Promise.resolve(largeDraft)).toBe(largeDraft)
    expect(largeDraft.rows).toBeUndefined()
    largeTracker.adopt(largePrepared)
  })

  test('aborts idempotently and revokes held descendants', () => {
    const tracker = track({ child: { value: 1 } })
    const change = tracker.beginChange()
    const { child } = change.state
    child.value = 2
    change.abort()
    change.abort()
    expect(tracker.value.child.value).toBe(1)
    expect(child.value).toBeUndefined()
    expect(() => change.prepare()).toThrow(/settled/u)
    const error = prepareError(change)
    expect(error).toBeInstanceOf(DeltaError.ChangeSettled)
    expect(error.message).toBe('Change has already been settled')
  })

  test('allows competing contexts and invalidates loser views', () => {
    const tracker = track({ value: 0 })
    const first = tracker.beginChange()
    const second = tracker.beginChange()
    first.state.value = 1
    second.state.value = 2
    const firstPrepared = first.prepare()
    const secondPrepared = second.prepare()
    const held = secondPrepared.value
    tracker.adopt(firstPrepared)
    expect(tracker.value.value).toBe(1)
    expect(held.value).toBe(2)
    expect(() => {
      tracker.adopt(secondPrepared)
    }).toThrow(/stale/u)
    expect(() => {
      tracker.adopt(firstPrepared)
    }).toThrow(/already been used/u)
    const stale = adoptError(tracker, secondPrepared)
    expect(stale).toBeInstanceOf(DeltaError.AdoptRejected)
    expect(stale.reason).toBe('stale')
    const consumed = adoptError(tracker, firstPrepared)
    expect(consumed.reason).toBe('consumed')
    expect(consumed.message).toBe('Prepared change has already been used')
  })

  test('invalidates competing open overlays without changing their base revision', () => {
    const initial = { rows: [{ value: 0 }, { value: 1 }] }
    const tracker = track(initial)
    const staleChange = tracker.beginChange()
    const held = staleChange.state.rows[1]
    expect(held).toBeDefined()
    if (held !== undefined) held.value = 2

    const winner = tracker.beginChange()
    const [winnerRow] = winner.state.rows
    if (winnerRow !== undefined) winnerRow.value = 3
    const prepared = winner.prepare()
    tracker.adopt(prepared)

    expect(initial).toEqual({ rows: [{ value: 0 }, { value: 1 }] })
    expect(tracker.value).toEqual({ rows: [{ value: 3 }, { value: 1 }] })
    expect(held?.value).toBeUndefined()
    expect(() => staleChange.prepare()).toThrow(/settled/u)
    const error = prepareError(staleChange)
    expect(error).toBeInstanceOf(DeltaError.ChangeSettled)
    expect(error.message).toBe('Cannot use a settled overlay')
    staleChange.abort()
  })

  test('adopts object edits as ordinary fast-layout immutable revisions', () => {
    const initial: { first: number; second?: number; third?: number } = { first: 1, second: 2 }
    const tracker = track(initial)
    const change = tracker.beginChange()
    change.state.first = 3
    delete change.state.second
    change.state.third = 4
    const prepared = change.prepare()
    tracker.adopt(prepared)
    expect(tracker.value).not.toBe(initial)
    expect(initial).toEqual({ first: 1, second: 2 })
    expect(tracker.value).toEqual({ first: 3, third: 4 })
    expect(Object.getOwnPropertyDescriptor(tracker.value, 'third')).toEqual({
      value: 4,
      writable: true,
      enumerable: true,
      configurable: true,
    })
  })

  test('supports adopt-then-publish ordering and retained publication reads', () => {
    const tracker = track({ value: 0, nested: { count: 0 } })
    const change = tracker.beginChange()
    change.state.value = 1
    change.state.nested.count = 1
    const prepared = change.prepare()
    const operations = prepared.ops
    tracker.adopt(prepared)
    // Matches services/state.ts: adopt first, then inspect ops and publish prepared.value.
    expect(prepared.ops).toBe(operations)
    const published = prepared.ops.length === 0 ? undefined : prepared.value
    expect(operations).toHaveLength(2)
    expect(published).toBeDefined()
    expect(published).toBe(tracker.value)
    expect(clone(published)).toEqual({ value: 1, nested: { count: 1 } })

    const next = tracker.beginChange()
    next.state.value = 2
    const nextPrepared = next.prepare()
    tracker.adopt(nextPrepared)
    // Retained immutable publications remain at their original revision.
    expect(published).not.toBe(tracker.value)
    expect(prepared.value).toEqual({ value: 1, nested: { count: 1 } })
    expect(tracker.value).toEqual({ value: 2, nested: { count: 1 } })
  })

  test('keeps replacement base/value structurally compatible and readable after adoption', () => {
    const original = { value: 0 }
    const replacement = { value: 1 }
    const tracker = track(original)
    const prepared = tracker.prepareReplace(replacement)
    expect(prepared.base).toBe(original)
    expect(prepared.value).toBe(replacement)
    tracker.adopt(prepared)
    expect(prepared.base).toBe(original)
    expect(prepared.value).toBe(replacement)
    expect(prepared.value).toBe(tracker.value)
  })

  test('keeps aborted and stale materialized candidates readable', () => {
    const tracker = track({ value: 0 })
    const abortedChange = tracker.beginChange()
    abortedChange.state.value = 1
    const aborted = abortedChange.prepare()
    const abortedView = aborted.value
    run(aborted.abort)
    expect(abortedView.value).toBe(1)
    expect(aborted.value.value).toBe(1)

    const loserChange = tracker.beginChange()
    loserChange.state.value = 2
    const loser = loserChange.prepare()
    const loserView = loser.value
    const winner = tracker.prepareReplace({ value: 3 })
    tracker.adopt(winner)
    expect(loserView.value).toBe(2)
    expect(loser.value.value).toBe(2)
  })

  test('lets a settled Change abort its prepared result without retaining its context', () => {
    const tracker = track({ value: 0, nested: { value: 1 } })
    const change = tracker.beginChange()
    change.state.value = 1
    const prepared = change.prepare()
    const operations = prepared.ops
    change.abort()
    change.abort()
    expect(prepared.ops).toBe(operations)
    expect(prepared.value).toEqual({ value: 1, nested: { value: 1 } })
    expect(() => {
      tracker.adopt(prepared)
    }).toThrow(/aborted/u)
    expect(adoptError(tracker, prepared).reason).toBe('aborted')
  })

  test('rejects foreign and aborted prepared values', () => {
    const first = track({ value: 0 })
    const second = track({ value: 0 })
    const prepared = first.prepareReplace({ value: 1 })
    expect(() => {
      second.adopt(prepared)
    }).toThrow(/different tracker/u)
    const foreign = adoptError(second, prepared)
    expect(foreign).toBeInstanceOf(DeltaError.AdoptRejected)
    expect(foreign.reason).toBe('foreign')
    expect(foreign.message).toBe('Prepared change belongs to a different tracker')
    run(prepared.abort)
    run(prepared.abort)
    expect(() => {
      first.adopt(prepared)
    }).toThrow(/aborted/u)
    const aborted = adoptError(first, prepared)
    expect(aborted.reason).toBe('aborted')
    expect(aborted.message).toBe('Prepared change has been aborted')
  })

  test('reads deleted own properties as absent', () => {
    // oxlint-disable-next-line no-extend-native -- the test needs an inherited document key
    Object.defineProperty(Object.prototype, 'trackerInherited', {
      value: 7,
      writable: true,
      configurable: true,
    })
    try {
      const initial: { value?: number; trackerInherited?: number } = { value: 1 }
      const tracker = track(initial)
      const change = tracker.beginChange()
      delete change.state.value
      change.state.trackerInherited = 1
      delete change.state.trackerInherited
      expect(change.state.value).toBeUndefined()
      expect(change.state.trackerInherited).toBeUndefined()
      const prepared = change.prepare()
      expect(prepared.value.value).toBeUndefined()
      // Plain committed values expose inherited properties through ordinary
      // JavaScript reads; strict-JSON membership remains own-properties only.
      expect(prepared.value.trackerInherited).toBe(7)
      tracker.adopt(prepared)
      expect(Object.keys(tracker.value)).toEqual([])
    } finally {
      Reflect.deleteProperty(Object.prototype, 'trackerInherited')
    }
  })

  test('detaches old handles for deeply equal object and array assignments before normalizing', () => {
    const initial = {
      object: { nested: { value: 1 } },
      array: [{ value: 1 }],
      rows: [{ value: 1 }],
    }
    const tracker = track(initial)
    const change = tracker.beginChange()
    const oldObject = change.state.object
    const oldArray = change.state.array
    const [oldRow] = change.state.rows
    change.state.object = { nested: { value: 1 } }
    change.state.array = [{ value: 1 }]
    change.state.rows[0] = { value: 1 }
    expect(change.state.object).not.toBe(oldObject)
    expect(change.state.array).not.toBe(oldArray)
    expect(change.state.rows[0]).not.toBe(oldRow)
    oldObject.nested.value = 9
    const [oldArrayRow] = oldArray
    if (oldArrayRow !== undefined) oldArrayRow.value = 9
    if (oldRow !== undefined) oldRow.value = 9
    expect(change.state).toEqual({
      object: { nested: { value: 1 } },
      array: [{ value: 1 }],
      rows: [{ value: 1 }],
    })
    const prepared = change.prepare()
    expect(prepared.ops).toEqual([])
    tracker.adopt(prepared)
    expect(tracker.value).toBe(initial)
    expect(tracker.value.object).toBe(initial.object)
    expect(tracker.value.array).toBe(initial.array)
    expect(tracker.value.rows[0]).toBe(initial.rows[0])
  })

  test('normalizes a deeply equal replacement without changing committed identity', () => {
    const initial = { nested: { value: 1 }, rows: [1, 2, 3] }
    const tracker = track(initial)
    const prepared = tracker.prepareReplace({ nested: { value: 1 }, rows: [1, 2, 3] })
    expect(prepared.ops).toEqual([])
    tracker.adopt(prepared)
    expect(tracker.value).toBe(initial)
  })

  test('takes O(1) immutable ownership for replacement and its operation payload', () => {
    const rows: Array<{ value: number }> = []
    const tracker = track({ value: 0, rows })
    const replacement = { value: 1, rows: [{ value: 2 }] }
    const prepared = tracker.prepareReplace(replacement)
    expect(prepared.base).toBe(tracker.value)
    expect(prepared.value).toBe(replacement)
    const operations = prepared.ops
    expect(operations).toEqual([['r', replacement]])
    const [operation] = operations
    expect(operation?.[1]).toBe(replacement)
    tracker.adopt(prepared)
    expect(tracker.value).toBe(replacement)
  })
})
