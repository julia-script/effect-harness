import { expect } from 'bun:test'
import { isProxy } from 'node:util/types'
import { Effect } from 'effect'
import { cast } from 'effect/Function'
import { applyBatches } from '../../storage/internal/delta.ts'
import type { Op } from '../../storage/schema/Document.ts'
import type { JsonObject } from '../../storage/schema/Json.ts'
import { Tracker } from '../index.ts'

/**
 * Helpers for the tracker tests, ported from Chord's `test/delta-tracker`. The tracker's Effects
 * are synchronous, so `track` runs them through a small adapter and the tests read like Chord's.
 */

export interface SyncChange<T extends object> {
  readonly change: Tracker.Change<T>
  readonly state: Tracker.Draft<T>
  readonly prepare: () => Tracker.Prepared<T>
  readonly abort: () => void
}

export interface SyncTracker<T extends object> {
  readonly tracker: Tracker.Tracker<T>
  readonly value: T
  readonly revision: number
  readonly beginChange: () => SyncChange<T>
  readonly prepareReplace: (value: T) => Tracker.Prepared<T>
  readonly adopt: (prepared: Tracker.Prepared<T>) => void
}

export const run = <A, E>(effect: Effect.Effect<A, E>): A => Effect.runSync(effect)

/** The failure of a prepare that must fail. */
export const prepareError = <T extends object>(change: SyncChange<T>) =>
  run(Effect.flip(change.change.prepare))

/** The failure of an adoption that must fail. */
export const adoptError = <T extends object>(
  tracker: SyncTracker<T>,
  prepared: Tracker.Prepared<T>,
) => run(Effect.flip(tracker.tracker.adopt(prepared)))

const syncChange = <T extends object>(change: Tracker.Change<T>): SyncChange<T> => ({
  change,
  state: change.state,
  prepare: () => run(change.prepare),
  abort: () => {
    run(change.abort)
  },
})

export const track = <T extends object>(initial: T): SyncTracker<T> => {
  const tracker = run(Tracker.make(initial))
  return {
    tracker,
    get value() {
      return run(tracker.value)
    },
    get revision() {
      return run(tracker.revision)
    },
    beginChange: () => syncChange(run(tracker.beginChange)),
    prepareReplace: (value) => run(tracker.prepareReplace(value)),
    adopt: (prepared) => {
      run(tracker.adopt(prepared))
    },
  }
}

export const clone = <T>(value: T): T => cast(JSON.parse(JSON.stringify(value)))

/** Replay ops on a detached copy of `base` with the storage applier. */
export const replay = <T>(base: T, operations: ReadonlyArray<Op>): T => {
  const root: JsonObject = cast(clone(base))
  return cast(run(applyBatches(root, [clone(operations)])))
}

export const expectAliasFree = (value: unknown): void => {
  const seen = new WeakMap<object, string>()
  const visit = (current: unknown, path: string): void => {
    if (current === null || typeof current !== 'object') return
    const previous = seen.get(current)
    expect(previous, `container at ${path} aliases ${previous ?? ''}`).toBeUndefined()
    seen.set(current, path)
    if (Array.isArray(current)) {
      for (const [index, item] of current.entries()) visit(item, `${path}[${index}]`)
      return
    }
    for (const [key, item] of Object.entries(current)) visit(item, `${path}.${key}`)
  }
  visit(value, '$root')
}

/**
 * Run one change through prepare and adopt, checking that the base is untouched, the candidate
 * is no proxy, and replaying the ops on the base yields the candidate.
 */
export const settle = <T extends object>(
  tracker: SyncTracker<T>,
  mutate: (draft: Tracker.Draft<T>) => void,
): T => {
  const baseRoot = tracker.value
  const base = clone(baseRoot)
  const change = tracker.beginChange()
  mutate(change.state)
  const prepared = change.prepare()
  const candidate = clone(prepared.value)
  expect(isProxy(prepared.value)).toBe(false)
  expect(replay(base, prepared.ops)).toEqual(candidate)
  expect(baseRoot).toEqual(base)
  tracker.adopt(prepared)
  expect(baseRoot).toEqual(base)
  expect(tracker.value).toBe(prepared.value)
  expect(tracker.value).toEqual(candidate)
  return candidate
}

export { isProxy }
