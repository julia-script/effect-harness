import { describe, expect, test } from 'bun:test'
import type { Tracker } from '../index.ts'
import { clone, expectAliasFree, isProxy, replay, track } from './support.ts'

/** Ported from Chord's `test/delta-tracker/tracker.test.ts`. */

interface Item {
  id: number
  score: number
}

interface Document {
  values: Array<Item>
  text: string
  meta: { revision: number; label?: string }
}

type Mutator = (document: Document | Tracker.Draft<Document>, value: number) => void

const random = (initialSeed: number): (() => number) => {
  let seed = initialSeed
  return () => {
    // `Math.imul(x, 1)` is Chord's `x | 0`: wrap to a 32-bit integer.
    seed = Math.imul(seed + 0x6d_2b_79_f5, 1)
    let value = Math.imul(seed ^ (seed >>> 15), 1 | seed)
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296
  }
}

const item = (value: number): Item => ({ id: value, score: value % 7 })

/** Chord's `mutate` switch, one entry per choice; any choice past the table deletes the label. */
const mutators: ReadonlyArray<Mutator> = [
  (document, value) => {
    document.text += `-${value}`
  },
  (document, value) => {
    document.values.push(item(value))
  },
  (document, value) => {
    document.values.unshift(item(value))
  },
  (document) => {
    if (document.values.length > 0) document.values.shift()
  },
  (document) => {
    if (document.values.length > 0) document.values.pop()
  },
  (document, value) => {
    const empty = document.values.length === 0
    const index = empty ? 0 : value % (document.values.length + 1)
    document.values.splice(index, empty ? 0 : value % 2, item(value))
  },
  (document) => {
    document.values.reverse()
  },
  (document) => {
    document.values.sort((left, right) => left.id - right.id)
  },
  (document, value) => {
    if (document.values.length === 0) return
    const target = document.values[value % document.values.length]
    if (target !== undefined) target.score = value
  },
  (document, value) => {
    document.meta.revision++
    document.meta.label = `r-${value}`
  },
]

const mutate = (document: Document | Tracker.Draft<Document>, choice: number, value: number) => {
  const mutator = mutators[choice]
  if (mutator === undefined) delete document.meta.label
  else mutator(document, value)
}

const randomizedTransactions = () => {
  for (let seed = 1; seed <= 40; seed++) {
    const rng = random(seed)
    const initial: Document = {
      values: Array.from({ length: 4 }, (_, id) => ({ id, score: 0 })),
      text: 'start',
      meta: { revision: 0 },
    }
    const tracker = track(initial)
    const expected = clone(initial)
    for (let transaction = 0; transaction < 25; transaction++) {
      const baseRoot = tracker.value
      const base = clone(baseRoot)
      const change = tracker.beginChange()
      for (let operation = 0; operation < 5; operation++) {
        const choice = Math.floor(rng() * 11)
        const value = seed * 10_000 + transaction * 10 + operation
        mutate(expected, choice, value)
        mutate(change.state, choice, value)
      }
      const prepared = change.prepare()
      const policy = clone(prepared.value)
      const where = `seed ${seed} transaction ${transaction}`
      expect(isProxy(prepared.value)).toBe(false)
      expect(baseRoot, `prepare changed base ${where}`).toEqual(base)
      expect(policy, `policy ${where}`).toEqual(expected)
      expect(replay(base, prepared.ops), `replay ${where}`).toEqual(policy)
      tracker.adopt(prepared)
      expect(baseRoot, `adopt changed base ${where}`).toEqual(base)
      expect(tracker.value, `adopt ${where}`).toBe(prepared.value)
      expect(tracker.value, `adopt ${where}`).toEqual(policy)
      expectAliasFree(tracker.value)
    }
  }
}

interface DeepNode {
  value: number
  next?: DeepNode
}

const reverseDepthEdits = () => {
  let initial: DeepNode = { value: 0 }
  for (let depth = 0; depth < 512; depth++) initial = { value: 0, next: initial }
  const tracker = track(initial)
  const change = tracker.beginChange()
  const nodes: Array<Tracker.Draft<DeepNode>> = []
  let node: Tracker.Draft<DeepNode> | undefined = change.state
  while (node !== undefined) {
    nodes.push(node)
    node = node.next
  }
  for (let index = nodes.length - 1; index >= 0; index--) {
    const current = nodes[index]
    if (current !== undefined) current.value = index + 1
  }
  const prepared = change.prepare()
  expect(prepared.ops).toHaveLength(nodes.length)
  expect(replay(initial, prepared.ops)).toEqual(prepared.value)
  tracker.adopt(prepared)
  expectAliasFree(tracker.value)
}

describe('delta tracker randomized transactions', () => {
  test(
    'matches policy, detached replay, and adopted state after multi-operation transactions',
    randomizedTransactions,
    30_000,
  )
})

describe('delta tracker object emission scaling', () => {
  test('orders many reverse-depth object edits through the bounded fallback', reverseDepthEdits)
})
