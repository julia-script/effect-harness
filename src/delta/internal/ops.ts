import type { Op } from '../../storage/schema/Document.ts'
import type { Json } from '../../internal/json.ts'

/**
 * Op paths and the string overlap of Chord's `delta/index.ts`. Ops are the storage `Op` tuples, so
 * a prepared batch is a document delta as it is.
 */

export type { Op }
export type Segment = string | number
export type Path = ReadonlyArray<Segment>
export type NonEmptyPath = readonly [Segment, ...Array<Segment>]

/**
 * Segments that reach the prototype chain through `parent[key] = value`. The tracker never emits
 * them: a mutation at or below one folds into a set of the nearest safe ancestor.
 */
export const RESERVED_SEGMENTS: ReadonlySet<Segment> = new Set([
  '__proto__',
  'constructor',
  'prototype',
])

export const isReserved = (segment: Segment): boolean =>
  typeof segment === 'string' && RESERVED_SEGMENTS.has(segment)

/** The index of the first reserved segment of `path`, or -1. */
export const reservedIndex = (path: Path): number =>
  path.findIndex((segment) => isReserved(segment))

const isNonEmpty = (path: Path): path is NonEmptyPath => path.length > 0

/** `path` extended by one segment. */
export const childPath = (path: Path, key: Segment): NonEmptyPath => {
  const next = [...path, key]
  return isNonEmpty(next) ? next : [key]
}

/** A set at `path`, or a replacement when it is the root. */
export const setOp = (path: Path, value: Json): Op =>
  isNonEmpty(path) ? ['s', path, value] : ['r', value]

const overlapAt = (tail: string, b: string, head: string, maxCandidates: number): number => {
  let tried = 0
  for (let k = tail.indexOf(head); k !== -1; k = tail.indexOf(head, k + 1)) {
    tried += 1
    if (tried > maxCandidates) break
    const n = tail.length - k
    if (n <= b.length && tail.slice(k) === b.slice(0, n)) return n
  }
  return 0
}

/**
 * Longest suffix of `a` that is a prefix of `b`. Probes with indexOf and verifies exact substring
 * equality, so the hot loops are native.
 *
 * A probe of length h only finds overlaps of at least h, so a long head goes first (few
 * candidates, and it catches the large overlaps a rolling window produces), then one character.
 * Candidates are bounded because repetitive output matches a long head at thousands of positions;
 * giving up returns 0, which emits a set: larger, never wrong.
 */
export const overlap = (a: string, b: string, scan: number, probe = 64, maxCandidates = 8) => {
  if (a.length === 0 || b.length === 0 || scan === 0) return 0
  const tail = a.length > scan ? a.slice(a.length - scan) : a
  const long = Math.min(probe, b.length)
  const found = overlapAt(tail, b, b.slice(0, long), maxCandidates)
  if (found > 0 || long === 1) return found
  return overlapAt(tail, b, b.slice(0, 1), maxCandidates)
}
