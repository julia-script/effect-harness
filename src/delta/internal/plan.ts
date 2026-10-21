import { arrayOverlay } from './arrays.ts'
import { nodeBase } from './context.ts'
import type { ArrayPlan, OverlayNode, Piece } from './context.ts'
import { isArray } from '../../internal/json.ts'
import { piecesOf } from './pieces.ts'

/**
 * The plan of a structural array edit: base runs to remove, an optional permutation of the kept
 * base elements, and runs of inserted values. Replayed in that order it turns the base array into
 * the draft's.
 */

const rangeLength = (pieces: ReadonlyArray<Piece>, start: number, end: number): number => {
  let length = 0
  for (let index = start; index < end; index++) length += pieces[index]!.length
  return length
}

/** Runs of consecutive insert pieces as `logical index, first piece, end piece` triples. */
const insertRunsOf = (pieces: ReadonlyArray<Piece>): Array<number> => {
  const runs: Array<number> = []
  let logicalIndex = 0
  for (let pieceIndex = 0; pieceIndex < pieces.length;) {
    const piece = pieces[pieceIndex]!
    if (piece.kind === 'base') {
      logicalIndex += piece.length
      pieceIndex += 1
      continue
    }
    const startPiece = pieceIndex
    while (pieceIndex < pieces.length && pieces[pieceIndex]!.kind === 'insert') {
      logicalIndex += pieces[pieceIndex]!.length
      pieceIndex += 1
    }
    runs.push(logicalIndex - rangeLength(pieces, startPiece, pieceIndex), startPiece, pieceIndex)
  }
  return runs
}

/**
 * The plan when the retained base elements keep their order, the common case of inserts and
 * removals: computed from the pieces alone, so appending to a long array costs no walk over its
 * elements. `undefined` when base elements moved.
 */
const orderedArrayPlan = (
  baseLength: number,
  pieces: ReadonlyArray<Piece>,
): ArrayPlan | undefined => {
  // Retained base ranges in piece order, `[first, last]` each, strictly ascending.
  const ranges: Array<number> = []
  for (const piece of pieces) {
    if (piece.kind !== 'base' || piece.length === 0) continue
    if (piece.step !== 1 && piece.length > 1) return undefined
    if (ranges.length > 0 && piece.start <= ranges.at(-1)!) return undefined
    ranges.push(piece.start, piece.start + piece.length - 1)
  }
  // Unretained runs from the end backwards: after the last range, between ranges, before the first.
  const removeRuns: Array<number> = []
  let end = baseLength
  for (let index = ranges.length - 2; index >= -2; index -= 2) {
    const start = index >= 0 ? ranges[index + 1]! + 1 : 0
    if (end > start) removeRuns.push(start, end - start)
    if (index >= 0) end = ranges[index]!
  }
  return { removeRuns, permutation: undefined, insertRuns: insertRunsOf(pieces) }
}

const unretainedRuns = (retained: Uint8Array): Array<number> => {
  const runs: Array<number> = []
  for (let end = retained.length; end > 0;) {
    if (retained[end - 1] !== 0) {
      end -= 1
      continue
    }
    let start = end - 1
    while (start > 0 && retained[start - 1] === 0) start -= 1
    runs.push(start, end - start)
    end = start
  }
  return runs
}

/** The plan from a walk over every base element, for pieces that moved base elements. */
const elementArrayPlan = (baseLength: number, pieces: ReadonlyArray<Piece>): ArrayPlan => {
  const retained = new Uint8Array(baseLength)
  const targetBase: Array<number> = []
  for (const piece of pieces) {
    if (piece.kind !== 'base') continue
    for (let offset = 0; offset < piece.length; offset++) {
      const index = piece.start + piece.step * offset
      retained[index] = 1
      targetBase.push(index)
    }
  }
  const retainedBase: Array<number> = []
  for (let index = 0; index < retained.length; index++) {
    if (retained[index] !== 0) retainedBase.push(index)
  }
  let permutation: Array<number> | undefined
  if (targetBase.some((value, index) => value !== retainedBase[index])) {
    const positions = new Map(retainedBase.map((value, index) => [value, index]))
    permutation = targetBase.map((value) => positions.get(value)!)
  }
  return { removeRuns: unretainedRuns(retained), permutation, insertRuns: insertRunsOf(pieces) }
}

export const buildArrayPlan = (node: OverlayNode): ArrayPlan => {
  const overlay = arrayOverlay(node)
  if (overlay.plan !== undefined) return overlay.plan
  const base = nodeBase(node)
  const length = isArray(base) ? base.length : 0
  const pieces = piecesOf(overlay)
  overlay.plan = orderedArrayPlan(length, pieces) ?? elementArrayPlan(length, pieces)
  return overlay.plan
}
