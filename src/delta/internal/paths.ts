import { arrayOverlay, entryValueAt } from './arrays.ts'
import { nodeBase, nodeParent } from './context.ts'
import type {
  ArrayOverlay,
  InsertSource,
  OverlayNode,
  ParentKind,
  PieceLocation,
} from './context.ts'
import { objectHas, objectValue } from './objects.ts'
import type { Path } from './ops.ts'
import { locatePiece, piecesOf } from './pieces.ts'

/**
 * Where a node sits in the draft at `prepare()`. A node records the base or insert index it was
 * read from; its logical index comes from a binary search over the piece locations, so held
 * handles follow their element through reindexing without scanning pieces.
 */

const byMinimum = (left: PieceLocation, right: PieceLocation): number =>
  left.minimum - right.minimum

const ensurePieceLocations = (overlay: ArrayOverlay): void => {
  if (overlay.baseLocations !== undefined) return
  const baseLocations: Array<PieceLocation> = []
  const insertLocations = new Map<InsertSource, Array<PieceLocation>>()
  let logicalStart = 0
  for (const piece of piecesOf(overlay)) {
    const last = piece.start + piece.step * (piece.length - 1)
    const location: PieceLocation = {
      piece,
      logicalStart,
      minimum: Math.min(piece.start, last),
      maximum: Math.max(piece.start, last),
    }
    if (piece.kind === 'base') baseLocations.push(location)
    else {
      let locations = insertLocations.get(piece.source)
      if (locations === undefined) {
        locations = []
        insertLocations.set(piece.source, locations)
      }
      locations.push(location)
    }
    logicalStart += piece.length
  }
  baseLocations.sort(byMinimum)
  for (const locations of insertLocations.values()) locations.sort(byMinimum)
  overlay.baseLocations = baseLocations
  overlay.insertLocations = insertLocations
}

/** The logical index of a base (`kind` 1) or inserted (`kind` 2) entry, if it is still present. */
export const findEntryIndex = (
  overlay: ArrayOverlay,
  kind: ParentKind,
  sourceIndex: number,
  source?: InsertSource,
): number | undefined => {
  if (kind === 1 && !overlay.structural) return sourceIndex
  ensurePieceLocations(overlay)
  const locations =
    kind === 1 ? overlay.baseLocations : overlay.insertLocations?.get(source ?? NO_SOURCE)
  if (locations === undefined) return undefined
  let low = 0
  let high = locations.length
  while (low < high) {
    const middle = (low + high) >>> 1
    if (locations[middle]!.minimum <= sourceIndex) low = middle + 1
    else high = middle
  }
  const location = locations[low - 1]
  if (location === undefined || sourceIndex > location.maximum) return undefined
  const offset = (sourceIndex - location.piece.start) / location.piece.step
  return offset >= 0 && offset < location.piece.length ? location.logicalStart + offset : undefined
}

const NO_SOURCE: InsertSource = { refs: [] }

const resolveInArray = (
  node: OverlayNode,
  parent: OverlayNode,
  parentPath: Path,
): Path | undefined => {
  const overlay = arrayOverlay(parent)
  const key = Number(node.parentKey)
  const index = findEntryIndex(overlay, node.parentKind, key, node.parentSource)
  if (index === undefined) return undefined
  const piece = locatePiece(overlay, index)
  if (entryValueAt(parent, piece, key) !== nodeBase(node)) return undefined
  return [...parentPath, index]
}

/** The node's path in the draft, cached for the rest of `prepare()`; `undefined` once detached. */
export const resolvePath = (node: OverlayNode): Path | undefined => {
  if (node.preparedPath !== undefined) return node.preparedPath
  const parent = nodeParent(node)
  if (parent === undefined) {
    node.preparedPath = []
    return node.preparedPath
  }
  const parentPath = resolvePath(parent)
  if (parentPath === undefined) return undefined
  if (node.parentKind !== 0) {
    const path = resolveInArray(node, parent, parentPath)
    if (path !== undefined) node.preparedPath = path
    return path
  }
  const key = String(node.parentKey)
  if (!objectHas(parent, key) || objectValue(parent, key) !== nodeBase(node)) return undefined
  node.preparedPath = [...parentPath, key]
  return node.preparedPath
}
