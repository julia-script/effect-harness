import type { ArrayOverlay, InsertPiece, Piece, PieceNode } from './context.ts'

/**
 * The piece table of an array overlay: a treap of pieces keyed by element count. A piece is a run
 * of base elements or inserted values, walked forwards or backwards, so splices, reversals, and
 * sorts rearrange pieces instead of elements.
 */

const nextPriority = (overlay: ArrayOverlay): number => {
  let value = overlay.seed
  value ^= value << 13
  value ^= value >>> 17
  value ^= value << 5
  overlay.seed = value >>> 0
  return overlay.seed
}

export const treeElements = (node: PieceNode | undefined): number => node?.elements ?? 0

const update = (node: PieceNode): void => {
  node.elements = treeElements(node.left) + node.piece.length + treeElements(node.right)
}

export const createPieceNode = (overlay: ArrayOverlay, piece: Piece): PieceNode => ({
  piece,
  left: undefined,
  right: undefined,
  priority: nextPriority(overlay),
  elements: piece.length,
})

const merge = (left: PieceNode | undefined, right: PieceNode | undefined) => {
  if (left === undefined) return right
  if (right === undefined) return left
  if (left.priority >= right.priority) {
    left.right = merge(left.right, right)
    update(left)
    return left
  }
  right.left = merge(left, right.left)
  update(right)
  return right
}

type Split = [PieceNode | undefined, PieceNode | undefined]

const splitInside = (overlay: ArrayOverlay, root: PieceNode, offset: number): Split => {
  const first: Piece = { ...root.piece, length: offset }
  const second: Piece = {
    ...root.piece,
    start: root.piece.start + root.piece.step * offset,
    length: root.piece.length - offset,
  }
  return [
    merge(root.left, createPieceNode(overlay, first)),
    merge(createPieceNode(overlay, second), root.right),
  ]
}

export const splitPieceTree = (
  overlay: ArrayOverlay,
  root: PieceNode | undefined,
  index: number,
): Split => {
  if (root === undefined) return [undefined, undefined]
  const leftLength = treeElements(root.left)
  const pieceEnd = leftLength + root.piece.length
  if (index < leftLength) {
    const [left, right] = splitPieceTree(overlay, root.left, index)
    root.left = right
    update(root)
    return [left, root]
  }
  if (index > pieceEnd) {
    const [left, right] = splitPieceTree(overlay, root.right, index - pieceEnd)
    root.right = left
    update(root)
    return [root, right]
  }
  if (index === leftLength) {
    const left = root.left
    root.left = undefined
    update(root)
    return [left, root]
  }
  if (index === pieceEnd) {
    const right = root.right
    root.right = undefined
    update(root)
    return [root, right]
  }
  return splitInside(overlay, root, index - leftLength)
}

const leftmost = (node: PieceNode): PieceNode => {
  let current = node
  while (current.left !== undefined) current = current.left
  return current
}

export const rightmost = (node: PieceNode): PieceNode => {
  let current = node
  while (current.right !== undefined) current = current.right
  return current
}

export const sameSource = (left: Piece, right: Piece): boolean =>
  left.kind === right.kind && (left.kind === 'base' || left.source === sourceOf(right))

const sourceOf = (piece: Piece) => (piece.kind === 'insert' ? piece.source : undefined)

/** Whether two singleton pieces are neighbors, which joins them into a run in either direction. */
export const adjacentSingletons = (left: Piece, right: Piece): boolean =>
  left.length === 1 && right.length === 1 && Math.abs(right.start - left.start) === 1

/** Whether `right` continues `left`'s run. */
export const continues = (left: Piece, right: Piece): boolean =>
  left.step === right.step && left.start + left.step * left.length === right.start

const mergeable = (left: Piece, right: Piece): boolean =>
  sameSource(left, right) && (adjacentSingletons(left, right) || continues(left, right))

const stepBetween = (left: Piece, right: Piece): 1 | -1 => (right.start > left.start ? 1 : -1)

const joined = (left: Piece, right: Piece): Piece => {
  const step = left.length === 1 ? stepBetween(left, right) : left.step
  const length = left.length + right.length
  return left.kind === 'base'
    ? { kind: 'base', start: left.start, length, step }
    : { kind: 'insert', source: left.source, start: left.start, length, step }
}

/** Concatenate two trees, joining the touching pieces when they form one run. */
export const joinNormalized = (
  overlay: ArrayOverlay,
  left: PieceNode | undefined,
  right: PieceNode | undefined,
): PieceNode | undefined => {
  if (left === undefined) return right
  if (right === undefined) return left
  const leftPiece = rightmost(left).piece
  const rightPiece = leftmost(right).piece
  if (!mergeable(leftPiece, rightPiece)) return merge(left, right)
  const [leftRest] = splitPieceTree(overlay, left, treeElements(left) - leftPiece.length)
  const [, rightRest] = splitPieceTree(overlay, right, rightPiece.length)
  const middle = createPieceNode(overlay, joined(leftPiece, rightPiece))
  return joinNormalized(overlay, joinNormalized(overlay, leftRest, middle), rightRest)
}

const flatten = (node: PieceNode | undefined, output: Array<Piece>): void => {
  if (node === undefined) return
  flatten(node.left, output)
  output.push(node.piece)
  flatten(node.right, output)
}

export const piecesOf = (overlay: ArrayOverlay): ReadonlyArray<Piece> => {
  if (overlay.pieces === undefined) {
    overlay.pieces = []
    flatten(overlay.root, overlay.pieces)
  }
  return overlay.pieces
}

/** Join neighboring pieces of one run in place. */
export const mergePieces = (pieces: Array<Piece>): void => {
  for (let index = 1; index < pieces.length;) {
    const left = pieces[index - 1]!
    const right = pieces[index]!
    if (sameSource(left, right) && adjacentSingletons(left, right)) {
      left.step = stepBetween(left, right)
      left.length = 2
      pieces.splice(index, 1)
    } else if (sameSource(left, right) && continues(left, right)) {
      left.length += right.length
      pieces.splice(index, 1)
    } else index += 1
  }
}

export const treeFromPieces = (overlay: ArrayOverlay, pieces: Array<Piece>) => {
  mergePieces(pieces)
  let root: PieceNode | undefined
  for (const piece of pieces) root = merge(root, createPieceNode(overlay, piece))
  return root
}

export const replaceAllPieces = (overlay: ArrayOverlay, pieces: Array<Piece>): void => {
  overlay.root = treeFromPieces(overlay, pieces)
  overlay.pieces = undefined
  overlay.baseLocations = undefined
  overlay.insertLocations = undefined
}

export const arrayLength = (overlay: ArrayOverlay): number => treeElements(overlay.root)

/**
 * The piece holding logical `index`, which must be in range; its offset lands in
 * `overlay.locatedOffset`.
 */
export const locatePiece = (overlay: ArrayOverlay, index: number): Piece => {
  let node = overlay.root
  let remaining = index
  while (node !== undefined) {
    const leftLength = treeElements(node.left)
    if (remaining < leftLength) node = node.left
    else if (remaining >= leftLength + node.piece.length) {
      remaining -= leftLength + node.piece.length
      node = node.right
    } else {
      overlay.locatedOffset = remaining - leftLength
      return node.piece
    }
  }
  overlay.locatedOffset = 0
  return EMPTY_PIECE
}

/** Never reached for an in-range index; keeps `locatePiece` total. */
const EMPTY_PIECE: Piece = { kind: 'base', start: 0, length: 0, step: 1 }

export const extendRightmostPiece = (node: PieceNode, amount: number): void => {
  if (node.right === undefined) node.piece.length += amount
  else extendRightmostPiece(node.right, amount)
  update(node)
}

export const invalidatePieceCaches = (overlay: ArrayOverlay): void => {
  overlay.pieces = undefined
  overlay.baseLocations = undefined
  overlay.insertLocations = undefined
  overlay.plan = undefined
}

/** Append `piece` to `pieces`, joining it with the last one when they form one run. */
export const appendMergedPiece = (pieces: Array<Piece>, piece: Piece): void => {
  const previous = pieces.at(-1)
  if (previous !== undefined && sameSource(previous, piece)) {
    if (adjacentSingletons(previous, piece)) {
      previous.step = stepBetween(previous, piece)
      previous.length = 2
      return
    }
    if (continues(previous, piece)) {
      previous.length += piece.length
      return
    }
  }
  pieces.push(piece)
}

export const singletonPiece = (piece: Piece, sourceIndex: number): Piece =>
  piece.kind === 'base'
    ? { kind: 'base', start: sourceIndex, length: 1, step: 1 }
    : { kind: 'insert', source: piece.source, start: sourceIndex, length: 1, step: 1 }

export type { InsertPiece }
